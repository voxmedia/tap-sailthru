"""Stream type classes for tap-sailthru."""

import copy
import csv
import heapq
import time
from collections import OrderedDict
from pathlib import Path
from typing import Iterable, List, Optional

import backoff
import pendulum
import requests
from google.cloud import bigquery
from requests.exceptions import ChunkedEncodingError
from sailthru.sailthru_client import SailthruClient
from sailthru.sailthru_error import SailthruClientError
from sailthru.sailthru_response import SailthruResponse
from urllib3.exceptions import MaxRetryError

from tap_sailthru.client import SailthruStream

SCHEMAS_DIR = Path(__file__).parent / Path("./schemas")


class SailthruJobTimeoutError(Exception):
    """class for sailthru job timeout error.

    Args:
        Exception: error
    """
    pass


class SailthruJobStream(SailthruStream):
    """sailthru stream class."""

    @backoff.on_exception(backoff.expo, SailthruClientError, max_tries=4, factor=3)
    def get_job_url(
        self, client: SailthruClient, job_id: str, timeout: int = 1200
    ) -> str:
        """Poll the /job endpoint and checks to see if export job is completed.

        :param client: SailthruClient, the Sailthru API client
        :param job_id: str, the job_id to poll
        :param timeout: int, number of seconds before halting request (default 1200)
        :returns: str, the export URL
        """
        status = ""
        job_start_time = pendulum.now()
        while status != "completed":
            response = client.api_get("job", {"job_id": job_id}).get_body()
            status = response.get("status")
            # pylint: disable=logging-fstring-interpolation
            self.logger.info(f"Job report status: {status}")
            now = pendulum.now()
            if (now - job_start_time).seconds > timeout:
                # pylint: disable=logging-fstring-interpolation
                self.logger.critical(
                    f"Request with job_id {job_id}"
                    f" exceeded {timeout} second timeout"
                    f"latest_status: {status}"
                )
                raise SailthruJobTimeoutError
            time.sleep(1)
        return response.get("export_url")

    @backoff.on_exception(backoff.expo, ChunkedEncodingError, max_tries=3, factor=2)
    def process_job_csv(
        self, export_url: str, chunk_size: int = 1024, parent_params: dict = None
    ) -> Iterable[dict]:
        """Fetch CSV from URL and streams each line.

        :param export_url: str, The URL from which to fetch the CSV data from
        :param chunk_size: int, The chunk size to read per line
        :param parent_params: dict, A dictionary with "parent" parameters to append
            to each record
        :returns: Iterable[dict], A generator of a dictionary
        """
        with requests.get(export_url, stream=True) as req:
            try:
                reader = csv.reader(
                    (
                        line.decode("utf-8")
                        for line in req.iter_lines(chunk_size=chunk_size)
                    ),
                    delimiter=",",
                    quotechar='"',
                )
                fieldnames = next(reader)
                for row in reader:
                    dicted_row = {}
                    for n, v in zip(fieldnames, row):
                        if n not in dicted_row.keys():
                            dicted_row[n] = v
                    if parent_params:
                        dicted_row.update(parent_params)
                    transformed_row = self.post_process(dicted_row)
                    if transformed_row is None:
                        continue
                    else:
                        yield self.post_process(dicted_row)
            except ChunkedEncodingError:
                self.logger.info(
                    "Chunked Encoding Error in the list member stream, stopping early"
                )
                pass


class AccountsStream(SailthruStream):
    """Define custom stream."""

    name = "accounts"
    path = "settings"
    primary_keys = ["id"]
    replication_key = None
    schema_filepath = SCHEMAS_DIR / "accounts.json"

    def parse_response(self, response: dict, context: Optional[dict]) -> Iterable[dict]:
        """Parse the response and return an iterator of result rows."""
        response["account_name"] = self.config.get("account_name")
        yield response

    def post_process(self, row: str, context: dict) -> dict:
        """As needed, append or transform raw data to match expected structure."""
        k_arr = row["domains"].copy().keys()
        for k in k_arr:
            if k == "":
                del row["domains"][k]
        return row


class BlastStream(SailthruStream):
    """Define custom stream."""

    name = "blasts"
    path = "blast"
    primary_keys = ["id"]
    replication_key = "start_time"
    schema_filepath = SCHEMAS_DIR / "blasts.json"

    def get_url(self, context: Optional[dict]) -> str:
        """Construct url for api request.

        Args:
            context (Optional[dict]): meltano context dict

        Returns:
            str: url
        """
        return self.path

    def prepare_request_payload(
        self, context: Optional[dict], next_page_token: Optional[str] = None
    ) -> dict:
        """Prepare request payload.

        :param context: Stream partition or context dictionary.
        :returns: dict, A dictionary containing the request payload.
        """

        # set the start date to 7 days before the last replication state
        starting_replication_time = self.stream_state.get("starting_replication_value") or self.config.get('start_date')
        starting_replication_date = pendulum.parse(starting_replication_time)
        starting_replication_date_minus_7 = starting_replication_date.subtract(days=7).start_of('day')
        start_date = starting_replication_date_minus_7.to_date_string()

        return {
            "status": "sent",
            "limit": 0,
            "start_date": start_date,
        }

    def get_child_context(self, record: dict, context: Optional[dict]) -> dict:
        """Return a child context object from the record and optional provided context.

        Args:
            record (dict): Individual record in the stream.
            context (Optional[dict]): Stream partition or context dictionary

        Returns:
            dict: dictionary for child stream
        """
        return {"blast_id": record["blast_id"]}

    def parse_response(
        self, response: SailthruResponse, context: Optional[dict]
    ) -> Iterable[dict]:
        """Parse the response and return an iterator of result rows."""
        for row in response["blasts"]:
            row["account_name"] = self.config.get("account_name")
            yield row

    def post_process(self, row: dict, context: Optional[dict]) -> dict:
        """As needed, append or transform raw data to match expected structure."""
        new_row = copy.deepcopy(row)
        new_row.pop("start_time")
        start_time = row["start_time"]
        parsed_start_time = pendulum.from_format(
            start_time, "ddd, D MMM YYYY HH:mm:ss ZZ"
        )
        new_row["start_time"] = parsed_start_time.to_datetime_string()
        return new_row


class BlastStatsStream(SailthruStream):
    """Define custom stream."""

    name = "blast_stats"
    path = "stats"
    primary_keys = ["blast_id"]
    schema_filepath = SCHEMAS_DIR / "blast_stats.json"
    parent_stream_type = BlastStream
    rest_method = "GET"

    def get_url(self, context: Optional[dict]) -> str:
        """Construct url for api request.

        Args:
            context (Optional[dict]): meltano context dict

        Returns:
            str: url
        """
        return self.path

    def prepare_request_payload(
        self, context: Optional[dict], next_page_token: Optional[str] = None
    ) -> dict:
        """Prepare request payload.

        :param context: Stream partition or context dictionary
        :returns: dict, A dictionary containing the request payload
        """
        return {
            "stat": "blast",
            "blast_id": context["blast_id"],
            "beacon_times": 1,
            "click_times": 1,
            "clickmap": 1,
            "domain": 1,
            "engagement": 1,
            "purchase_times": 1,
            "signup": 1,
            "subject": 1,
            "topusers": 1,
            "urls": 1,
            "banners": 1,
            "purchase_items": 1,
            "device": 1,
        }

    def parse_response(self, response: dict, context: Optional[dict]) -> Iterable[dict]:
        """Parse the response and return an iterator of result rows."""
        response["account_name"] = self.config.get("account_name")
        response["blast_id"] = context["blast_id"]
        yield response

    def post_process(self, row: dict, context: Optional[dict]) -> dict:
        """As needed, append or transform raw data to match expected structure."""
        keys_arr = row.copy().keys()
        if "beacon_times" in keys_arr:
            beacon_times_dict = row.copy()["beacon_times"]
            new_beacon_times_arr = []
            for k, v in beacon_times_dict.items():
                new_beacon_times_arr.append({"beacon_time": k, "count": v})
            row["beacon_times"] = new_beacon_times_arr
        if "click_times" in keys_arr:
            click_times_dict = row.copy()["click_times"]
            new_click_times_arr = []
            for k, v in click_times_dict.items():
                new_click_times_arr.append({"click_time": k, "count": v})
            row["click_times"] = new_click_times_arr
        if "domain" in keys_arr:
            domain_stats_dict = row.pop("domain")
            new_domain_stats_arr = []
            for domain, domain_dict in domain_stats_dict.items():
                new_domain_stats_arr.append({**{"domain": domain}, **domain_dict})
            row["domain_stats"] = new_domain_stats_arr
        if "signup" in keys_arr:
            signup_stats_dict = row.pop("signup")
            new_signup_stats_arr = [
                signup_dict for signup_dict in signup_stats_dict.values()
            ]
            row["signup"] = new_signup_stats_arr
        if "subject" in keys_arr:
            subject_stats_dict = row.pop("subject")
            new_subject_stats_arr = []
            for subject, subject_dict in subject_stats_dict.items():
                new_subject_stats_arr.append({**{"subject": subject}, **subject_dict})
            row["subject"] = new_subject_stats_arr
        if "urls" in keys_arr:
            url_stats_dict = row.pop("urls")
            new_url_stats_arr = []
            for url, url_dict in url_stats_dict.items():
                new_url_stats_arr.append({**{"url": url}, **url_dict})
            row["urls"] = new_url_stats_arr
        if "device" in keys_arr:
            device_stats_dict = row.pop("device")
            new_device_stats_arr = []
            for device, device_dict in device_stats_dict.items():
                new_device_stats_arr.append({**{"device": device}, **device_dict})
            row["device_stats"] = new_device_stats_arr
        return row


class BlastQueryStream(SailthruJobStream):
    """Custom Stream for the results of a Blast Query job."""
    name = "blast_query"
    job_name = "blast_query"
    path = "job"
    primary_keys = ["job_id"]
    schema_filepath = SCHEMAS_DIR / "blast_query.json"
    parent_stream_type = BlastStream

    def prepare_request_payload(
        self, context: Optional[dict], next_page_token: Optional[str] = None
    ) -> dict:
        """Prepare request payload.

        Args:
            context: Stream partition or context dictionary.

        Returns:
            A dictionary containing the request payload.
        """
        return {"job": "blast_query", "blast_id": context["blast_id"]}

    def get_records(self, context: Optional[dict]):
        """Retrieve records by creating and processing export job.

        Args:
            context (Optional[dict]): Stream partition or context dictionary

        Yields:
            dict: Individual record from stream
        """
        blast_id = context["blast_id"]
        client = self.authenticator
        payload = self.prepare_request_payload(context=context)
        response = client.api_post("job", payload).get_body()
        if response.get("error"):
            # https://getstarted.sailthru.com/developers/api/job/#Error_Codes
            # Error code 99 = You may not export a blast that has been sent
            # pylint: disable=logging-fstring-interpolation
            self.logger.info(f"Skipping blast_id: {blast_id}")
        try:
            export_url = self.get_job_url(client=client, job_id=response["job_id"])
        except MaxRetryError:
            self.logger.info(f"Skipping blast_id: {blast_id}")
            return

        # Add blast id to each record
        yield from self.process_job_csv(
            export_url=export_url,
            parent_params={
                "blast_id": blast_id,
                "account_name": self.config.get("account_name"),
            },
        )

    def post_process(self, row: dict) -> dict:
        """As needed, append or transform raw data to match expected structure."""
        new_row = {}
        for k, v in row.items():
            new_row[k.lower().replace(" ", "_")] = v
        return new_row


class TemplateStream(SailthruStream):
    """Custom Stream for templates."""

    name = "templates"
    path = "template"
    primary_keys = ["template_id"]
    schema_filepath = SCHEMAS_DIR / "templates.json"

    def prepare_request_payload(
        self, context: Optional[dict], next_page_token: Optional[str] = None
    ) -> dict:
        """Prepare request payload.

        Args:
            context: Stream partition or context dictionary.

        Returns:
            A dictionary containing the request payload.
        """
        return {}

    def parse_response(
        self, response: SailthruResponse, context: Optional[dict]
    ) -> Iterable[dict]:
        """Parse the response and return an iterator of result rows."""
        for row in response["templates"]:
            row["account_name"] = self.config.get("account_name")
            yield row


class ListStream(SailthruStream):
    """Custom Stream for lists."""

    name = "lists"
    path = "list"
    primary_keys = ["list_id"]
    schema_filepath = SCHEMAS_DIR / "lists.json"

    def prepare_request_payload(
        self, context: Optional[dict], next_page_token: Optional[str] = None
    ) -> dict:
        """Prepare request payload.

        Args:
            context: Stream partition or context dictionary.

        Returns:
            A dictionary containing the request payload.
        """
        return {}

    def get_child_context(self, record: dict, context: Optional[dict]) -> dict:
        """Return a child context object from the record and optional provided context.

        Args:
            record (dict): Individual record from the stream
            context (Optional[dict]): Stream partition or context dictionary

        Returns:
            dict: dictionary with context for the child stream
        """
        return {"list_id": record["list_id"], "list_name": record["name"]}

    def parse_response(
        self, response: SailthruResponse, context: Optional[dict]
    ) -> Iterable[dict]:
        """Parse the response and return an iterator of result rows."""
        for row in response["lists"]:
            row["account_name"] = self.config.get("account_name")
            yield row


class PrimaryListStream(ListStream):
    """Custom Stream for lists."""

    name = "primary_lists"
    path = "list"
    primary_keys = ["list_id"]
    replication_key = "create_time"
    schema_filepath = SCHEMAS_DIR / "lists.json"

    def prepare_request_payload(
        self, context: Optional[dict], next_page_token: Optional[str] = None
    ) -> dict:
        """Prepare request payload.

        Args:
            context: Stream partition or context dictionary.

        Returns:
            A dictionary containing the request payload.
        """
        return {"primary": 1}

    def get_child_context(self, record: dict, context: Optional[dict]) -> dict:
        """Return a child context object from the record and optional provided context.

        Args:
            record (dict): Individual record from the stream
            context (Optional[dict]): Stream partition or context dictionary

        Returns:
            dict: dictionary with context for the child stream
        """
        return {"list_id": record["list_id"], "list_name": record["name"]}

    def parse_response(
        self, response: SailthruResponse, context: Optional[dict]
    ) -> Iterable[dict]:
        """Parse the response and return an iterator of result rows."""
        for row in response["lists"]:
            row["account_name"] = self.config.get("account_name")
            yield row


class ListMembersParentStream(SailthruStream):
    """Manufactured Stream to help with load balancing for List Members stream."""

    name = "list_members_parent"
    schema_filepath = SCHEMAS_DIR / "lists.json"

    def query_for_lists(self, context: Optional[dict]) -> List[dict]:
        """Send query to data warehouse to retrieve newsletter data."""
        lists_query = f"""
            SELECT
                id as list_id,
                list_name,
                max(valid_email_count) email_count
            FROM `{self.config.get('table_id')}`
            where
                account = @account_name
                and is_primary
            group by 1, 2
            order by 2
        """
        self.logger.info(f"Executing query: {lists_query}")
        client = bigquery.Client()
        query_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter(
                    "account_name", "STRING", self.config.get("account_name")
                )
            ]
        )
        results = client.query(lists_query, job_config=query_config).result()
        return results

    def split_lists_into_chunks(
        self, list_results: List, num_chunks: int, context: Optional[dict]
    ):
        """Split list of newsletters using a priority queue into sub-arrays.

        Using the PSL heapq library, receives a list of dictionaries
        containing data about email newsletters and divides the list into
        a configurable number of lists whose summed size is as equal as possible.
        Loops through list of newsletters and appends each newsletter
        to the sub-array that has the smallest size at that time.

        Args:
            list_results (List): list of dicts with newsletter data
                e.g. [{
                    'list_id': 'SAMPLE_LIST_ID',
                    'list_name': 'SAMPLE_LIST_NAME',
                    'email_count': 100
                }]
            num_chunks (int): number of sub-arrays to create, comes from tap config
            context (Optional[dict]): optional meltano context dictionary
        """
        list_names = [[] for _ in range(num_chunks)]
        list_ids = [[] for _ in range(num_chunks)]
        totals = [(0, i) for i in range(num_chunks)]
        # create a heap, a priority queue
        # in which the smallest element receives the highest priority
        heapq.heapify(totals)
        # get the chunk with the highest priority (lowest cumulative email count)
        for list_result in list_results:
            total, index = heapq.heappop(
                totals
            )
            # append the newsletter name to the corresponding array
            list_names[index].append(list_result.list_name)
            list_ids[index].append(list_result.list_id)
            # add the valid count of the added newsletter, then re-prioritize the queue
            heapq.heappush(
                totals, (total + list_result.email_count, index)
            )
        return list_names, list_ids

    def request_records(self, context: Optional[dict]) -> Iterable[dict]:
        """Retrieve records from SQL query, returning response records.

        Args:
            context: Stream partition or context dictionary.

        Yields:
            An item for every record in the response.
        """
        number_of_chunks = self.config.get("num_chunks")
        results = self.query_for_lists(context=context)
        list_names, list_ids = self.split_lists_into_chunks(
            results, number_of_chunks, context=context
        )
        self.logger.info(list_names)

        for list_name, list_id in zip(
            list_names[self.config.get("chunk_number")],
            list_ids[self.config.get("chunk_number")],
        ):
            yield {"list_name": list_name, "list_id": list_id}

    def get_child_context(self, record: dict, context: Optional[dict]) -> dict:
        """Return a child context object from the record and optional provided context.

        Args:
            record (dict): Individual record from the stream
            context (Optional[dict]): Stream partition or context dictionary

        Returns:
            dict: dictionary with context for the child stream
        """
        return {"list_name": record["list_name"], "list_id": record["list_id"]}


class ListStatsStream(SailthruStream):
    """Define custom stream."""

    name = "list_stats"
    path = "stats"
    primary_keys = ["list_id"]
    schema_filepath = SCHEMAS_DIR / "list_stats.json"
    parent_stream_type = ListStream
    rest_method = "GET"

    def prepare_request_payload(
        self, context: Optional[dict], next_page_token: Optional[str] = None
    ) -> dict:
        """Prepare request payload.

        Args:
            context: Stream partition or context dictionary.

        Returns:
            A dictionary containing the request payload.
        """
        return {
            "stat": "list", "list": context["list_name"]
        }

    def parse_response(self, response: dict, context: Optional[dict]) -> Iterable[dict]:
        """Parse the response and return an iterator of result rows."""
        response["account_name"] = self.config.get("account_name")
        response["list_id"] = context["list_id"]
        yield response

    def post_process(self, row: dict, context: Optional[dict]) -> dict:
        """As needed, append or transform raw data to match expected structure."""
        keys_arr = row.copy().keys()
        if "signup_month" in keys_arr:
            signup_month_stats_dict = row.pop("signup_month")
            new_signup_month_stats_arr = []
            for signup_month, signup_month_dict in signup_month_stats_dict.items():
                new_signup_month_stats_arr.append(
                    {**{"signup_month": signup_month}, **signup_month_dict}
                )
            row["signup_month"] = new_signup_month_stats_arr
        if "source_count" in keys_arr:
            source_count_dict = row.copy()["source_count"]
            new_source_count_arr = []
            for k, v in source_count_dict.items():
                new_source_count_arr.append({"source": k, "count": v})
            row["source_count"] = new_source_count_arr
        if "source_signup_count" in keys_arr:
            source_signup_count_dict = row.copy()["source_signup_count"]
            new_source_signup_count_arr = []
            try:
                for k, v in source_signup_count_dict.items():
                    new_source_signup_count_arr.append({"source": k, "count": v})
                row["source_signup_count"] = new_source_signup_count_arr
            except AttributeError:
                row["source_signup_count"] = []
        return row


class ListMemberStream(SailthruJobStream):
    """Custom Stream for the results of a Export List Data job."""
    name = "list_members"
    job_name = "list_members"
    path = "job"
    schema_filepath = SCHEMAS_DIR / "list_members.json"
    parent_stream_type = ListMembersParentStream

    def prepare_request_payload(
        self, context: Optional[dict], next_page_token: Optional[str] = None
    ) -> dict:
        """Prepare request payload.

        Args:
            context: Stream partition or context dictionary.

        Returns:
            A dictionary containing the request payload.
        """
        return {"job": "export_list_data", "list": context["list_name"]}

    def get_records(self, context: Optional[dict]):
        """Retrieve records by creating and processing export job.

        Args:
            context (Optional[dict]): Stream partition or context dictionary

        Yields:
            dict: Individual record from stream
        """
        list_name = context["list_name"]
        list_id = context["list_id"]
        client = self.authenticator
        payload = self.prepare_request_payload(context=context)
        response = client.api_post("job", payload).get_body()
        if response.get("error"):
            # https://getstarted.sailthru.com/developers/api/job/#Error_Codes
            # Error code 99 = You may not export a blast that has been sent
            # pylint: disable=logging-fstring-interpolation
            self.logger.info(f"Skipping list_name: {list_name}")
            return
        try:
            export_url = self.get_job_url(
                client=client,
                job_id=response["job_id"],
                timeout=self.config.get('request_timeout')
            )
        except MaxRetryError:
            self.logger.info(f"Skipping list: {list_name}")
            return

        # Add list id to each record
        yield from self.process_job_csv(
            export_url=export_url,
            parent_params={
                "list_name": list_name,
                "list_id": list_id,
                "account_name": self.config.get("account_name"),
            },
        )

    def post_process(self, row: dict) -> dict:
        """As needed, append or transform raw data to match expected structure."""
        new_row = {}
        # We convert the row into an ordered dict to prioritize Sailthru-native vars
        # over user-created vars with the same name
        ordered_row = OrderedDict(row)
        schema_keys = list(self._schema.copy()["properties"].keys())
        custom_vars_arr = []
        for k, v in ordered_row.items():
            cleaned_key = k.lower().replace(" ", "_")
            if cleaned_key in schema_keys and cleaned_key not in new_row.keys():
                new_row[cleaned_key] = v
            else:
                custom_vars_arr.append({"var_name": k, "var_value": str(v)})
        new_row["custom_vars"] = custom_vars_arr
        if "email_hash" not in new_row.keys():
            return None
        return new_row


class UsersStream(SailthruJobStream):
    """Define custom stream."""

    name = "users"
    path = "user"
    primary_keys = ["email"]
    schema_filepath = SCHEMAS_DIR / "users.json"
    parent_stream_type = PrimaryListStream

    def prepare_request_payload(
        self, context: Optional[dict], next_page_token: Optional[str] = None
    ) -> dict:
        """Prepare request payload.

        Args:
            context: Stream partition or context dictionary.

        Returns:
            A dictionary containing the request payload.
        """
        return {
            "job": "snapshot",
            "query": {
                "source_list": context["list_name"],
                "criteria": ["var_date"],
                "field": ["signup"],
                "timerange": ["since_date"],
                "value": ["-3 days"],
            },
        }

    def get_records(self, context: Optional[dict]):
        """Retrieve records by creating and processing export job.

        Args:
            context (Optional[dict]): Stream partition or context dictionary

        Yields:
            dict: Individual record from stream
        """
        list_name = context["list_name"]
        client = self.authenticator
        payload = self.prepare_request_payload(context=context)
        response = client.api_post("job", payload).get_body()
        if response.get("error"):
            # https://getstarted.sailthru.com/developers/api/job/#Error_Codes
            # Error code 99 = You may not export a blast that has been sent
            # pylint: disable=logging-fstring-interpolation
            self.logger.info(f"Skipping list_name: {list_name}")
        try:
            export_url = self.get_job_url(client=client, job_id=response["job_id"])
        except MaxRetryError:
            self.logger.info(f"Skipping list: {list_name}")
            return

        # Add list id to each record
        yield from self.process_job_csv(
            export_url=export_url,
            parent_params={
                "list_name": list_name,
                "list_id": context["list_id"],
                "account_name": self.config.get("account_name"),
            },
        )

    def parse_response(self, response: dict, context: Optional[dict]) -> Iterable[dict]:
        """Parse the response and return an iterator of result rows."""
        yield response

    def post_process(self, row: dict, context: dict) -> dict:
        """As needed, append or transform raw data to match expected structure."""
        keys_arr = list(row.copy().keys())
        if "lists" in keys_arr:
            lists_arr = []
            lists_dict = row.pop("lists")
            if lists_dict:
                for k, v in lists_dict.items():
                    lists_arr.append({"list_name": k, "signup_time": v})
            row["lists"] = lists_arr
        return row
