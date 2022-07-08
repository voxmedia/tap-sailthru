"""Stream type classes for tap-sailthru."""

import copy
import csv
import json
from pathlib import Path
import time
from typing import Any, Dict, Optional, Union, List, Iterable

import pendulum
import requests
from sailthru.sailthru_client import SailthruClient
from sailthru.sailthru_response import SailthruResponse
from singer_sdk import typing as th  # JSON Schema typing helpers
from singer_sdk.exceptions import InvalidStreamSortException
from singer_sdk.helpers.jsonpath import extract_jsonpath
from singer_sdk.helpers._state import finalize_state_progress_markers, log_sort_error

from tap_sailthru.client import sailthruStream

# TODO: Delete this is if not using json files for schema definition
SCHEMAS_DIR = Path(__file__).parent / Path("./schemas")
# TODO: - Override `UsersStream` and `GroupsStream` with your own stream definition.
#       - Copy-paste as many times as needed to create multiple stream types.


class SailthruJobTimeoutError(Exception):
    pass


class SailthruJobStream(sailthruStream):
    """sailthru stream class."""

    def get_job_url(
        self,
        client: SailthruClient,
        job_id: str,
        timeout: int = 600
    ) -> str:
        """
        Polls the /job endpoint and checks to see if export job is completed.
        Returns the export URL when job is ready.
        :param job_id: the job_id to poll
        :param timeout: the default timeout (seconds) before halting request
        :return: the export URL
        """
        status = ''
        job_start_time = pendulum.now()
        while status != 'completed':
            response = client.api_get('job', {'job_id': job_id}).get_body()
            status = response.get('status')
            # pylint: disable=logging-fstring-interpolation
            self.logger.info(f'Job report status: {status}')
            now = pendulum.now()
            if (now - job_start_time).seconds > timeout:
                # pylint: disable=logging-fstring-interpolation
                self.logger.critical(
                    f'Request with job_id {job_id}'
                    f' exceeded {timeout} second timeout'
                    f'latest_status: {status}')
                raise SailthruJobTimeoutError
            time.sleep(1)
        return response.get('export_url')

    def process_job_csv(
        self,
        export_url: str,
        chunk_size: int = 1024,
        parent_params: dict = None
    ) -> Iterable[dict]:
        """
        Fetches CSV from URL and streams each line.
        :param export_url: The URL from which to fetch the CSV data from
        :param chunk_size: The chunk size to read per line
        :param parent_params: A dictionary with "parent" parameters to append
            to each record
        :return: A generator of a dictionary
        """
        with requests.get(export_url, stream=True) as req:
            reader = csv.DictReader(
                line.decode('utf-8') for line in req.iter_lines(
                    chunk_size=chunk_size
                )
            )
            for row in reader:
                if parent_params:
                    row.update(parent_params)
                if self.name == 'list_members':
                    yield self.post_process(row)
                else:
                    yield row


class AccountsStream(sailthruStream):
    """Define custom stream."""
    name = "accounts"
    path = "settings"
    primary_keys = ["id"]
    replication_key = None
    schema_filepath = SCHEMAS_DIR / "accounts.json"

    def get_url(self, context: Optional[dict]) -> str:
        """Get stream entity URL.

        Developers override this method to perform dynamic URL generation.

        Args:
            context: Stream partition or context dictionary.

        Returns:
            A URL, optionally targeted to a specific partition or context.
        """
        return self.path


    def _sync_records(  # noqa C901  # too complex
            self, context: Optional[dict] = None
    ) -> None:
        """Sync records, emitting RECORD and STATE messages.

        Args:
            context: Stream partition or context dictionary.

        Raises:
            InvalidStreamSortException: TODO
        """
        record_count = 0
        current_context: Optional[dict]
        context_list: Optional[List[dict]]
        context_list = [context] if context is not None else self.partitions
        selected = self.selected

        for current_context in context_list or [{}]:
            partition_record_count = 0
            current_context = current_context or None
            state = self.get_context_state(current_context)
            state_partition_context = self._get_state_partition_context(current_context)
            self._write_starting_replication_value(current_context)
            accounts = current_context.pop('accounts')

            for account in accounts:
                current_context['api_key'] = account['api_key']
                current_context['api_secret'] = account['api_secret']
                current_context['account_name'] = account['name']
                child_context: Optional[dict] = (
                    None if current_context is None else copy.copy(current_context)
                )
                for record_result in self.get_records(current_context):
                    if isinstance(record_result, tuple):
                        # Tuple items should be the record and the child context
                        record, child_context = record_result
                    else:
                        record = record_result
                    child_context = copy.copy(
                        self.get_child_context(record=record, context=child_context)
                    )
                    record['account_name'] = current_context['account_name']

                    # Sync children, except when primary mapper filters out the record
                    if self.stream_maps[0].get_filter_result(record):
                        self._sync_children(child_context)
                    self._check_max_record_limit(record_count)
                    if selected:
                        if (record_count - 1) % self.STATE_MSG_FREQUENCY == 0:
                            self._write_state_message()
                        self._write_record_message(record)
                        try:
                            self._increment_stream_state(record, context=current_context)
                        except InvalidStreamSortException as ex:
                            log_sort_error(
                                log_fn=self.logger.error,
                                ex=ex,
                                record_count=record_count + 1,
                                partition_record_count=partition_record_count + 1,
                                current_context=current_context,
                                state_partition_context=state_partition_context,
                                stream_name=self.name,
                            )
                            raise ex

                    record_count += 1
                    partition_record_count += 1
                if current_context == state_partition_context:
                    # Finalize per-partition state only if 1:1 with context
                    finalize_state_progress_markers(state)
            if not context:
                # Finalize total stream only if we have the full full context.
                # Otherwise will be finalized by tap at end of sync.
                finalize_state_progress_markers(self.stream_state)
            self._write_record_count_log(record_count=record_count, context=context)
            # Reset interim bookmarks before emitting final STATE message:
            self._write_state_message()

    def post_process(self, row: str, context: dict) -> dict:
        """As needed, append or transform raw data to match expected structure."""
        self.logger.debug(f'THIS IS THE RECORD: {row}')
        self.logger.debug(f'THIS IS THE RECORD TYPE: {type(row)}')
        row = json.loads(row)
        self.logger.debug(f'THIS IS THE NEW RECORD TYPE: {type(row)}')
        k_arr = row['domains'].copy().keys()
        for k in k_arr:
            if k == '':
                del row['domains'][k]
        return row


class BlastStream(sailthruStream):
    """Define custom stream."""
    name = "blasts"
    path = "blast"
    primary_keys = ["id"]
    replication_key = "modify_time"
    schema_filepath = SCHEMAS_DIR / "blasts.json"
    parent_stream_type = AccountsStream

    def get_url(self, context: Optional[dict]) -> str:
        return self.path

    def prepare_request_payload(
        self,
        context: Optional[dict],
        next_page_token: Optional[str] = None
    ) -> dict:
        """Prepare request payload.

        Args:
            context: Stream partition or context dictionary.

        Returns:
            A dictionary containing the request payload.
        """
        return {'status': 'sent', 'limit': 0, 'start_date': context['start_date']}

    def get_child_context(self, record: dict, context: Optional[dict]) -> dict:
        return {
            "account_name": context["account_name"],
            "blast_id": record["blast_id"],
            "api_key": context["api_key"],
            "api_secret": context["api_secret"]
        }

    def parse_response(
        self,
        response: SailthruResponse,
        context: Optional[dict]
    ) -> Iterable[dict]:
        """Parse the response and return an iterator of result rows."""
        for row in response['blasts']:
            row['account_name'] = context['account_name']
            yield row


class BlastStatsStream(sailthruStream):
    """Define custom stream."""
    name = "blast_stats"
    path = "stats"
    primary_keys = ["blast_id"]
    schema_filepath = SCHEMAS_DIR / "blast_stats.json"
    parent_stream_type = BlastStream
    rest_method = 'GET'

    def get_url(self, context: Optional[dict]) -> str:
        return self.path

    def prepare_request_payload(
        self,
        context: Optional[dict],
        next_page_token: Optional[str] = None
    ) -> dict:
        """Prepare request payload.

        Args:
            context: Stream partition or context dictionary.

        Returns:
            A dictionary containing the request payload.
        """
        return {
            'stat': 'blast',
            'blast_id': context['blast_id'],
            'beacon_times': 1,
            'click_times': 1,
            'clickmap': 1,
            'domain': 1,
            'engagement': 1,
            'purchase_times': 1,
            'signup': 1,
            'subject': 1,
            'topusers': 1,
            'urls': 1,
            'banners': 1,
            'purchase_items': 1,
            'device': 1
        }

    def parse_response(
        self,
        response: dict,
        context: Optional[dict]
    ) -> Iterable[dict]:
        """Parse the response and return an iterator of result rows."""
        self.logger.info(response)
        response['account_name'] = context['account_name']
        response['blast_id'] = context['blast_id']
        yield response

    def post_process(self, row: dict, context: Optional[dict]) -> dict:
        """As needed, append or transform raw data to match expected structure."""
        keys_arr = row.copy().keys()
        if 'beacon_times' in keys_arr:
            beacon_times_dict = row.copy()['beacon_times']
            new_beacon_times_arr = []
            for k, v in beacon_times_dict.items():
                new_beacon_times_arr.append(
                    {'beacon_time': k, 'count': v}
                )
            row['beacon_times'] = new_beacon_times_arr
        if 'click_times' in keys_arr:
            click_times_dict = row.copy()['click_times']
            new_click_times_arr = []
            for k, v in click_times_dict.items():
                new_click_times_arr.append(
                    {'click_time': k, 'count': v}
                )
            row['click_times'] = new_click_times_arr
        if 'domain' in keys_arr:
            domain_stats_dict = row.pop('domain')
            new_domain_stats_arr = []
            for domain, domain_dict in domain_stats_dict.items():
                new_domain_stats_arr.append({
                    **{'domain': domain},
                    **domain_dict
                })
            row['domain_stats'] = new_domain_stats_arr
        if 'signup' in keys_arr:
            signup_stats_dict = row.pop('signup')
            new_signup_stats_arr = [
                signup_dict for signup_dict in signup_stats_dict.values()
            ]
            row['signup'] = new_signup_stats_arr
        if 'subject' in keys_arr:
            subject_stats_dict = row.pop('subject')
            new_subject_stats_arr = []
            for subject, subject_dict in subject_stats_dict.items():
                new_subject_stats_arr.append({
                    **{'subject': subject},
                    **subject_dict
                })
            row['subject'] = new_subject_stats_arr
        if 'urls' in keys_arr:
            url_stats_dict = row.pop('urls')
            new_url_stats_arr = []
            for url, url_dict in url_stats_dict.items():
                new_url_stats_arr.append({
                    **{'url': url},
                    **url_dict
                })
            row['urls'] = new_url_stats_arr
        if 'device' in keys_arr:
            device_stats_dict = row.pop('device')
            new_device_stats_arr = []
            for device, device_dict in device_stats_dict.items():
                new_device_stats_arr.append({
                    **{'device': device},
                    **device_dict
                })
            row['device_stats'] = new_device_stats_arr
        return row


class BlastQueryStream(SailthruJobStream):
    "Custom Stream for the results of a Blast Query job"
    name = "blast_query"
    job_name = "blast_query"
    path = "job"
    primary_keys = ["job_id"]
    replication_key = "send_time"
    schema_filepath = SCHEMAS_DIR / "blast_query.json"
    parent_stream_type = BlastStream

    def prepare_request_payload(
        self,
        context: Optional[dict],
        next_page_token: Optional[str] = None
    ) -> dict:
        """Prepare request payload.

        Args:
            context: Stream partition or context dictionary.

        Returns:
            A dictionary containing the request payload.
        """
        return {
            'job': 'blast_query',
            'blast_id': context['blast_id']
        }

    def get_records(
        self,
        context: Optional[dict]
    ):
        blast_id = context['blast_id']
        client = SailthruClient(
            context['api_key'],
            context['api_secret'],
        )
        payload = self.prepare_request_payload(context=context)
        response = client.api_post('job', payload).get_body()
        self.logger.info(f"Got response: {response}")
        if response.get("error"):
            # https://getstarted.sailthru.com/developers/api/job/#Error_Codes
            # Error code 99 = You may not export a blast that has been sent
            # pylint: disable=logging-fstring-interpolation
            self.logger.info(f"Skipping blast_id: {blast_id}")
        export_url = self.get_job_url(client=client, job_id=response['job_id'])

        # Add blast id to each record
        yield from self.process_job_csv(
            export_url=export_url,
            parent_params={
                'blast_id': blast_id,
                'account_name': context['account_name']
            }
        )


class ListStream(sailthruStream):
    """Custom Stream for lists"""
    name = "lists"
    path = "list"
    primary_keys = ["list_id"]
    replication_key = "create_time"
    schema_filepath = SCHEMAS_DIR / "lists.json"
    parent_stream_type = AccountsStream

    def get_url(self, context: Optional[dict]) -> str:
        return self.path

    def prepare_request_payload(
        self,
        context: Optional[dict],
        next_page_token: Optional[str] = None
    ) -> dict:
        """Prepare request payload.

        Args:
            context: Stream partition or context dictionary.

        Returns:
            A dictionary containing the request payload.
        """
        return {'primary': 1}

    def get_child_context(self, record: dict, context: Optional[dict]) -> dict:
        return {
            "account_name": context["account_name"],
            "list_id": record["list_id"],
            "list_name": record["name"],
            "api_key": context["api_key"],
            "api_secret": context["api_secret"],
            "start_date": context["start_date"]
        }

    def parse_response(
        self,
        response: SailthruResponse,
        context: Optional[dict]
    ) -> Iterable[dict]:
        """Parse the response and return an iterator of result rows."""
        for row in response['lists']:
            row['account_name'] = context['account_name']
            yield row


class ListStatsStream(sailthruStream):
    """Define custom stream."""
    name = "list_stats"
    path = "stats"
    primary_keys = ["list_id"]
    schema_filepath = SCHEMAS_DIR / "list_stats.json"
    parent_stream_type = ListStream
    rest_method = 'GET'

    def get_url(self, context: Optional[dict]) -> str:
        return self.path

    def prepare_request_payload(
        self,
        context: Optional[dict],
        next_page_token: Optional[str] = None
    ) -> dict:
        """Prepare request payload.

        Args:
            context: Stream partition or context dictionary.

        Returns:
            A dictionary containing the request payload.
        """
        return {
            'stat': 'list',
            'list': context['list_name']
        }

    def parse_response(
        self,
        response: dict,
        context: Optional[dict]
    ) -> Iterable[dict]:
        """Parse the response and return an iterator of result rows."""
        response['account_name'] = context['account_name']
        response['list_id'] = context['list_id']
        yield response


class ListMemberStream(SailthruJobStream):
    "Custom Stream for the results of a Export List Data job"
    name = "list_members"
    job_name = "list_members"
    path = "job"
    primary_keys = ["email_hash", "list_id"]
    replication_key = "list_signup"
    schema_filepath = SCHEMAS_DIR / "list_members.json"
    parent_stream_type = ListStream
    signup_dt = pendulum.datetime(2022, 5, 15, tz='UTC')
    selectively_sync_children = True

    def prepare_request_payload(
        self,
        context: Optional[dict],
        next_page_token: Optional[str] = None
    ) -> dict:
        """Prepare request payload.

        Args:
            context: Stream partition or context dictionary.

        Returns:
            A dictionary containing the request payload.
        """
        return {
            'job': 'export_list_data',
            'list': context['list_name']
        }

    def get_child_context(self, record: dict, context: Optional[dict]) -> dict:
        return_dict = {
            "account_name": context["account_name"],
            "list_id": record["list_id"],
            "user_id": record["profile_id"],
            "api_key": context["api_key"],
            "api_secret": context["api_secret"]
        }
        try:
            return_dict["list_name"] = record["list_name"]
        except KeyError:
            try:
                return_dict["list_name"] = record["name"]
            except KeyError:
                pass
        return return_dict

    def _sync_records(  # noqa C901  # too complex
        self, context: Optional[dict] = None
    ) -> None:
        """Sync records, emitting RECORD and STATE messages.

        Args:
            context: Stream partition or context dictionary.

        Raises:
            InvalidStreamSortException: TODO
        """
        record_count = 0
        current_context: Optional[dict]
        context_list: Optional[List[dict]]
        context_list = [context] if context is not None else self.partitions
        selected = self.selected

        for current_context in context_list or [{}]:
            partition_record_count = 0
            current_context = current_context or None
            state = self.get_context_state(current_context)
            state_partition_context = self._get_state_partition_context(current_context)
            self._write_starting_replication_value(current_context)
            child_context: Optional[dict] = (
                None if current_context is None else copy.copy(current_context)
            )
            record_results = self.get_records(current_context)
            for record_result in record_results:
                if isinstance(record_result, tuple):
                    # Tuple items should be the record and the child context
                    record, child_context = record_result
                else:
                    record = record_result
                child_context = copy.copy(
                    self.get_child_context(record=record, context=child_context)
                )
                for key, val in (state_partition_context or {}).items():
                    # Add state context to records if not already present
                    if key not in record:
                        record[key] = val

                # Sync children when list_signup is after midnight UTC yesterday
                try:
                    if 'list_signup' in record.keys():
                        if len(record['list_signup']) > 0:
                            list_signup = pendulum.parse(record['list_signup'])
                        else:
                            list_signup = pendulum.parse(record['profile_created_date'])
                    else:
                        list_signup = pendulum.parse(record['profile_created_date'])
                except ValueError:
                    list_signup = pendulum.parse(record['profile_created_date'])
                    if self.selectively_sync_children and list_signup > self.signup_dt:
                        self._sync_children(child_context)
                # if self.stream_maps[0].get_filter_result(record):
                #     self._sync_children(child_context)
                self._check_max_record_limit(record_count)
                if selected:
                    if (record_count - 1) % self.STATE_MSG_FREQUENCY == 0:
                        self._write_state_message()
                    # self.logger.info('THIS IS THE TYPE OF THE RECORD: %s', type(record))
                    # self.logger.info('THIS IS THE RECORD: %s', record)
                    self._write_record_message(record)
                    try:
                        self._increment_stream_state(record, context=current_context)
                    except InvalidStreamSortException as ex:
                        log_sort_error(
                            log_fn=self.logger.error,
                            ex=ex,
                            record_count=record_count + 1,
                            partition_record_count=partition_record_count + 1,
                            current_context=current_context,
                            state_partition_context=state_partition_context,
                            stream_name=self.name,
                        )
                        raise ex

                record_count += 1
                partition_record_count += 1
            if current_context == state_partition_context:
                # Finalize per-partition state only if 1:1 with context
                finalize_state_progress_markers(state)
        if not context:
            # Finalize total stream only if we have the full full context.
            # Otherwise will be finalized by tap at end of sync.
            finalize_state_progress_markers(self.stream_state)
        self._write_record_count_log(record_count=record_count, context=context)
        # Reset interim bookmarks before emitting final STATE message:
        self._write_state_message()

    def get_records(
        self,
        context: Optional[dict]
    ):
        list_name = context['list_name']
        client = SailthruClient(
            context['api_key'],
            context['api_secret'],
        )
        payload = self.prepare_request_payload(context=context)
        response = client.api_post('job', payload).get_body()
        self.logger.info(f"Got response: {response}")
        if response.get("error"):
            # https://getstarted.sailthru.com/developers/api/job/#Error_Codes
            # Error code 99 = You may not export a blast that has been sent
            # pylint: disable=logging-fstring-interpolation
            self.logger.info(f"Skipping list_name: {list_name}")
        export_url = self.get_job_url(client=client, job_id=response['job_id'])

        # Add blast id to each record
        yield from self.process_job_csv(
            export_url=export_url,
            parent_params={
                'list_name': list_name,
                'list_id': context['list_id'],
                'account_name': context['account_name']
            }
        )

    def post_process(self, row: dict) -> dict:
        """As needed, append or transform raw data to match expected structure."""
        keys_arr = list(row.keys())
        for k in keys_arr:
            snake_k = k.replace(' ', '_').lower()
            if snake_k not in row.keys():
                row[snake_k] = row[k]
                del row[k]
        return row


class UsersStream(sailthruStream):
    """Define custom stream."""
    name = "users"
    path = "user"
    primary_keys = ["email"]
    schema_filepath = SCHEMAS_DIR / "users.json"
    parent_stream_type = ListMemberStream
    state_partitioning_keys = []

    def get_url(self, context: Optional[dict]) -> str:
        return self.path

    def prepare_request_payload(
        self,
        context: Optional[dict],
        next_page_token: Optional[str] = None
    ) -> dict:
        """Prepare request payload.

        Args:
            context: Stream partition or context dictionary.

        Returns:
            A dictionary containing the request payload.
        """
        sid = context['user_id']
        return {
            'id': sid,
            'key': 'sid',
            "fields": {
                "activity": 1,
                "device": 1,
                "engagement": 1,
                "keys": 1,
                "lifetime": 1,
                "lists": 1,
                "optout_email": 1,
                "purchase_incomplete": 1,
                "purchases": 1,
                "smart_lists": 1,
                "vars": 1
            }
        }

    def parse_response(
        self,
        response: dict,
        context: Optional[dict]
    ) -> Iterable[dict]:
        """Parse the response and return an iterator of result rows."""
        yield response
