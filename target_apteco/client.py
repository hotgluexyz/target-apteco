"""Apteco Connect API client and batch sink base."""

from __future__ import annotations

import csv
import io
import time
import uuid
from typing import Any, Dict, List, Optional
from urllib.parse import urljoin

import backoff
import requests
import singer
from hotglue_etl_exceptions import InvalidCredentialsError, InvalidPayloadError
from hotglue_singer_sdk.exceptions import FatalAPIError, RetriableAPIError
from hotglue_singer_sdk.target_sdk.client import HotglueBatchSink

from target_apteco.auth import AptecoAuthenticator

LOGGER = singer.get_logger()

TERMINAL_JOB_STATES = {"Done", "Errored", "Cancelled", "Unknown"}


class AptecoSink(HotglueBatchSink):
    """Batch sink that loads CSV rows into Apteco CDP via OrbitConnectAPI."""

    endpoint = ""
    meta_table_name: str = ""
    data_source_title: str = ""
    dedupe_rule_codes: List[str] = ["SourceURN"]
    transaction_type_name: Optional[str] = None
    amount_attribute_name: Optional[str] = None

    def __init__(self, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)
        self._authenticator = AptecoAuthenticator(dict(self.config))
        self._meta_columns_by_name: Optional[Dict[str, int]] = None
        self._cdp_source_id: Optional[int] = None

    @property
    def base_url(self) -> str:
        base = self.config.get("base_url", "https://hotglue.ca-1.apteco.cloud").rstrip("/")
        return self.config.get("connect_api_url") or f"{base}/OrbitConnectAPI"

    @property
    def data_view_name(self) -> str:
        return self.config.get("data_view_name", "DB01")

    @property
    def cdp_source_name(self) -> str:
        return self.config.get("cdp_source_name", "Blackbaud Raiser's Edge")

    @property
    def authenticator(self) -> AptecoAuthenticator:
        return self._authenticator

    def url(self, endpoint: str = "") -> str:
        path = endpoint.lstrip("/")
        return urljoin(self.base_url.rstrip("/") + "/", f"{self.data_view_name}/{path}")

    def validate_response(self, response: requests.Response) -> None:
        if response.status_code in {401, 403}:
            raise InvalidCredentialsError(response.text)
        if response.status_code == 429 or 500 <= response.status_code < 600:
            raise RetriableAPIError(self.response_error_message(response), response)
        if 400 <= response.status_code < 500:
            try:
                payload = response.json()
                message = payload.get("message") or response.text
                details = payload.get("parameters") or []
                if details:
                    message = f"{message}: {details}"
            except Exception:
                message = response.text
            raise InvalidPayloadError(message)

    @backoff.on_exception(
        backoff.expo,
        (RetriableAPIError, requests.exceptions.ConnectionError, requests.exceptions.Timeout),
        max_tries=5,
        factor=2,
    )
    def request_api(
        self,
        method: str,
        endpoint: str = "",
        params: Optional[dict] = None,
        request_data: Any = None,
        headers: Optional[dict] = None,
        files: Optional[dict] = None,
        data: Any = None,
    ) -> requests.Response:
        url = self.url(endpoint) if not endpoint.startswith("http") else endpoint
        req_headers = {**self.authenticator.auth_headers, **(headers or {})}
        if files is None and data is None and request_data is not None:
            req_headers.setdefault("Content-Type", "application/json")

        response = requests.request(
            method=method,
            url=url,
            params=params,
            json=request_data if files is None and data is None else None,
            data=data,
            files=files,
            headers=req_headers,
            timeout=120,
        )
        self.validate_response(response)
        return response

    def _as_id(self, payload: Any, key: str = "id") -> int:
        if isinstance(payload, int):
            return payload
        if isinstance(payload, dict):
            if key in payload:
                return int(payload[key])
            if "id" in payload:
                return int(payload["id"])
        raise FatalAPIError(f"Could not parse id from response: {payload}")

    def upload_temporary_file(self, filename: str, content: bytes) -> str:
        file_id = str(uuid.uuid4())
        response = self.request_api(
            "PUT",
            endpoint=f"TemporaryFiles/{file_id}",
            files={"file": (filename, content, "text/csv")},
            headers={"Accept": "application/json"},
        )
        payload = response.json() if response.text else {"id": file_id}
        return payload.get("id", file_id)

    def records_to_csv(self, records: List[dict], fieldnames: List[str]) -> bytes:
        buffer = io.StringIO()
        writer = csv.DictWriter(buffer, fieldnames=fieldnames, extrasaction="ignore")
        writer.writeheader()
        for record in records:
            writer.writerow({key: record.get(key, "") if record.get(key) is not None else "" for key in fieldnames})
        return buffer.getvalue().encode("utf-8")

    def find_data_source_by_title(self, title: str) -> Optional[dict]:
        response = self.request_api("GET", endpoint="DataSources", params={"count": 1000})
        payload = response.json()
        for item in payload.get("list", []):
            if item.get("title") == title and not item.get("deletionDate"):
                return item
        return None

    def upsert_data_source(self, title: str, temporary_file_id: str, filename: str) -> dict:
        details = {
            "type": "FileUpload",
            "delimiter": ",",
            "encloser": '"',
            "headerRow": True,
            "fileName": filename,
            "temporaryFileId": temporary_file_id,
        }
        existing = self.find_data_source_by_title(title)
        if existing:
            response = self.request_api(
                "PUT",
                endpoint=f"DataSources/{existing['id']}/Updates",
                request_data={"title": title, "type": "FileUpload", "details": details},
            )
            return response.json()

        response = self.request_api(
            "POST",
            endpoint="DataSources",
            request_data={
                "title": title,
                "type": "FileUpload",
                "details": details,
                "useEavTablePerDataSource": True,
            },
        )
        return response.json()

    def run_data_import(self, data_source_id: int, import_type: str) -> dict:
        response = self.request_api(
            "POST",
            endpoint="DataImports",
            request_data={"dataSourceId": data_source_id, "importType": import_type},
        )
        import_id = self._as_id(response.json())
        status = self.poll_job(f"DataImports/{import_id}/Status")
        if status.get("state") != "Done":
            log = self.request_api("GET", endpoint=f"DataImports/{import_id}/Log").text
            raise FatalAPIError(f"Data import {import_id} failed: {status}. Log: {log[-2000:]}")
        detail = self.request_api("GET", endpoint=f"DataImports/{import_id}").json()
        return detail

    def poll_job(self, endpoint: str, timeout_seconds: int = 300) -> dict:
        deadline = time.time() + timeout_seconds
        last = {}
        while time.time() < deadline:
            response = self.request_api("GET", endpoint=endpoint)
            last = response.json()
            state = last.get("state")
            LOGGER.info("%s -> %s (%s%% %s)", endpoint, state, last.get("progress"), last.get("operation"))
            if state in TERMINAL_JOB_STATES:
                return last
            time.sleep(2)
        raise FatalAPIError(f"Timed out waiting for {endpoint}: {last}")

    def ensure_cdp_source(self) -> int:
        if self._cdp_source_id:
            return self._cdp_source_id
        response = self.request_api("GET", endpoint="CDP/Sources")
        sources = response.json() or []
        for source in sources:
            if source.get("sourceName") == self.cdp_source_name:
                self._cdp_source_id = int(source["sourceId"])
                return self._cdp_source_id
        created = self.request_api(
            "POST",
            endpoint="CDP/Sources",
            request_data={"sourceName": self.cdp_source_name},
        ).json()
        self._cdp_source_id = int(created["sourceId"])
        return self._cdp_source_id

    def get_meta_columns(self) -> Dict[str, int]:
        if self._meta_columns_by_name is not None:
            return self._meta_columns_by_name
        response = self.request_api("GET", endpoint="CDP/MetaTables")
        columns: Dict[str, int] = {}
        for table in response.json() or []:
            for column in table.get("columns", []):
                columns[column["columnName"]] = column["metaColumnId"]
        self._meta_columns_by_name = columns
        return columns

    def get_eav_attributes(self, source_table_id: int) -> List[dict]:
        response = self.request_api("GET", endpoint=f"EAV/Tables/{source_table_id}/Attributes")
        return response.json() or []

    def get_auto_mapping(self, source_table_id: int) -> List[dict]:
        response = self.request_api(
            "GET",
            endpoint="CDP/AutoMapping",
            params={"eavSourceTableId": source_table_id},
        )
        payload = response.json() or {}
        return payload.get("columns") or []

    def _meta_columns_for_table(self, table_name: str) -> Dict[str, int]:
        response = self.request_api("GET", endpoint="CDP/MetaTables")
        for table in response.json() or []:
            if table.get("tableName") == table_name:
                return {
                    column["columnName"]: column["metaColumnId"]
                    for column in table.get("columns", [])
                }
        return {}

    def build_table_mapping_columns(self, source_table_id: int) -> List[dict]:
        """Build CDP mappings from AutoMapping, then fill gaps via same-table name match.

        Contact-point "Primary *" Individuals columns are left to AutoMapping only —
        manually mapping them triggers CDP loader errors (e.g. Invalid column EmailAddress).
        """
        eav_attributes = self.get_eav_attributes(source_table_id)
        eav_by_id = {attr["id"]: attr for attr in eav_attributes}
        auto_mapped = {
            column["eavAttributeId"]: column
            for column in self.get_auto_mapping(source_table_id)
            if column.get("metaColumnId") is not None or column.get("attributeId") is not None
        }
        transaction_type_id = self.ensure_transaction_type()
        transaction_meta_ids = set(self._meta_columns_for_table("Individual Transactions").values())

        columns: List[dict] = []
        mapped_eav_ids = set()

        def _with_txn_type(mapped: dict) -> dict:
            meta_id = mapped.get("metaColumnId")
            if (
                transaction_type_id
                and meta_id in transaction_meta_ids
                and mapped.get("transactionTypeId") is None
            ):
                mapped["transactionTypeId"] = transaction_type_id
            return mapped

        for eav_id, auto_column in auto_mapped.items():
            # Skip automap hits that point at Individuals for a Transactions sink —
            # those collide with transaction-type import requirements.
            meta_id = auto_column.get("metaColumnId")
            if self.meta_table_name == "Individual Transactions" and meta_id not in transaction_meta_ids:
                # Keep Source Unique Reference linkage from Source Unique References.
                table_for_meta = None
                for table_name, cols in [
                    ("Source Unique References", self._meta_columns_for_table("Source Unique References"))
                ]:
                    if meta_id in cols.values():
                        table_for_meta = table_name
                if table_for_meta != "Source Unique References":
                    continue

            mapped = {"eavAttributeId": eav_id}
            if auto_column.get("metaColumnId") is not None:
                mapped["metaColumnId"] = auto_column["metaColumnId"]
            if auto_column.get("attributeId") is not None:
                mapped["attributeId"] = auto_column["attributeId"]
            if auto_column.get("transactionTypeId") is not None:
                mapped["transactionTypeId"] = auto_column["transactionTypeId"]
            columns.append(_with_txn_type(mapped))
            mapped_eav_ids.add(eav_id)

        safe_meta_columns = self._meta_columns_for_table(self.meta_table_name)
        # Individuals "Primary *" fields must come from AutoMapping, not name match.
        if self.meta_table_name == "Individuals":
            safe_meta_columns = {
                name: meta_id
                for name, meta_id in safe_meta_columns.items()
                if not name.startswith("Primary ")
            }

        amount_attribute_id = None
        if self.amount_attribute_name:
            amount_attribute_id = self.ensure_amount_attribute()

        for attr in eav_attributes:
            eav_id = attr["id"]
            if eav_id in mapped_eav_ids:
                continue
            name = attr["name"]
            if name == self.amount_attribute_name and amount_attribute_id is not None:
                columns.append(
                    {
                        "eavAttributeId": eav_id,
                        "attributeId": amount_attribute_id,
                        **({"transactionTypeId": transaction_type_id} if transaction_type_id else {}),
                    }
                )
                mapped_eav_ids.add(eav_id)
                continue
            meta_column_id = safe_meta_columns.get(name)
            if meta_column_id is not None:
                columns.append(
                    _with_txn_type({"eavAttributeId": eav_id, "metaColumnId": meta_column_id})
                )
                mapped_eav_ids.add(eav_id)

        # Ensure Source Unique Reference is mapped when present in the file.
        for attr in eav_attributes:
            if attr["name"] != "Source Unique Reference" or attr["id"] in mapped_eav_ids:
                continue
            source_urn_meta = None
            for name, meta_id in self._meta_columns_for_table("Source Unique References").items():
                if name == "Source Unique Reference":
                    source_urn_meta = meta_id
            if source_urn_meta is not None:
                columns.append({"eavAttributeId": attr["id"], "metaColumnId": source_urn_meta})
                mapped_eav_ids.add(attr["id"])

        unused = [eav_by_id[i]["name"] for i in eav_by_id if i not in mapped_eav_ids]
        if unused:
            LOGGER.info("Unmapped EAV columns for %s: %s", self.name, unused)
        return columns

    def get_dedupe_rules(self) -> List[dict]:
        response = self.request_api("GET", endpoint="CDP/DedupeRules")
        rules = response.json() or []
        selected = [rule for rule in rules if rule.get("code") in self.dedupe_rule_codes]
        if not selected:
            selected = [rule for rule in rules if rule.get("includeInDefault")]
        return [{"id": rule["id"]} for rule in selected[:1]]

    def ensure_transaction_type(self) -> Optional[int]:
        if not self.transaction_type_name:
            return None
        response = self.request_api("GET", endpoint="CDP/TransactionTypes")
        for item in response.json() or []:
            if item.get("transactionTypeName") == self.transaction_type_name:
                return int(item["transactionTypeId"])
        created = self.request_api(
            "POST",
            endpoint="CDP/TransactionTypes",
            request_data={
                "transactionTypeName": self.transaction_type_name,
                "transactionTypeReferencePrefix": "GFT",
            },
        ).json()
        return int(created["transactionTypeId"])

    def ensure_amount_attribute(self) -> Optional[int]:
        """Best-effort Amount attribute on Individual Transaction Attributes."""
        try:
            response = requests.get(
                self.url("CDP/Attributes"),
                headers=self.authenticator.auth_headers,
                timeout=60,
            )
            if response.ok:
                for item in response.json() or []:
                    if item.get("description") == self.amount_attribute_name and not item.get(
                        "deletionDate"
                    ):
                        return int(item["attributeId"])

            transaction_type_id = self.ensure_transaction_type()
            metas = self.request_api("GET", endpoint="CDP/MetaTables").json() or []
            meta_table_id = next(
                (
                    table["metaTableId"]
                    for table in metas
                    if table.get("tableName") == "Individual Transaction Attributes"
                ),
                21,
            )
            response = requests.post(
                self.url("CDP/Attributes"),
                headers={**self.authenticator.auth_headers, "Content-Type": "application/json"},
                json={
                    "description": self.amount_attribute_name,
                    "variableType": "Numeric",
                    "metaTableId": meta_table_id,
                    "transactionTypeIds": [transaction_type_id] if transaction_type_id else [],
                    "addCodeToDescriptions": False,
                    "addUnclassified": False,
                    "selectable": True,
                    "browseable": True,
                    "exportable": True,
                    "exportDescriptions": False,
                    "numericType": "FixedPointDecimal",
                    "precision": 2,
                    "included": True,
                },
                timeout=60,
            )
            if not response.ok:
                LOGGER.warning(
                    "Could not create Amount attribute (%s): %s",
                    response.status_code,
                    response.text[:500],
                )
                return None
            return int(response.json()["attributeId"])
        except Exception as exc:
            LOGGER.warning("Could not create Amount attribute: %s", exc)
            return None

    def find_table_mapping(self, title: str) -> Optional[dict]:
        response = self.request_api("GET", endpoint="CDP/TableMappings", params={"count": 1000})
        for item in (response.json() or {}).get("list", []):
            if item.get("title") == title and not item.get("deletionDate"):
                return self.request_api("GET", endpoint=f"CDP/TableMappings/{item['id']}").json()
        return None

    def upsert_table_mapping(
        self,
        title: str,
        source_table_id: int,
        source_id: int,
        columns: List[dict],
        transaction_type_ids: Optional[List[int]] = None,
    ) -> int:
        dedupe_rules = self.get_dedupe_rules()
        payload = {
            "sourceTableId": source_table_id,
            "sourceId": source_id,
            "title": title,
            "columns": columns,
            "dedupeRules": dedupe_rules,
        }
        if transaction_type_ids:
            payload["transactionTypeIds"] = transaction_type_ids

        existing = self.find_table_mapping(title)
        if existing:
            self.request_api(
                "POST",
                endpoint=f"CDP/TableMappings/{existing['id']}/Updates",
                request_data=payload,
            )
            return int(existing["id"])

        created = self.request_api("POST", endpoint="CDP/TableMappings", request_data=payload).json()
        return int(created["id"])

    def run_cdp_import(self, table_mapping_id: int) -> dict:
        response = self.request_api(
            "POST",
            endpoint="CDP/Imports",
            request_data={"tableMappingId": table_mapping_id},
        )
        import_payload = response.json()
        import_id = self._as_id(import_payload)
        status = self.poll_job(f"CDP/Imports/{import_id}/Status")
        if status.get("state") != "Done":
            log = self.request_api("GET", endpoint=f"CDP/Imports/{import_id}/Log").text
            raise FatalAPIError(f"CDP import {import_id} failed: {status}. Log: {log[-2000:]}")
        return self.request_api("GET", endpoint=f"CDP/Imports/{import_id}/Summary").json()

    def map_record(self, record: dict) -> dict:
        raise NotImplementedError()

    def csv_fieldnames(self) -> List[str]:
        raise NotImplementedError()

    def process_batch_record(self, record: dict, index: int) -> dict:
        return self.map_record(record)

    def make_batch_request(self, records: List[dict]):
        if not records:
            return {"records": [], "summary": {}}

        fieldnames = self.csv_fieldnames()
        title = self.config.get(f"{self.name.lower()}_data_source_title") or self.data_source_title
        filename = f"{self.name.lower()}-{uuid.uuid4().hex[:8]}.csv"
        csv_bytes = self.records_to_csv(records, fieldnames)

        temporary_file_id = self.upload_temporary_file(filename, csv_bytes)
        existing = self.find_data_source_by_title(title)
        data_source = self.upsert_data_source(title, temporary_file_id, filename)
        data_source_id = int(data_source["id"])
        source_table_id = int(data_source.get("sourceTableId") or (existing or {}).get("sourceTableId"))

        import_type = "Update" if existing else "Create"
        import_detail = self.run_data_import(data_source_id, import_type)
        source_table_id = int(import_detail.get("dataSourceEavSourceTableId") or source_table_id)

        source_id = self.ensure_cdp_source()
        columns = self.build_table_mapping_columns(source_table_id)
        if not columns:
            raise FatalAPIError(f"No CDP column mappings could be resolved for {title}")

        transaction_type_ids = None
        transaction_type_id = self.ensure_transaction_type()
        if transaction_type_id:
            transaction_type_ids = [transaction_type_id]

        mapping_title = f"{title} -> {self.meta_table_name}"
        table_mapping_id = self.upsert_table_mapping(
            title=mapping_title,
            source_table_id=source_table_id,
            source_id=source_id,
            columns=columns,
            transaction_type_ids=transaction_type_ids,
        )
        summary = self.run_cdp_import(table_mapping_id)
        LOGGER.info("CDP import summary for %s: %s", self.name, summary)
        return {"records": records, "summary": summary}

    def handle_batch_response(self, response) -> dict:
        records = response.get("records") or []
        state_updates = []
        for record in records:
            if self.meta_table_name == "Individual Transactions":
                record_id = record.get("Transaction Source URN") or record.get(
                    "Source Unique Reference"
                )
            else:
                record_id = record.get("Source Unique Reference")
            state_updates.append({"success": True, "id": record_id})
        return {"state_updates": state_updates}
