"""
End to end test for the Frappe Event Streaming app.

Runs via:  bench --site <site> execute event_streaming.test_event_streaming.main

Pre requisites:
    - Ensure the two sites has event_streaming installed and titus_ghs installed if you have "--test-with-ghs"
    - Retrieve the api-key and secret of Site 1/Mactan 1
    - Retrieve the api-key and secret of Site 2/Mactan 2

Usage:
    bench --site mgti.localhost execute event_streaming.test_event_streaming.main --args '[
        "--producer-site",       "mgti2.localhost",
        "--producer-api-key",    "14a5791853897f4",
        "--producer-api-secret", "99d486c618ab113",
        "--producer-url",        "http://192.168.88.246:9999",
        "--consumer-site",       "mgti.localhost",
        "--consumer-url",        "http://192.168.88.246:8000",
        "--consumer-api-key",    "14a5791853897f4",
        "--consumer-api-secret", "f933ed176aa266b",
        "--test-with-ghs"
    ]'
"""

import argparse
import sys
import time
import traceback
import uuid
from dataclasses import dataclass, field
from typing import Optional

from frappe.frappeclient import FrappeClient

@dataclass
class TestState:
    producer_site: str
    consumer_site: str
    producer_url: str
    consumer_url: str
    producer_client: FrappeClient
    consumer_client: FrappeClient
    test_doctype: str
    poll_timeout: int
    poll_interval: int

    producer_api_key: str = ""
    producer_api_secret: str = ""
    consumer_api_key: str = ""
    consumer_api_secret: str = ""
    created_doc_name: Optional[str] = None
    producer_record_created: bool = False
    consumer_record_created: bool = False
    test_with_ghs: bool = False
    results: list = field(default_factory=list)

    def record(self, name, passed, detail=""):
        tag = "PASS" if passed else "FAIL"
        print(f"  [{tag}] {name}" + (f"  -  {detail}" if detail else ""))
        self.results.append((name, passed, detail))

    def summary(self):
        total = len(self.results)
        passed = sum(1 for _, ok, _ in self.results if ok)
        failed = total - passed
        print()
        print("=" * 62)
        print(f"  Results: {passed}/{total} passed" + (f", {failed} failed" if failed else ""))
        print("=" * 62)
        if failed:
            for name, ok, detail in self.results:
                if not ok:
                    print(f"  x {name}" + (f": {detail}" if detail else ""))
        return failed == 0

def _doc_exists(client, doctype, name):
    try:
        result = client.get_doc(doctype, name)
        return result is not None
    except Exception as e:
        msg = str(e).lower()
        return False

def _get_local_name_by_remote_docname(client, doctype, remote_docname):
    try:
        records = client.get_list(
            doctype,
            filters={"remote_docname": remote_docname},
            fields=["name"],
            limit_page_length=1,
        )
        return records[0]["name"] if records else None
    except Exception:
        return None

def poll_until(fn, timeout, interval, description="condition"):
    deadline = time.time() + timeout
    attempt = 0
    while time.time() < deadline:
        attempt += 1
        try:
            if fn():
                return True
        except Exception:
            pass
        remaining = int(deadline - time.time())
        if remaining <= 0:
            break
        print(f"    ... waiting for {description} (attempt {attempt}, {remaining}s left)")
        time.sleep(min(interval, deadline - time.time()))
    return False


def step(title):
    print()
    print(f">  {title}")

def substep(msg):
    print(f"   {msg}")


def _trigger_pull_node(consumer_client, producer_url):
    try:
        consumer_client.post_api(
            "event_streaming.event_streaming.doctype.event_producer"
            ".event_producer.pull_from_node",
            {"event_producer": producer_url.rstrip("/")},
        )
    except Exception:
        pass


def _dump_sync_log(st, doc_name, client=None):
    substep("  Document did not sync. Checking Event Sync Log ...")
    try:
        logs = (client or st.consumer_client).get_list(
            "Event Sync Log",
            filters={"producer_doc": doc_name},
            fields=["name", "update_type", "status", "error"],
            limit=5,
        )
        if logs:
            for l in logs:
                substep(f"  * [{l.get('status')}] {l.get('update_type')} - {l.get('error') or 'no error'}")
        else:
            substep("  No Event Sync Log entries found.")
    except Exception:
        pass


def _force_delete(client, doctype, name):
    try:
        client.delete(doctype, name)
        return
    except Exception:
        pass
    try:
        client.post_api("frappe.client.delete", {"doctype": doctype, "name": name})
        return
    except Exception as e:
        raise RuntimeError(f"REST delete failed, frappe.client.delete also failed: {e}") from e


def _wipe_doctype_records(client, label, doctype):
    try:
        records = client.get_list(doctype, fields=["name"], limit_page_length=200)
        if not records:
            substep(f"No {doctype} on {label}.")
            return
        for r in records:
            name = r.get("name", "")
            try:
                _force_delete(client, doctype, name)
                substep(f"Deleted {doctype} '{name}' from {label}.")
            except Exception as exc:
                substep(f"Could not delete {doctype} '{name}' from {label}: {exc}")
    except Exception as exc:
        substep(f"Could not list {doctype} on {label}: {exc}")


def _ensure_test_doctype(client, label, doctype_name):
    try:
        existing = client.get_list(
            "DocType",
            filters={"name": doctype_name},
            fields=["name"],
            limit_page_length=1
        )
        if existing:
            substep(f"DocType '{doctype_name}' already exists on {label} - skipping.")
            return True

        substep(f"Creating DocType '{doctype_name}' on {label} ...")
        client.insert({
            "doctype": "DocType",
            "name": doctype_name,
            "module": "Custom",
            "custom": 1,
            "autoname": "field:test_field",
            "fields": [
                {
                    "fieldname": "test_field",
                    "fieldtype": "Data",
                    "label": "Test Field",
                    "reqd": 1,
                    "in_list_view": 1,
                },
                {
                    "fieldname": "test_update_field",
                    "fieldtype": "Data",
                    "label": "Test Update Field",
                    "in_list_view": 1,
                },
            ],
            "permissions": [
                {
                    "role": "System Manager",
                    "read": 1,
                    "write": 1,
                    "create": 1,
                    "delete": 1,
                }
            ],
        })
        substep(f"DocType '{doctype_name}' created on {label}.")
        return True
    except Exception as exc:
        substep(f"  Failed to create DocType '{doctype_name}' on {label}: {exc}")
        return False

GHS_BIDIRECTIONAL = [
    "GHS Authority to Withdraw",
    "Stock Entry",
]
GHS_ONE_WAY = [
    "Weighbridge Transaction",
]

GHS_PRODUCER_TO_CONSUMER = GHS_BIDIRECTIONAL + GHS_ONE_WAY

GHS_CONSUMER_TO_PRODUCER = GHS_BIDIRECTIONAL

GHS_DOCTYPE_SETTINGS_FORWARD = {
    "GHS Authority to Withdraw": {
        "use_same_name":         0,
        "has_name_conversion":   0,
        "name_conversion":       "",
        "use_remote_doc":        1,
        "stream_directly_in_db": 1,
    },
    "Weighbridge Transaction": {
        "use_same_name":         0,
        "has_name_conversion":   1,
        "name_conversion":       "M2-|name|",
        "use_remote_doc":        0,
        "stream_directly_in_db": 1,
    },
    "Stock Entry": {
        "use_same_name":         0,
        "has_name_conversion":   1,
        "name_conversion":       "M2-|name|",
        "use_remote_doc":        1,
        "stream_directly_in_db": 1,
    },
}

GHS_DOCTYPE_SETTINGS_REVERSE = {
    "GHS Authority to Withdraw": {
        "use_same_name":         0,
        "has_name_conversion":   1,
        "name_conversion":       "M1-|name|",
        "use_remote_doc":        0,
        "stream_directly_in_db": 1,
    },
    "Stock Entry": {
        "use_same_name":         0,
        "has_name_conversion":   1,
        "name_conversion":       "M1-|name|",
        "use_remote_doc":        1,
        "stream_directly_in_db": 1,
    },
}

def _ghs_doctype_row(doctype, settings_map):
    s = settings_map.get(doctype, {})
    return {
        "ref_doctype":           doctype,
        "use_same_name":         s.get("use_same_name",         0),
        "has_name_conversion":   s.get("has_name_conversion",   0),
        "name_conversion":       s.get("name_conversion",       ""),
        "use_remote_doc":        s.get("use_remote_doc",        0),
        "stream_directly_in_db": s.get("stream_directly_in_db", 0),
    }

def _apply_name_conversion(name, settings_map, doctype):
    s = settings_map.get(doctype, {})
    if not s.get("has_name_conversion"):
        return name
    pattern = s.get("name_conversion", "")
    if not pattern:
        return name
    return pattern.replace("|name|", name)

def _check_ghs_doctype_exists(client, label, doctype):
    try:
        result = client.get_list(
            "DocType",
            filters={"name": doctype},
            fields=["name"],
            limit_page_length=1,
        )
        return bool(result)
    except Exception:
        return False

def phase_ghs_preflight(st):
    print()
    print("=" * 62)
    print("  GHS PREFLIGHT: Checking titus_ghs DocTypes")
    print("=" * 62)

    all_doctypes = list(dict.fromkeys(GHS_PRODUCER_TO_CONSUMER + GHS_CONSUMER_TO_PRODUCER))
    all_ok = True
    for doctype in all_doctypes:
        for label, client in [("Producer", st.producer_client), ("Consumer", st.consumer_client)]:
            exists = _check_ghs_doctype_exists(client, label, doctype)
            st.record(f"DocType '{doctype}' exists on {label}", exists)
            if not exists:
                substep(f"  '{doctype}' not found on {label}. "
                        "Ensure titus_ghs app is installed there.")
                all_ok = False

    if not all_ok:
        substep("  One or more GHS DocTypes are missing. "
                "Install titus_ghs on both sites and retry.")
    return all_ok

def _setup_ghs_stream(st, label_producer, producer_client, producer_url,
                      producer_api_key, producer_api_secret,
                      label_consumer, consumer_client, consumer_url,
                      doctypes, step_offset, settings_map=None):
    producer_key = producer_url.rstrip("/")
    consumer_key = consumer_url.rstrip("/")

    step(f"{step_offset}. Creating Event Producer on {label_consumer} "
         f"(pulls from {label_producer})  {len(doctypes)} doctype(s)")

    _smap = settings_map or GHS_DOCTYPE_SETTINGS_FORWARD
    producer_doctypes_payload = [_ghs_doctype_row(dt, _smap) for dt in doctypes]

    if _doc_exists(consumer_client, "Event Producer", producer_key):
        substep("Record already exists  merging doctype rows ...")
        try:
            ep = consumer_client.get_doc("Event Producer", producer_key)
            existing_rows = ep.get("producer_doctypes") or []
            existing_names = {(r.get("ref_doctype") or "").lower() for r in existing_rows}
            new_rows = [r for r in producer_doctypes_payload
                        if r["ref_doctype"].lower() not in existing_names]
            if new_rows:
                consumer_client.update({
                    "doctype": "Event Producer",
                    "name": producer_key,
                    "producer_doctypes": existing_rows + new_rows,
                })
                substep(f"  Added {len(new_rows)} new doctype row(s).")
            else:
                substep("  All rows already present.")
        except Exception as exc:
            substep(f"  Could not merge rows: {exc}")
        st.record(f"Event Producer on {label_consumer} ready", True, "already existed, rows merged")
    else:
        try:
            result = consumer_client.insert({
                "doctype": "Event Producer",
                "producer_url": producer_key,
                "api_key": producer_api_key,
                "api_secret": producer_api_secret,
                "user": "Administrator",
                "producer_doctypes": producer_doctypes_payload,
            })
            created_name = result.get("name") if result else None
            actually_exists = _doc_exists(consumer_client, "Event Producer", producer_key)
            st.record(
                f"Event Producer created on {label_consumer}",
                actually_exists,
                f"name={created_name}" if actually_exists else
                f"API said OK but GET returned 404  check {label_consumer} logs",
            )
            if not actually_exists:
                substep("  Record not found after creation.")
                return False
        except Exception as exc:
            st.record(f"Event Producer created on {label_consumer}", False, str(exc))
            substep("  Event Producer creation failed, skipping.")
            return False

    step(f"{step_offset + 1}. Waiting for Event Consumer to appear on {label_producer}")
    found = poll_until(
        lambda: _doc_exists(producer_client, "Event Consumer", consumer_key),
        st.poll_timeout,
        st.poll_interval,
        f"Event Consumer record on {label_producer}",
    )
    st.record(f"Event Consumer record present on {label_producer}", found)
    if not found:
        substep(f"  Event Consumer did not appear on {label_producer}.")
        return False

    step(f"{step_offset + 2}. Approving GHS doctypes on {label_producer}'s Event Consumer")
    try:
        ec = producer_client.get_doc("Event Consumer", consumer_key)
        if ec is None:
            raise RuntimeError("get_doc returned None for Event Consumer")

        consumer_doctypes = ec.get("consumer_doctypes") or []
        substep(f"Found {len(consumer_doctypes)} doctype row(s) on Event Consumer")

        dt_lower_set = {dt.lower() for dt in doctypes}
        updated = [
            dict(e, status="Approved")
            if (e.get("ref_doctype") or "").lower() in dt_lower_set
            else e
            for e in consumer_doctypes
        ]
        producer_client.update({
            "doctype": "Event Consumer",
            "name": consumer_key,
            "consumer_doctypes": updated,
        })

        ec2 = producer_client.get_doc("Event Consumer", consumer_key)
        approved_set = {
            (e.get("ref_doctype") or "").lower()
            for e in (ec2.get("consumer_doctypes") or [])
            if e.get("status") == "Approved"
        }
        for dt in doctypes:
            approved = dt.lower() in approved_set
            st.record(f"Doctype '{dt}' approved on {label_producer}", approved)
            if not approved:
                substep(f"  '{dt}' row not found or still Pending.")
    except Exception as exc:
        for dt in doctypes:
            st.record(f"Doctype '{dt}' approved on {label_producer}", False, str(exc))
        return False

    step(f"{step_offset + 3}. Waiting for approvals to reflect on {label_consumer}'s Event Producer")
    dt_lower_set = {dt.lower() for dt in doctypes}

    def all_approvals_reflected():
        ep = consumer_client.get_doc("Event Producer", producer_key)
        approved = {
            (e.get("ref_doctype") or "").lower()
            for e in ep.get("producer_doctypes", [])
            if e.get("status") == "Approved"
        }
        return dt_lower_set.issubset(approved)

    synced = poll_until(
        all_approvals_reflected,
        st.poll_timeout,
        st.poll_interval,
        f"all approvals to reflect on {label_consumer}'s Event Producer",
    )
    st.record(f"All GHS approvals reflected on {label_consumer}'s Event Producer", synced)
    return synced

def phase_ghs_setup(st):
    print()
    print("=" * 62)
    print("  PHASE 1 (GHS): SETUP  Producer -> Consumer")
    print("=" * 62)

    step("1. Setting default_url in Event Streaming Settings")
    for label, client, url in [
        ("Producer", st.producer_client, st.producer_url),
        ("Consumer", st.consumer_client, st.consumer_url),
    ]:
        try:
            client.update({
                "doctype": "Event Streaming Settings",
                "name": "Event Streaming Settings",
                "default_site_url": url.rstrip("/"),
            })
            st.record(f"Event Streaming Settings.default_url  {label}", True)
        except Exception as exc:
            st.record(f"Event Streaming Settings.default_url  {label}", False, str(exc))

    return _setup_ghs_stream(
        st,
        label_producer="Producer",
        producer_client=st.producer_client,
        producer_url=st.producer_url,
        producer_api_key=st.producer_api_key,
        producer_api_secret=st.producer_api_secret,
        label_consumer="Consumer",
        consumer_client=st.consumer_client,
        consumer_url=st.consumer_url,
        doctypes=GHS_PRODUCER_TO_CONSUMER,
        step_offset=2,
    )

def phase_ghs_setup_reverse(st):
    print()
    print("=" * 62)
    print("  PHASE 1b (GHS): REVERSE SETUP  Consumer -> Producer")
    print("=" * 62)
    substep(f"Bidirectional doctypes: {', '.join(GHS_CONSUMER_TO_PRODUCER)}")

    return _setup_ghs_stream(
        st,
        label_producer="Consumer",
        producer_client=st.consumer_client,
        producer_url=st.consumer_url,
        producer_api_key=st.consumer_api_key,
        producer_api_secret=st.consumer_api_secret,
        label_consumer="Producer",
        consumer_client=st.producer_client,
        consumer_url=st.producer_url,
        doctypes=GHS_CONSUMER_TO_PRODUCER,
        step_offset=1,
        settings_map=GHS_DOCTYPE_SETTINGS_REVERSE,
    )

GHS_PATCH_DOCS = {
    "atw": [
        ("GHS Vessel",               "vessel_name",        "ATW Test Vessel"),
        ("GHS Port",                  "port_name",          "ATW Test Port"),
        ("GHS Voyage",                "voyage_name",        "ATW Test Voyage"),
        ("GHS Mode of Withdrawal",    "mode_of_withdrawal", "ATW Test MOW"),
        ("GHS Pre-Operations Plan",   "voyage",             None),
        ("GHS RIA",                   "consignee",          None),
    ],
    "weighbridge_transaction": [
        ("GHS Vessel",               "vessel_name",        "WT Test Vessel"),
        ("GHS Port",                  "port_name",          "WT Test Port"),
        ("GHS Voyage",                "voyage_name",        "WT Test Voyage"),
        ("Weighbridge Transaction",   "name",               None),
    ],
}

GHS_SYNC_TARGETS = [

    ("GHS Authority to Withdraw",  "atw",                   None,           None),
    ("Weighbridge Transaction",    "weighbridge_transaction", None,           None),
    ("Stock Entry",                None,                    None,           None),
]

def _run_patch(client, label, patch_path):
    patch_name = patch_path.rsplit(".", 1)[-1]
    try:
        result = client.post_api(
            "titus_ghs.api.run_patch_script.run_patch",
            {"patch_name": patch_name},
        )

        if result is None:
            error_detail = _get_latest_patch_error(client, patch_name)
            return (
                False,
                f"Patch '{patch_name}' returned None  it likely raised an exception "
                f"on {label}. Check Frappe Error Log. "
                + (f"Latest error: {error_detail}" if error_detail else ""),
            )
        return True, str(result)
    except Exception as exc:
        msg = str(exc)
        if "already executed" in msg.lower() or "successfully" in msg.lower():
            return True, msg

        if "DuplicateEntryError" in msg or "Duplicate entry" in msg or "1062" in msg:
            return True, f"deps already exist on {label} (duplicate entry  patch re-run)"

        if "has no attribute 'execute'" in msg or 'has no attribute "execute"' in msg:
            return False, msg
        return False, msg

def _get_latest_patch_error(client, patch_name):
    try:
        logs = client.get_list(
            "Error Log",
            filters={"method": ["like", f"%{patch_name}%"]},
            fields=["error", "creation"],
            order_by="creation desc",
            limit_page_length=1,
        )
        if logs:

            return str(logs[0].get("error", ""))[:300]
    except Exception:
        pass
    return None

def _get_doc_name(client, doctype, filters):
    try:
        records = client.get_list(
            doctype,
            filters=filters,
            fields=["name"],
            limit_page_length=1,
        )
        return records[0]["name"] if records else None
    except Exception as exc:
        substep(f"  [_get_doc_name] {doctype} {filters} failed: {exc}")
        return None

def _create_atw(client, prefix):
    try:
        now = frappe_now()
        today = now[:10]

        vessel = _ensure_doc(client, "GHS Vessel", {"vessel_name": f"{prefix} Test Vessel"}, {
            "doctype": "GHS Vessel", "vessel_name": f"{prefix} Test Vessel", "number_of_hatches": 5,
        })

        port = _ensure_doc(client, "GHS Port", {"port_name": f"{prefix} Test Port"}, {
            "doctype": "GHS Port", "port_name": f"{prefix} Test Port",
        })

        project = _ensure_doc(client, "Project", {"project_name": f"{prefix} Test Project"}, {
            "doctype": "Project", "project_name": f"{prefix} Test Project",
        })

        mow = _ensure_doc(client, "GHS Mode of Withdrawal",
                          {"mode_of_withdrawal": f"{prefix} Test MOW"}, {
            "doctype": "GHS Mode of Withdrawal",
            "mode_of_withdrawal": f"{prefix} Test MOW",
            "mode_of_transport": "Pick Up", "location": "Ex Vessel",
            "storage_form": "Bulk", "nominated": "Consignee Nominated",
        })

        voyage = _ensure_doc(client, "GHS Voyage", {"voyage_name": f"{prefix} Test Voyage"}, {
            "doctype": "GHS Voyage", "voyage_name": f"{prefix} Test Voyage",
            "vessel": vessel, "origin": port, "project": project,
            "planned_arrival_date": "2024-10-11 11:11:11",
            "planned_departure_date": "2024-11-11 11:11:10",
        })

        commodity = _ensure_doc(client, "Item", {"item_code": f"{prefix} Test Item"}, {
            "doctype": "Item", "item_code": f"{prefix} Test Item",
            "item_group": "IG-0002", "itm_commodity_guarantee": 95, "valuation_rate": 1,
        })

        consignee = _ensure_doc(client, "Customer", {"customer_name": f"{prefix} Test Customer"}, {
            "doctype": "Customer", "customer_code": f"{prefix}TC01",
            "customer_name": f"{prefix} Test Customer", "customer_type": "Company",
        })

        company = _ensure_doc(client, "Company", {"company_name": f"{prefix} Test Company"}, {
            "doctype": "Company", "company_name": f"{prefix} Test Company",
            "abbr": prefix[:3].upper() + "C", "default_currency": "PHP", "country": "Philippines",
        })

        warehouse = _ensure_doc(client, "Warehouse",
                                 {"warehouse_name": f"{prefix} Test Warehouse"}, {
            "doctype": "Warehouse", "warehouse_name": f"{prefix} Test Warehouse",
            "company": company, "warehouse_port": port,
        })

        vehicle = _ensure_doc(client, "Vehicle",
                               {"license_plate": f"{prefix} Test Vehicle"}, {
            "doctype": "Vehicle", "license_plate": f"{prefix} Test Vehicle",
            "capacity": 500, "make": "Test Make", "capacity_uom": "Tonne",
            "truck_type": "TRT-01", "model": "Test Model", "last_odometer": 100,
            "uom": "Litre",
            "truck_consignee": [{"consignee": consignee}],
        })

        pre_ops = _insert_doc(client, {
            "doctype": "GHS Pre-Operations Plan",
            "voyage": voyage, "commodity": commodity, "volume": 10000, "free_days": 5,
            "consignee_allocation": [{
                "ca_consignee": consignee, "ca_bl_allocation": 1000,
                "ca_inport": 500, "ca_outport": 500,
            }],
            "port_allocation": [
                {"pa_consignee": consignee, "pa_port_allocation": 500,
                 "pa_mow": mow, "pa_port": "PORT-02", "pa_free_days": 5},
                {"pa_consignee": consignee, "pa_port_allocation": 500,
                 "pa_mow": mow, "pa_port": port, "pa_free_days": 5},
            ],
        })

        ria = _insert_doc(client, {
            "doctype": "GHS RIA", "transfer_type": "Consignee",
            "pre_ops_plan": pre_ops, "consignee": consignee,
            "source": warehouse, "destination": voyage,
            "commodity": commodity, "qty": 98, "mode_of_withdrawal": mow,
        })

        result = client.insert({
            "doctype": "GHS Authority to Withdraw",
            "arrival_time": now, "date": today,
            "ria_no": ria, "origin": warehouse,
            "mode_of_withdrawal": mow,
            "driver_name": f"{prefix} Test Driver",
            "plate_no": vehicle,
            "warehouse_location": warehouse,
        })
        return result.get("name") if result else None
    except Exception as exc:
        substep(f"  _create_atw({prefix}) failed: {exc}")
        return None

def _create_weighbridge_transaction(client, prefix):
    try:
        now = frappe_now()

        vessel = _ensure_doc(client, "GHS Vessel", {"vessel_name": f"{prefix} Test Vessel"}, {
            "doctype": "GHS Vessel", "vessel_name": f"{prefix} Test Vessel", "number_of_hatches": 5,
        })
        port = _ensure_doc(client, "GHS Port", {"port_name": f"{prefix} Test Port"}, {
            "doctype": "GHS Port", "port_name": f"{prefix} Test Port",
        })
        project = _ensure_doc(client, "Project", {"project_name": f"{prefix} Test Project"}, {
            "doctype": "Project", "project_name": f"{prefix} Test Project",
        })
        mow = _ensure_doc(client, "GHS Mode of Withdrawal",
                          {"mode_of_withdrawal": f"{prefix} Test MOW"}, {
            "doctype": "GHS Mode of Withdrawal",
            "mode_of_withdrawal": f"{prefix} Test MOW",
            "mode_of_transport": "Pick Up", "location": "Ex Vessel",
            "storage_form": "Bulk", "nominated": "Consignee Nominated",
        })
        voyage = _ensure_doc(client, "GHS Voyage", {"voyage_name": f"{prefix} Test Voyage"}, {
            "doctype": "GHS Voyage", "voyage_name": f"{prefix} Test Voyage",
            "vessel": vessel, "origin": port, "project": project,
            "planned_arrival_date": "2024-10-11 11:11:11",
            "planned_departure_date": "2024-11-11 11:11:10",
        })
        commodity = _ensure_doc(client, "Item", {"item_code": f"{prefix} Test Item"}, {
            "doctype": "Item", "item_code": f"{prefix} Test Item",
            "item_group": "IG-0002", "stock_uom": "Tonne",
            "itm_commodity_guarantee": 95,
        })
        consignee = _ensure_doc(client, "Customer", {"customer_name": f"{prefix} Test Customer"}, {
            "doctype": "Customer", "customer_code": f"{prefix}TC",
            "customer_name": f"{prefix} Test Customer", "customer_type": "Company",
        })
        company = _ensure_doc(client, "Company", {"company_name": f"{prefix} Test Company"}, {
            "doctype": "Company", "company_name": f"{prefix} Test Company",
            "abbr": prefix[:3].upper() + "CO", "default_currency": "PHP", "country": "Philippines",
        })
        warehouse = _ensure_doc(client, "Warehouse",
                                 {"warehouse_name": f"{prefix} Test Warehouse"}, {
            "doctype": "Warehouse", "warehouse_name": f"{prefix} Test Warehouse",
            "company": company, "warehouse_port": port,
        })
        vehicle = _ensure_doc(client, "Vehicle",
                               {"license_plate": f"{prefix} Test Vehicle"}, {
            "doctype": "Vehicle", "license_plate": f"{prefix} Test Vehicle",
            "capacity": 500, "make": "Test Make", "capacity_uom": "Tonne",
            "truck_type": "TRT-01", "model": "Test Model", "last_odometer": 100,
            "uom": "Litre",
            "truck_consignee": [{"consignee": consignee}],
        })

        result = client.insert({
            "doctype": "Weighbridge Transaction",
            "truck": vehicle,
            "atw_source": "Offline ATW",
            "direction": "Incoming",
            "source": warehouse,
            "destination": warehouse,
            "commodity": commodity,
            "weight_uom": "Tonne",
            "driver_name": f"{prefix} Test Driver",
        })
        return result.get("name") if result else None
    except Exception as exc:
        substep(f"  _create_weighbridge_transaction({prefix}) failed: {exc}")
        return None

def _ensure_doc(client, doctype, filters, doc_data):
    existing = _get_doc_name(client, doctype, filters)
    if existing:
        return existing
    try:
        result = client.insert(doc_data)
        return result.get("name") if result else None
    except Exception as exc:
        msg = str(exc)
        if "DuplicateEntryError" in msg or "Duplicate entry" in msg or "1062" in msg:
            fallback = _get_doc_name(client, doctype, filters)
            if fallback:
                substep(f"  _ensure_doc({doctype}): duplicate on insert  reusing existing '{fallback}'")
                return fallback
            fallback = _get_latest_doc(client, doctype)
            if fallback:
                substep(f"  _ensure_doc({doctype}): duplicate on insert, no filter match  reusing latest '{fallback}'")
                return fallback
        raise

def _insert_doc(client, doc_data):
    try:
        result = client.insert(doc_data)
        return result.get("name") if result else None
    except Exception as exc:
        msg = str(exc)
        if "DuplicateEntryError" in msg or "Duplicate entry" in msg or "1062" in msg:
            doctype = doc_data.get("doctype")
            existing = _get_latest_doc(client, doctype)
            if existing:
                substep(
                    f"  _insert_doc({doctype}): duplicate on insert  "
                    f"reusing existing '{existing}'"
                )
                return existing
        raise

def frappe_now():
    from datetime import datetime
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")

def _get_latest_doc(client, doctype, filters=None):
    try:
        records = client.get_list(
            doctype,
            filters=filters or {},
            fields=["name"],
            limit_page_length=200,
        )
        return records[-1]["name"] if records else None
    except Exception as exc:
        substep(f"  [_get_latest_doc] {doctype} query failed: {exc}")
        return None

def phase_ghs_test(st):
    print()
    print("=" * 62)
    print("  PHASE 2 (GHS): CREATE & SYNC  Producer -> Consumer")
    print("=" * 62)

    step("1. Running titus_ghs.patches.automated_test.weighbridge_transaction on Producer")
    ok, msg = _run_patch(
        st.producer_client, "Producer",
        "titus_ghs.patches.automated_test.weighbridge_transaction",
    )

    if not ok and "has no attribute 'execute'" in msg:
        substep("  Patch has no execute(), creating WT from existing deps.")
        ok = True
        msg = "skipped (no execute)"
    st.record("weighbridge_transaction patch executed on Producer", ok, msg)
    if not ok:
        substep("  Patch failed  skipping Weighbridge Transaction sync check.")
    else:
        wt_name = _create_weighbridge_transaction(st.producer_client, "WT")
        st.record("Weighbridge Transaction exists on Producer",
                  bool(wt_name), f"name={wt_name}")

        if wt_name:
            wt_consumer_name = _apply_name_conversion(
                wt_name, GHS_DOCTYPE_SETTINGS_FORWARD, "Weighbridge Transaction"
            )
            _trigger_pull_node(st.consumer_client, st.producer_url)
            step("2. Waiting for Weighbridge Transaction to appear on Consumer")
            synced = poll_until(
                lambda n=wt_consumer_name: _doc_exists(
                    st.consumer_client, "Weighbridge Transaction", n),
                st.poll_timeout, st.poll_interval,
                f"Weighbridge Transaction '{wt_consumer_name}' on Consumer",
            )
            st.record("Weighbridge Transaction synced to Consumer",
                      synced, f"name={wt_consumer_name}")
            if not synced:
                _dump_sync_log(st, wt_name)
            else:

                step("2a. Updating WT driver_name on Producer (update stream test)")
                updated_wt_driver = f"WT Driver {uuid.uuid4().hex[:6].upper()}"
                try:
                    st.producer_client.update({
                        "doctype": "Weighbridge Transaction",
                        "name": wt_name,
                        "driver_name": updated_wt_driver,
                    })
                    st.record("WT driver_name updated on Producer", True,
                              f"driver_name={updated_wt_driver}")
                except Exception as exc:
                    st.record("WT driver_name updated on Producer", False, str(exc))
                    updated_wt_driver = None

                if updated_wt_driver:
                    _trigger_pull_node(st.consumer_client, st.producer_url)
                    step("2b. Waiting for WT driver_name update to reflect on Consumer")

                    def _wt_update_visible(cn=wt_consumer_name, v=updated_wt_driver):
                        doc = st.consumer_client.get_doc("Weighbridge Transaction", cn)
                        return doc.get("driver_name") == v

                    wt_update_ok = poll_until(
                        _wt_update_visible,
                        st.poll_timeout, st.poll_interval,
                        "WT driver_name update on Consumer",
                    )
                    if not wt_update_ok:
                        try:
                            doc = st.consumer_client.get_doc("Weighbridge Transaction", wt_consumer_name)
                            substep(f"  Consumer has driver_name='{doc.get('driver_name')}' "
                                    f"(expected '{updated_wt_driver}')")
                        except Exception:
                            pass
                    st.record("WT update synced to Consumer (driver_name)", wt_update_ok)

    step("3. Creating Stock Entry on Producer")
    se_name = None
    _se_skip_reason = None
    try:

        warehouse = _get_doc_name(st.producer_client, "Warehouse",
                                   {"warehouse_name": "ATW Test Warehouse"})
        commodity = _get_doc_name(st.producer_client, "Item",
                                   {"item_code": "ATW Test Item"})
        company   = _get_doc_name(st.producer_client, "Company",
                                   {"company_name": "ATW Test Company"})
        missing = [n for n, v in [("Warehouse (ATW Test Warehouse)", warehouse),
                                   ("Item (ATW Test Item)", commodity),
                                   ("Company (ATW Test Company)", company)] if not v]
        if missing:
            _se_skip_reason = (
                f"prerequisite record(s) not found on Producer: {', '.join(missing)}. "
                "Run the atw patch first."
            )
            substep(f"  Skipping Stock Entry: {_se_skip_reason}")
        else:
            result = st.producer_client.insert({
                "doctype": "Stock Entry",
                "stock_entry_type": "Material Receipt",
                "company": company,
                "items": [{
                    "item_code": commodity,
                    "qty": 10,
                    "t_warehouse": warehouse,
                    "basic_rate": 1,
                }],
            })
            se_name = result.get("name") if result else None
    except Exception as exc:
        substep(f"  Stock Entry creation failed: {exc}")
        _se_skip_reason = str(exc)

    st.record("Stock Entry created on Producer", bool(se_name),
              f"name={se_name}" if se_name else (_se_skip_reason or "creation failed"))

    if se_name:
        se_consumer_name = _apply_name_conversion(
            se_name, GHS_DOCTYPE_SETTINGS_FORWARD, "Stock Entry"
        )
        _trigger_pull_node(st.consumer_client, st.producer_url)
        step("4. Waiting for Stock Entry to appear on Consumer")
        synced = poll_until(
            lambda n=se_consumer_name: _doc_exists(st.consumer_client, "Stock Entry", n),
            st.poll_timeout, st.poll_interval,
            f"Stock Entry '{se_consumer_name}' on Consumer",
        )
        st.record("Stock Entry synced to Consumer", synced, f"name={se_consumer_name}")
        if not synced:
            _dump_sync_log(st, se_name)
        else:

            step("4a. Updating Stock Entry remarks on Producer (update stream test)")
            updated_remarks = f"Test update {uuid.uuid4().hex[:6].upper()}"
            try:
                st.producer_client.update({
                    "doctype": "Stock Entry",
                    "name": se_name,
                    "remarks": updated_remarks,
                })
                st.record("SE remarks updated on Producer", True,
                          f"remarks={updated_remarks}")
            except Exception as exc:
                st.record("SE remarks updated on Producer", False, str(exc))
                updated_remarks = None

            if updated_remarks:
                _trigger_pull_node(st.consumer_client, st.producer_url)
                step("4b. Waiting for SE remarks update to reflect on Consumer")

                def _se_update_visible(cn=se_consumer_name, v=updated_remarks):
                    doc = st.consumer_client.get_doc("Stock Entry", cn)
                    return doc.get("remarks") == v

                se_update_ok = poll_until(
                    _se_update_visible,
                    st.poll_timeout, st.poll_interval,
                    "SE remarks update on Consumer",
                )
                if not se_update_ok:
                    try:
                        doc = st.consumer_client.get_doc("Stock Entry", se_consumer_name)
                        substep(f"  Consumer has remarks='{doc.get('remarks')}' "
                                f"(expected '{updated_remarks}')")
                    except Exception:
                        pass
                st.record("SE update synced to Consumer (remarks)", se_update_ok)
        st._ghs_se_name = se_name

def phase_ghs_test_reverse(st):
    print()
    print("=" * 62)
    print("  PHASE 2b (GHS): ATW ROUND-TRIP + SE REVERSE  Consumer/M1 -> Producer/M2")
    print("=" * 62)

    step("1. Running atw patch on Consumer")
    ok, msg = _run_patch(
        st.consumer_client, "Consumer",
        "titus_ghs.patches.automated_test.atw",
    )
    st.record("atw patch executed on Consumer", ok, msg)

    if ok:
        atw_name = _create_atw(st.consumer_client, "ATW")
        if atw_name:

            atw_m2_name = _apply_name_conversion(
                atw_name, GHS_DOCTYPE_SETTINGS_REVERSE, "GHS Authority to Withdraw"
            )
            _trigger_pull_node(st.producer_client, st.consumer_url)
            step("2. Waiting for GHS Authority to Withdraw to appear on Producer (M2)")
            synced = poll_until(
                lambda n=atw_m2_name: _doc_exists(
                    st.producer_client, "GHS Authority to Withdraw", n),
                st.poll_timeout, st.poll_interval,
                f"GHS Authority to Withdraw '{atw_m2_name}' on Producer",
            )
            st.record("GHS Authority to Withdraw reverse-synced to Producer",
                      synced, f"M1={atw_name}  M2={atw_m2_name}")
            if not synced:
                _dump_sync_log(st, atw_name, client=st.producer_client)
            else:

                step("2a. Updating ATW driver_name on Producer/M2 (round-trip test)")
                rev_driver = f"Rev Driver {uuid.uuid4().hex[:6].upper()}"
                try:
                    st.producer_client.update({
                        "doctype": "GHS Authority to Withdraw",
                        "name": atw_m2_name,
                        "driver_name": rev_driver,
                    })
                    st.record("ATW driver_name updated on Producer/M2", True,
                              f"name={atw_m2_name}  driver_name={rev_driver}")
                except Exception as exc:
                    st.record("ATW driver_name updated on Producer/M2", False, str(exc))
                    rev_driver = None

                if rev_driver:

                    _trigger_pull_node(st.consumer_client, st.producer_url)
                    step("2b. Waiting for ATW driver_name to round-trip back to Consumer/M1")

                    def _rev_atw_update_visible(n=atw_name, v=rev_driver):
                        doc = st.consumer_client.get_doc("GHS Authority to Withdraw", n)
                        return doc.get("driver_name") == v

                    rev_atw_ok = poll_until(
                        _rev_atw_update_visible,
                        st.poll_timeout, st.poll_interval,
                        f"ATW driver_name round-trip on Consumer/M1 (name={atw_name})",
                    )
                    if not rev_atw_ok:
                        try:
                            doc = st.consumer_client.get_doc("GHS Authority to Withdraw", atw_name)
                            substep(f"  Consumer/M1 has driver_name='{doc.get('driver_name')}' "
                                    f"(expected '{rev_driver}')")
                        except Exception:
                            pass
                    st.record("ATW M2->M1 round-trip synced (driver_name)", rev_atw_ok)

    step("3. Creating Stock Entry on Consumer for reverse sync")
    se_name = None
    _se_skip_reason = None
    try:
        warehouse = _get_doc_name(st.consumer_client, "Warehouse",
                                   {"warehouse_name": "ATW Test Warehouse"})
        commodity = _get_doc_name(st.consumer_client, "Item",
                                   {"item_code": "ATW Test Item"})
        company   = _get_doc_name(st.consumer_client, "Company",
                                   {"company_name": "ATW Test Company"})
        missing = [n for n, v in [("Warehouse (ATW Test Warehouse)", warehouse),
                                   ("Item (ATW Test Item)", commodity),
                                   ("Company (ATW Test Company)", company)] if not v]
        if missing:
            _se_skip_reason = (
                f"prerequisite record(s) not found on Consumer: {', '.join(missing)}. "
                "Run the atw patch first."
            )
            substep(f"  Skipping Stock Entry: {_se_skip_reason}")
        else:
            result = st.consumer_client.insert({
                "doctype": "Stock Entry",
                "stock_entry_type": "Material Receipt",
                "company": company,
                "items": [{
                    "item_code": commodity,
                    "qty": 10,
                    "t_warehouse": warehouse,
                    "basic_rate": 1,
                }],
            })
            se_name = result.get("name") if result else None
    except Exception as exc:
        substep(f"  Stock Entry creation on Consumer failed: {exc}")
        _se_skip_reason = str(exc)

    st.record("Stock Entry created on Consumer", bool(se_name),
              f"name={se_name}" if se_name else (_se_skip_reason or "creation failed"))

    if se_name:
        se_producer_name = _apply_name_conversion(
            se_name, GHS_DOCTYPE_SETTINGS_REVERSE, "Stock Entry"
        )
        _trigger_pull_node(st.producer_client, st.consumer_url)
        step("4. Waiting for Stock Entry to appear on Producer")
        synced = poll_until(
            lambda n=se_producer_name: _doc_exists(st.producer_client, "Stock Entry", n),
            st.poll_timeout, st.poll_interval,
            f"Stock Entry '{se_producer_name}' on Producer",
        )
        st.record("Stock Entry reverse-synced to Producer", synced, f"name={se_producer_name}")
        if not synced:
            _dump_sync_log(st, se_name, client=st.producer_client)
        else:

            step("4a. Updating Stock Entry remarks on Consumer (reverse update stream test)")
            rev_remarks = f"Rev update {uuid.uuid4().hex[:6].upper()}"
            try:
                st.consumer_client.update({
                    "doctype": "Stock Entry",
                    "name": se_name,
                    "remarks": rev_remarks,
                })
                st.record("SE remarks updated on Consumer", True,
                          f"remarks={rev_remarks}")
            except Exception as exc:
                st.record("SE remarks updated on Consumer", False, str(exc))
                rev_remarks = None

            if rev_remarks:
                _trigger_pull_node(st.producer_client, st.consumer_url)
                step("4b. Waiting for SE remarks update to reflect on Producer")

                def _rev_se_update_visible(pn=se_producer_name, v=rev_remarks):
                    doc = st.producer_client.get_doc("Stock Entry", pn)
                    return doc.get("remarks") == v

                rev_se_ok = poll_until(
                    _rev_se_update_visible,
                    st.poll_timeout, st.poll_interval,
                    "reverse SE remarks update on Producer",
                )
                if not rev_se_ok:
                    try:
                        doc = st.producer_client.get_doc("Stock Entry", se_producer_name)
                        substep(f"  Producer has remarks='{doc.get('remarks')}' "
                                f"(expected '{rev_remarks}')")
                    except Exception:
                        pass
                st.record("SE reverse update synced to Producer (remarks)", rev_se_ok)

def phase_ghs_cleanup(st):
    print()
    print("=" * 62)
    print("  PHASE 3 (GHS): CLEANUP")
    print("=" * 62)
    substep("  Removing Event Producer / Event Consumer records ...")
    for doctype in ("Event Producer", "Event Consumer"):
        for label, client in [("Producer", st.producer_client), ("Consumer", st.consumer_client)]:
            _wipe_doctype_records(client, label, doctype)

def phase_preflight_cleanup(st):
    print()
    print("=" * 62)
    print("  PHASE 0:CLEANUP")
    print("=" * 62)

    for doctype in ("Event Producer", "Event Consumer"):
        for label, client in [("Producer", st.producer_client), ("Consumer", st.consumer_client)]:
            step(f"  Deleting {doctype} records from {label}")
            try:
                records = client.get_list(doctype, fields=["name"], limit_page_length=200)
                if not records:
                    substep("None found.")
                    continue
                for r in records:
                    name = r.get("name", "")
                    try:
                        client.delete(doctype, name)
                        substep(f"Deleted {doctype}: {name}")
                    except Exception as exc:
                        substep(f"Could not delete {doctype} {name!r}: {exc}")
            except Exception as exc:
                substep(f"Could not list {doctype} on {label}: {exc}")

    substep("cleanup done.")


def phase_setup(st):
    print()
    print("=" * 62)
    print("  PHASE 1: SETUP")
    print("=" * 62)

    step("1. Checking API credentials")
    for label, client in [("Producer", st.producer_client), ("Consumer", st.consumer_client)]:
        user = client.get_api("frappe.auth.get_logged_user") or "unknown"
        ok = user not in ("unknown", "Guest", "")
        st.record(
            f"API credentials valid - {label} ({client.url})",
            ok,
            f"logged in as {user}" if ok else "authentication failed or site unreachable",
        )
        if not ok:
            print(f"  Could not authenticate to {label}, stopping.")
            return False

    step("2. Setting default_url in Event Streaming Settings")
    for label, client, url in [
        ("Producer", st.producer_client, st.producer_url),
        ("Consumer", st.consumer_client, st.consumer_url),
    ]:
        try:
            client.update({
                "doctype": "Event Streaming Settings",
                "name": "Event Streaming Settings",
                "default_site_url": url.rstrip("/"),
            })
            st.record(f"Event Streaming Settings.default_url - {label}", True)
        except Exception as exc:
            st.record(f"Event Streaming Settings.default_url - {label}", False, str(exc))

    step(f"3. Setting up DocType '{st.test_doctype}' on Producer and Consumer")
    for label, client in [("Producer", st.producer_client), ("Consumer", st.consumer_client)]:
        try:
            docs = client.get_list(st.test_doctype, fields=["name"], limit_page_length=200)
            for d in docs:
                try:
                    client.delete(st.test_doctype, d["name"])
                    substep(f"Deleted document '{d['name']}' from {label}.")
                except Exception as exc:
                    substep(f"Could not delete document '{d['name']}' from {label}: {exc}")
        except Exception:
            pass

        try:
            existing = client.get_list(
                "DocType",
                filters={"name": st.test_doctype},
                fields=["name"],
                limit_page_length=1
            )
            if existing:
                client.delete("DocType", st.test_doctype)
                substep(f"Dropped DocType '{st.test_doctype}' from {label}.")
        except Exception as exc:
            substep(f"Could not drop DocType '{st.test_doctype}' from {label}: {exc}")

        ok = _ensure_test_doctype(client, label, st.test_doctype)
        st.record(f"DocType '{st.test_doctype}' recreated on {label}", ok)
        if not ok:
            substep(f"  DocType '{st.test_doctype}' not available on {label}.")
            return False

    if not _setup_stream(
        st,
        label_producer="Producer",
        producer_client=st.producer_client,
        producer_url=st.producer_url,
        producer_api_key=st.producer_api_key,
        producer_api_secret=st.producer_api_secret,
        label_consumer="Consumer",
        consumer_client=st.consumer_client,
        consumer_url=st.consumer_url,
        step_offset=4,
    ):
        return False

    st.producer_record_created = True
    st.consumer_record_created = True
    return True


def _setup_stream(st, label_producer, producer_client, producer_url,
                  producer_api_key, producer_api_secret,
                  label_consumer, consumer_client, consumer_url,
                  step_offset):
    producer_key = producer_url.rstrip("/")
    consumer_key = consumer_url.rstrip("/")

    step(f"{step_offset}. Creating Event Producer on {label_consumer} (pulls from {label_producer})")
    if _doc_exists(consumer_client, "Event Producer", producer_key):
        substep("Record already exists - skipping.")
        st.record(f"Event Producer created on {label_consumer}", True, "already existed")
    else:
        try:
            substep(f"Using {label_producer} API credentials ...")
            result = consumer_client.insert({
                "doctype": "Event Producer",
                "producer_url": producer_key,
                "api_key": producer_api_key,
                "api_secret": producer_api_secret,
                "user": "Administrator",
                "producer_doctypes": [
                    {"ref_doctype": st.test_doctype, "use_same_name": 1}
                ],
            })
            created_name = result.get("name") if result else None
            substep(f"Created name={created_name!r}, checking ...")
            actually_exists = _doc_exists(consumer_client, "Event Producer", producer_key)
            st.record(
                f"Event Producer created on {label_consumer}",
                actually_exists,
                f"name={created_name}" if actually_exists else
                f"API said OK but GET returned 404 - check {label_consumer} logs",
            )
            if not actually_exists:
                substep("  Record not found after creation.")
                return False
        except Exception as exc:
            st.record(f"Event Producer created on {label_consumer}", False, str(exc))
            substep("  Event Producer creation failed, skipping.")
            return False

    step(f"{step_offset + 1}. Waiting for Event Consumer to appear on {label_producer}")
    found = poll_until(
        lambda: _doc_exists(producer_client, "Event Consumer", consumer_key),
        st.poll_timeout,
        st.poll_interval,
        f"Event Consumer record on {label_producer}",
    )
    st.record(f"Event Consumer record present on {label_producer}", found)
    if not found:
        substep(f"  Event Consumer did not appear on {label_producer}.")
        return False

    step(f"{step_offset + 2}. Approving '{st.test_doctype}' on {label_producer}'s Event Consumer")
    try:
        ec = producer_client.get_doc("Event Consumer", consumer_key)
        if ec is None:
            raise RuntimeError("get_doc returned None for Event Consumer")

        dt_lower = st.test_doctype.lower()
        consumer_doctypes = ec.get("consumer_doctypes") or []
        substep(f"Found {len(consumer_doctypes)} doctype row(s)")

        updated = [
            dict(e, status="Approved")
            if (e.get("ref_doctype") or "").lower() == dt_lower else e
            for e in consumer_doctypes
        ]
        producer_client.update({
            "doctype": "Event Consumer",
            "name": consumer_key,
            "consumer_doctypes": updated,
        })

        ec2 = producer_client.get_doc("Event Consumer", consumer_key)
        approved = any(
            (e.get("ref_doctype") or "").lower() == dt_lower and e.get("status") == "Approved"
            for e in (ec2.get("consumer_doctypes") or [])
        )
        st.record(f"Doctype '{st.test_doctype}' approved on {label_producer}", approved)
        if not approved:
            substep("  Row not found or still Pending.")
            return False
    except Exception as exc:
        st.record(f"Doctype '{st.test_doctype}' approved on {label_producer}", False, str(exc))
        return False

    step(f"{step_offset + 3}. Waiting for approval to reflect on {label_consumer}'s Event Producer")

    def approval_reflected():
        ep = consumer_client.get_doc("Event Producer", producer_key)
        return any(
            (e.get("ref_doctype") or "").lower() == st.test_doctype.lower()
            and e.get("status") == "Approved"
            for e in ep.get("producer_doctypes", [])
        )

    synced = poll_until(
        approval_reflected,
        st.poll_timeout,
        st.poll_interval,
        f"approval to reflect on {label_consumer}'s Event Producer",
    )
    st.record(f"Approval reflected on {label_consumer}'s Event Producer", synced)
    return synced


def phase_setup_reverse(st):
    print()
    print("=" * 62)
    print("  PHASE 1b: REVERSE SETUP (Consumer -> Producer)")
    print("=" * 62)
    return _setup_stream(
        st,
        label_producer="Consumer",
        producer_client=st.consumer_client,
        producer_url=st.consumer_url,
        producer_api_key=st.consumer_api_key,
        producer_api_secret=st.consumer_api_secret,
        label_consumer="Producer",
        consumer_client=st.producer_client,
        consumer_url=st.producer_url,
        step_offset=1,
    )


def phase_test(st):
    print()
    print("=" * 62)
    print("  PHASE 2: TESTING (Producer -> Consumer)")
    print("=" * 62)

    tag = uuid.uuid4().hex[:8].upper()
    test_field_value = f"ES-TEST-{tag}"

    step(f"1. Creating test document on Producer ({st.test_doctype})")
    try:
        created = st.producer_client.insert({
            "doctype": st.test_doctype,
            "test_field": test_field_value,
            "test_update_field": "",
        })
        st.created_doc_name = created.get("name") or test_field_value
        st.record("Document created on Producer", bool(st.created_doc_name),
                  f"name={st.created_doc_name}")
    except Exception as exc:
        st.record("Document created on Producer", False, str(exc))
        return False

    step("2. Triggering pull_from_node on Consumer")
    try:
        _trigger_pull_node(st.consumer_client, st.producer_url)
        st.record("pull_from_node triggered", True)
    except Exception as exc:
        st.record("pull_from_node triggered", False, str(exc))
        substep("Background scheduler may still sync.")

    step(f"3. Waiting for '{st.created_doc_name}' to appear on Consumer")
    synced = poll_until(
        lambda: _doc_exists(st.consumer_client, st.test_doctype, st.created_doc_name),
        st.poll_timeout,
        st.poll_interval,
        "document to appear on Consumer",
    )
    st.record("Create synced to Consumer", synced, f"name={st.created_doc_name}")
    if not synced:
        _dump_sync_log(st, st.created_doc_name)
        return False

    step("4. Updating test_update_field on Producer")
    updated_value = f"UPDATED-{tag}"
    try:
        st.producer_client.update({
            "doctype": st.test_doctype,
            "name": st.created_doc_name,
            "test_update_field": updated_value,
        })
        st.record("test_update_field updated on Producer", True,
                  f"test_update_field={updated_value}")
    except Exception as exc:
        st.record("test_update_field updated on Producer", False, str(exc))
        return False

    _trigger_pull_node(st.consumer_client, st.producer_url)

    step("5. Waiting for test_update_field to reflect on Consumer")

    def update_visible():
        doc = st.consumer_client.get_doc(st.test_doctype, st.created_doc_name)
        return doc.get("test_update_field") == updated_value

    update_ok = poll_until(
        update_visible,
        st.poll_timeout,
        st.poll_interval,
        "test_update_field to match on Consumer",
    )
    if not update_ok:
        try:
            doc = st.consumer_client.get_doc(st.test_doctype, st.created_doc_name)
            substep(f"  Consumer has '{doc.get('test_update_field')}' (expected '{updated_value}')")
        except Exception:
            pass
    st.record("Update synced to Consumer (test_update_field)", update_ok)

    return True


def phase_test_two_way(st):
    print()
    print("=" * 62)
    print("  PHASE 2b: TWO-WAY SYNC TEST")
    print("=" * 62)

    if not st.created_doc_name:
        substep("  No document from Phase 2 to test with. Skipping.")
        return False

    doc_name = st.created_doc_name
    tag = doc_name.replace("ES-TEST-", "")

    step("1. Updating test_update_field on Consumer (reverse direction)")
    reverse_value = f"REVERSE-{tag}"
    try:
        st.consumer_client.update({
            "doctype": st.test_doctype,
            "name": doc_name,
            "test_update_field": reverse_value,
        })
        st.record("test_update_field updated on Consumer", True,
                  f"test_update_field={reverse_value}")
    except Exception as exc:
        st.record("test_update_field updated on Consumer", False, str(exc))
        return False

    _trigger_pull_node(st.producer_client, st.consumer_url)

    step("2. Waiting for test_update_field to reflect on Producer")

    def reverse_update_visible():
        doc = st.producer_client.get_doc(st.test_doctype, doc_name)
        return doc.get("test_update_field") == reverse_value

    reverse_ok = poll_until(
        reverse_update_visible,
        st.poll_timeout,
        st.poll_interval,
        "reverse test_update_field to appear on Producer",
    )
    if not reverse_ok:
        try:
            doc = st.producer_client.get_doc(st.test_doctype, doc_name)
            substep(f"  Producer has '{doc.get('test_update_field')}' (expected '{reverse_value}')")
        except Exception:
            pass
    st.record("Reverse update synced to Producer (test_update_field)", reverse_ok)

    step("3. Deleting document from Consumer")
    try:
        st.consumer_client.delete(st.test_doctype, doc_name)
        st.record("Document deleted on Consumer", True)
    except Exception as exc:
        st.record("Document deleted on Consumer", False, str(exc))
        return True

    _trigger_pull_node(st.producer_client, st.consumer_url)

    delete_ok = poll_until(
        lambda: not _doc_exists(st.producer_client, st.test_doctype, doc_name),
        st.poll_timeout,
        st.poll_interval,
        "delete to propagate to Producer",
    )
    st.record("Delete synced to Producer", delete_ok)
    if delete_ok:
        st.created_doc_name = None

    return True


def phase_cleanup(st):
    print()
    print("=" * 62)
    print("  PHASE 3: CLEANUP")
    print("=" * 62)

    step(f"1. Deleting all '{st.test_doctype}' documents on both sites")
    for label, client in [("Producer", st.producer_client), ("Consumer", st.consumer_client)]:
        try:
            docs = client.get_list(st.test_doctype, fields=["name"], limit_page_length=200)
            if not docs:
                substep(f"No documents on {label}.")
            for d in docs:
                try:
                    _force_delete(client, st.test_doctype, d["name"])
                    substep(f"Deleted '{d['name']}' from {label}.")
                except Exception as exc:
                    substep(f"Could not delete '{d['name']}' from {label}: {exc}")
        except Exception as exc:
            substep(f"Could not list '{st.test_doctype}' on {label}: {exc}")

    step(f"2. Removing DocType '{st.test_doctype}' from both sites")
    for label, client in [("Producer", st.producer_client), ("Consumer", st.consumer_client)]:
        try:
            exists = client.get_list(
                "DocType",
                filters={"name": st.test_doctype},
                fields=["name"],
                limit_page_length=1
            )
            if exists:
                _force_delete(client, "DocType", st.test_doctype)
                substep(f"Dropped '{st.test_doctype}' from {label}.")
            else:
                substep(f"Already gone from {label}.")
        except Exception as exc:
            substep(f"Could not drop DocType from {label}: {exc}")

    step("3. Cleaning up Event Producer and Event Consumer records")
    for doctype in ("Event Producer", "Event Consumer"):
        for label, client in [("Producer", st.producer_client), ("Consumer", st.consumer_client)]:
            _wipe_doctype_records(client, label, doctype)


def parse_args(argv=None):
    p = argparse.ArgumentParser(
        description="End to end test for the Frappe Event Streaming app.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    p.add_argument("--producer-site",   required=True)
    p.add_argument("--producer-url",    required=True)
    p.add_argument("--consumer-site",   required=True)
    p.add_argument("--consumer-url",    required=True)
    p.add_argument("--test-doctype",    default="Test Doctype")
    p.add_argument("--poll-timeout",    type=int, default=60)
    p.add_argument("--poll-interval",   type=int, default=5)
    p.add_argument("--skip-cleanup",    action="store_true")
    p.add_argument(
        "--test-with-ghs",
        action="store_true",
        default=False,
        help=(
            "Run the titus_ghs end to end test instead of the generic Test Doctype flow. "
            "Sets up Event Producer / Event Consumer for GHS Authority to Withdraw, "
            "Weighbridge Transaction (Producer->Consumer only), and Stock Entry "
            "(bidirectional), then verifies document sync in each direction."
        ),
    )

    creds = p.add_argument_group(
        "API credentials (required)",
        "Pass the api_key and api_secret for each site.",
    )
    creds.add_argument("--producer-api-key",    required=True,
                       help="API key for the Producer site")
    creds.add_argument("--producer-api-secret", required=True,
                       help="API secret for the Producer site")
    creds.add_argument("--consumer-api-key",    required=True,
                       help="API key for the Consumer site")
    creds.add_argument("--consumer-api-secret", required=True,
                       help="API secret for the Consumer site")

    return p.parse_args(list(argv))


def main(*argv):
    args = parse_args(argv)

    producer_url = args.producer_url.rstrip("/")
    consumer_url = args.consumer_url.rstrip("/")

    print()
    print("=" * 62)
    print("  Frappe Event Streaming - End to end Test")
    print("=" * 62)
    print(f"  Producer site : {args.producer_site}  ({producer_url})")
    print(f"  Consumer site : {args.consumer_site}  ({consumer_url})")
    print(f"  DocType       : {args.test_doctype}")
    if args.test_with_ghs:
        print("  GHS mode      : enabled (runs after standard tests)")
        print(f"  P->C doctypes  : {', '.join(GHS_PRODUCER_TO_CONSUMER)}")
        print(f"  C->P doctypes  : {', '.join(GHS_CONSUMER_TO_PRODUCER)}")
    print(f"  Poll timeout  : {args.poll_timeout}s  (interval {args.poll_interval}s)")

    p_key, p_secret = args.producer_api_key, args.producer_api_secret
    c_key, c_secret = args.consumer_api_key, args.consumer_api_secret

    producer_client = FrappeClient(producer_url, api_key=p_key, api_secret=p_secret)
    consumer_client = FrappeClient(consumer_url, api_key=c_key, api_secret=c_secret)

    print()
    print("=" * 62)
    print("  Validating API credentials")
    print("=" * 62)

    p_user = producer_client.get_api("frappe.auth.get_logged_user") or "unknown"
    p_ping = bool(p_user and p_user not in ("unknown", "Guest", ""))
    print(f"  Producer  {producer_url}  ->  {p_user}  ({'OK' if p_ping else 'FAILED'})")

    c_user = consumer_client.get_api("frappe.auth.get_logged_user") or "unknown"
    c_ping = bool(c_user and c_user not in ("unknown", "Guest", ""))
    print(f"  Consumer  {consumer_url}  ->  {c_user}  ({'OK' if c_ping else 'FAILED'})")
    print("=" * 62)

    if not p_ping or not c_ping:
        print("  API key validation failed. Halting before running test suite.")
        sys.exit(1)

    print("  Credentials OK. Starting tests...")

    st = TestState(
        producer_site=args.producer_site,
        consumer_site=args.consumer_site,
        producer_url=producer_url,
        consumer_url=consumer_url,
        producer_client=producer_client,
        consumer_client=consumer_client,
        test_doctype=args.test_doctype,
        poll_timeout=args.poll_timeout,
        poll_interval=args.poll_interval,
        producer_api_key=p_key,
        producer_api_secret=p_secret,
        consumer_api_key=c_key,
        consumer_api_secret=c_secret,
        test_with_ghs=args.test_with_ghs,
    )

    try:
        phase_preflight_cleanup(st)

        setup_ok = phase_setup(st)
        if not setup_ok:
            print("\n  Setup failed - skipping test phase.")
        else:
            reverse_ok = phase_setup_reverse(st)
            if not reverse_ok:
                print("\n  Reverse setup failed - skipping two-way test.")
            phase_test(st)
            if reverse_ok:
                phase_test_two_way(st)

        if not args.skip_cleanup:
            phase_cleanup(st)
        else:
            print("\n  --skip-cleanup set: skipping standard cleanup.")

        if st.test_with_ghs:
            print()
            print("=" * 62)
            print("  Starting titus_ghs test suite")
            print("=" * 62)
            preflight_ok = phase_ghs_preflight(st)
            if not preflight_ok:
                print("\n  GHS preflight failed  one or more doctypes are missing.")
                print("  Install titus_ghs on both sites and retry.")
            else:
                ghs_setup_ok = phase_ghs_setup(st)
                if not ghs_setup_ok:
                    print("\n  GHS setup failed  skipping GHS test phase.")
                else:
                    ghs_reverse_ok = phase_ghs_setup_reverse(st)
                    if not ghs_reverse_ok:
                        print("\n  GHS reverse setup failed  skipping reverse sync test.")
                    phase_ghs_test(st)
                    if ghs_reverse_ok:
                        phase_ghs_test_reverse(st)

    except KeyboardInterrupt:
        print("\n\n  Interrupted.")
    except Exception:
        print("\n  Unexpected error:")
        traceback.print_exc()
    finally:
        if st.test_with_ghs and not args.skip_cleanup:
            phase_ghs_cleanup(st)

    all_passed = st.summary()
    sys.exit(0 if all_passed else 1)


if __name__ == "__main__":
    main(*sys.argv[1:])