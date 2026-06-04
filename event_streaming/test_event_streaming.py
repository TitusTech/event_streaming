#!/usr/bin/env python3
"""
Integration test for the Frappe Event Streaming app.

Runs via:  bench --site <site> execute test_event_streaming.main
Requires frappe.frappeclient (available inside any bench environment).

Usage:

    python test_event_streaming.py \
        --producer-site       mgti.localhost \
        --producer-url        http://localhost:8000 \
        --consumer-site       mgti2.localhost \
        --consumer-url        http://localhost:9999 \
        --producer-api-key    <key> \
        --producer-api-secret <secret> \
        --consumer-api-key    <key> \
        --consumer-api-secret <secret> \
        [--test-doctype "Test Doctype"] \
        [--poll-timeout 60] \
        [--poll-interval 5] \
        [--skip-cleanup]
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
        if any(k in msg for k in ("not exist", "404", "notfound", "does not exist",
                                   "doesnot", "could not find", "no document")):
            return False
        raise


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


def _dump_sync_log(st, doc_name):
    substep("  Document did not sync. Fetching Event Sync Log for diagnosis ...")
    try:
        logs = st.consumer_client.get_list(
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


def phase_preflight_cleanup(st):
    print()
    print("=" * 62)
    print("  PHASE 0:CLEANUP")
    print("=" * 62)

    for doctype in ("Event Producer", "Event Consumer"):
        for label, client in [("Producer", st.producer_client), ("Consumer", st.consumer_client)]:
            step(f"  Removing all {doctype} records from {label}")
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

    step("1. Verifying API credentials")
    for label, client in [("Producer", st.producer_client), ("Consumer", st.consumer_client)]:
        user = client.get_api("frappe.auth.get_logged_user") or "unknown"
        ok = user not in ("unknown", "Guest", "")
        st.record(
            f"API credentials valid - {label} ({client.url})",
            ok,
            f"logged in as {user}" if ok else "authentication failed or site unreachable",
        )
        if not ok:
            print(f"  Cannot authenticate to {label}. Aborting.")
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

    step(f"3. Recreating DocType '{st.test_doctype}' on Producer and Consumer")
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
            substep(f"  Cannot continue without '{st.test_doctype}' on {label}.")
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
            substep(f"Embedding {label_producer} API credentials ...")
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
            substep(f"API returned name={created_name!r} - verifying ...")
            actually_exists = _doc_exists(consumer_client, "Event Producer", producer_key)
            st.record(
                f"Event Producer created on {label_consumer}",
                actually_exists,
                f"name={created_name}" if actually_exists else
                f"API said OK but GET returned 404 - check {label_consumer} logs",
            )
            if not actually_exists:
                substep("  Record not found after creation. Cannot continue.")
                return False
        except Exception as exc:
            st.record(f"Event Producer created on {label_consumer}", False, str(exc))
            substep("  Cannot continue without Event Producer record.")
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
        substep("Continuing - background scheduler may still sync.")

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

    step(f"2. Dropping DocType '{st.test_doctype}' from both sites")
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

    step("3. Removing all Event Producer / Event Consumer records from both sites")
    for doctype in ("Event Producer", "Event Consumer"):
        for label, client in [("Producer", st.producer_client), ("Consumer", st.consumer_client)]:
            _wipe_doctype_records(client, label, doctype)


def parse_args(argv=None):
    p = argparse.ArgumentParser(
        description="Integration test for the Frappe Event Streaming app.",
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
    print("  Frappe Event Streaming - Integration Test")
    print("=" * 62)
    print(f"  Producer site : {args.producer_site}  ({producer_url})")
    print(f"  Consumer site : {args.consumer_site}  ({consumer_url})")
    print(f"  DocType       : {args.test_doctype}")
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

    print("  Both credential sets passed. Proceeding to tests...")

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
    except KeyboardInterrupt:
        print("\n\n  Interrupted.")
    except Exception:
        print("\n  Unexpected error:")
        traceback.print_exc()
    finally:
        if not args.skip_cleanup:
            phase_cleanup(st)
        else:
            print("\n  --skip-cleanup set: skipping cleanup.")

    all_passed = st.summary()
    sys.exit(0 if all_passed else 1)


if __name__ == "__main__":
    main(*sys.argv[1:])