# Copyright (c) 2019, Frappe Technologies and contributors
# License: MIT. See LICENSE

import json
import time
import requests
import frappe
from frappe import _
from frappe.custom.doctype.custom_field.custom_field import create_custom_field
from frappe.frappeclient import FrappeClient
from frappe.model.document import Document
from frappe.utils.background_jobs import get_jobs
from frappe.utils.data import get_link_to_form
from frappe.utils.password import get_decrypted_password
from event_streaming.utils.utils import get_url

EVENT_STREAMING_CACHE_KEY_PREFIX = "event_producer_document_types_map"

class EventProducer(Document):

    def after_insert(self):
        self.rebuild_cache()

    def rebuild_cache(self):
        records = frappe.get_all(
            "Event Producer Document Type",
            fields=["*"],
            ignore_ddl=True,
        )

        event_streaming_map = {
            d["ref_doctype"]: d
            for d in records
        }

        frappe.cache().set_value(
            f"{EVENT_STREAMING_CACHE_KEY_PREFIX}_{frappe.scrub(self.producer_url)}",
            event_streaming_map
        )

    def before_insert(self):
        self.check_url()
        self.validate_event_subscriber()
        self.incoming_change = True
        self.create_event_consumer()
        self.create_custom_fields()

    def validate(self):
        self.validate_event_subscriber()

        if frappe.flags.in_test:
            for entry in self.producer_doctypes:
                entry.status = "Approved"

    def validate_event_subscriber(self):
        if not frappe.db.get_value("User", self.user, "api_key"):
            frappe.throw(
                _("Please generate keys for the Event Subscriber User {0} first.").format(
                    frappe.bold(get_link_to_form("User", self.user))
                )
            )

    def on_update(self):
        if not self.incoming_change:
            if frappe.db.exists("Event Producer", self.name):
                if not self.api_key or not self.api_secret:
                    frappe.throw(
                        _("Please set API Key and Secret on the producer and consumer sites first.")
                    )
                else:
                    doc_before_save = self.get_doc_before_save()

                    if doc_before_save.api_key != self.api_key or doc_before_save.api_secret != self.api_secret:
                        return

                    self.update_event_consumer()
                    self.create_custom_fields()
        else:
            # when producer doc is updated it updates the consumer doc, set flag to avoid deadlock
            self.db_set("incoming_change", 0)
            self.reload()

        self.rebuild_cache()

    def on_trash(self):
        last_update = frappe.db.get_value("Event Producer Last Update", dict(event_producer=self.name))
        if last_update:
            frappe.delete_doc("Event Producer Last Update", last_update)
        self.rebuild_cache()

    def check_url(self):
        valid_url_schemes = ("http", "https")
        frappe.utils.validate_url(self.producer_url, throw=True, valid_schemes=valid_url_schemes)

        if self.producer_url.endswith("/"):
            self.producer_url = self.producer_url[:-1]

    def create_event_consumer(self):
        """register event consumer on the producer site"""
        if self.is_producer_online():
            producer_site = FrappeClient(
                url=self.producer_url, 
                api_key=self.api_key, 
                api_secret=self.get_password("api_secret")
            )

            response = producer_site.post_api(
                "event_streaming.event_streaming.doctype.event_consumer.event_consumer.register_consumer",
                params={"data": json.dumps(self.get_request_data())},
            )
            if response:
                response = json.loads(response)
                self.set_last_update(response["last_update"])
            else:
                frappe.throw(
                    _("Failed to create an Event Consumer or an Event Consumer for the current site is already registered.")
                )

    def set_last_update(self, last_update):
        last_update_doc_name = frappe.db.get_value(
            "Event Producer Last Update", dict(event_producer=self.name)
        )
        if not last_update_doc_name:
            frappe.get_doc(
                dict(
                    doctype="Event Producer Last Update",
                    event_producer=self.producer_url,
                    last_update=last_update,
                )
            ).insert(ignore_permissions=True)
        else:
            frappe.db.set_value(
                "Event Producer Last Update", last_update_doc_name, "last_update", last_update
            )

    def get_last_update(self):
        return frappe.db.get_value(
            "Event Producer Last Update", dict(event_producer=self.name), "last_update"
        )

    def get_request_data(self):
        consumer_doctypes = []
        for entry in self.producer_doctypes:
            if entry.has_mapping:
                dt = frappe.db.get_value("Document Type Mapping", entry.mapping, "remote_doctype")
            else:
                dt = entry.ref_doctype
            consumer_doctypes.append({"doctype": dt, "condition": entry.condition})

        user_key = frappe.db.get_value("User", self.user, "api_key")
        user_secret = get_decrypted_password("User", self.user, "api_secret")
        return {
            "event_consumer": get_url(),
            "consumer_doctypes": json.dumps(consumer_doctypes),
            "user": self.user,
            "api_key": user_key,
            "api_secret": user_secret,
        }

    def create_custom_fields(self):
        """create custom field to store remote docname and remote site url"""
        for entry in self.producer_doctypes:
            if not entry.use_same_name:
                if not frappe.db.exists(
                    "Custom Field", {"fieldname": "remote_docname", "dt": entry.ref_doctype}
                ):
                    df = dict(
                        fieldname="remote_docname",
                        label="Remote Document Name",
                        fieldtype="Data",
                        read_only=1,
                        print_hide=1,
                    )
                    create_custom_field(entry.ref_doctype, df)
                if not frappe.db.exists(
                    "Custom Field", {"fieldname": "remote_site_name", "dt": entry.ref_doctype}
                ):
                    df = dict(
                        fieldname="remote_site_name",
                        label="Remote Site",
                        fieldtype="Data",
                        read_only=1,
                        print_hide=1,
                    )
                    create_custom_field(entry.ref_doctype, df)

    def update_event_consumer(self):
        if self.is_producer_online():
            producer_site = get_producer_site(self.producer_url)
            event_consumer = producer_site.get_doc("Event Consumer", get_url())
            event_consumer = frappe._dict(event_consumer)
            if event_consumer:
                config = event_consumer.consumer_doctypes
                event_consumer.consumer_doctypes = []
                for entry in self.producer_doctypes:
                    if entry.has_mapping:
                        ref_doctype = frappe.db.get_value("Document Type Mapping", entry.mapping, "remote_doctype")
                    else:
                        ref_doctype = entry.ref_doctype

                    event_consumer.consumer_doctypes.append(
                        {
                            "ref_doctype": ref_doctype,
                            "status": get_approval_status(config, ref_doctype),
                            "unsubscribed": entry.unsubscribe,
                            "condition": entry.condition,
                        }
                    )
                event_consumer.user = self.user
                event_consumer.incoming_change = True
                producer_site.update(event_consumer)

    def is_producer_online(self):
        """check connection status for the Event Producer site"""
        retry = 3
        while retry > 0:
            res = requests.get(self.producer_url)
            if res.status_code == 200:
                return True
            retry -= 1
            time.sleep(5)
        frappe.throw(_("Failed to connect to the Event Producer site. Retry after some time."))


def get_producer_site(producer_url):
	"""create a FrappeClient object for event producer site"""
	producer_doc = frappe.get_doc("Event Producer", producer_url)
	producer_site = FrappeClient(
		url=producer_url,
		api_key=producer_doc.api_key,
		api_secret=producer_doc.get_password("api_secret"),
	)
	return producer_site


def get_approval_status(config, ref_doctype):
	"""check the approval status for consumption"""
	for entry in config:
		if entry.get("ref_doctype") == ref_doctype:
			return entry.get("status")
	return "Pending"


@frappe.whitelist()
def pull_producer_data():
	"""Fetch data from producer node."""
	response = requests.get(get_url())
	if response.status_code == 200:
		for event_producer in frappe.get_all("Event Producer"):
			pull_from_node(event_producer.name)
		return "success"
	return None


@frappe.whitelist()
def pull_from_node(event_producer):
	"""pull all updates after the last update timestamp from event producer site"""
	event_producer = frappe.get_doc("Event Producer", event_producer)
	producer_site = get_producer_site(event_producer.producer_url)
	last_update = event_producer.get_last_update()

	(doctypes, mapping_config, naming_config, name_conversion_config, name_conversion, use_remote_doc, stream_directly_in_db) = get_config(event_producer.producer_doctypes)

	updates = get_updates(producer_site, last_update, doctypes)

	for update in updates:
		update.use_same_name = naming_config.get(update.ref_doctype)
		update.has_name_conversion = name_conversion_config.get(update.ref_doctype)
		update.use_remote_doc = use_remote_doc.get(update.ref_doctype)
		update.stream_directly_in_db = stream_directly_in_db.get(update.ref_doctype)
		if update.has_name_conversion:
			update.name_conversion = name_conversion.get(update.ref_doctype)
		mapping = mapping_config.get(update.ref_doctype)
		if mapping:
			update.mapping = mapping
			update = get_mapped_update(update, producer_site)
		if not update.update_type == "Delete":
			update.data = json.loads(update.data)

		sync(update, producer_site, event_producer)


def get_config(event_config):
	"""get the doctype mapping and naming configurations for consumption"""
	doctypes, mapping_config, naming_config, name_conversion_config, name_conversion, use_remote_doc, stream_directly_in_db = [], {}, {}, {}, {}, {}, {}

	for entry in event_config:
		if entry.status == "Approved":
			if entry.has_mapping:
				(mapped_doctype, mapping) = frappe.db.get_value(
					"Document Type Mapping", entry.mapping, ["remote_doctype", "name"]
				)
				mapping_config[mapped_doctype] = mapping
				naming_config[mapped_doctype] = entry.use_same_name
				doctypes.append(mapped_doctype)
			else:
				naming_config[entry.ref_doctype] = entry.use_same_name
				doctypes.append(entry.ref_doctype)
			name_conversion_config[entry.ref_doctype] = entry.name_conversion
			if entry.has_name_conversion:
				name_conversion[entry.ref_doctype] = entry.name_conversion
			use_remote_doc[entry.ref_doctype] = entry.use_remote_doc
			stream_directly_in_db[entry.ref_doctype] = entry.stream_directly_in_db
	return (doctypes, mapping_config, naming_config, name_conversion_config, name_conversion, use_remote_doc, stream_directly_in_db)


def sync(update, producer_site, event_producer, in_retry=False):
    """Sync the individual update"""
    frappe.flags.in_event_streaming = True
    frappe.flags.stream_directly_in_db = bool(update.get("stream_directly_in_db"))
    try:
        if update.update_type == "Create":
            if not update.use_same_name and update.has_name_conversion:
                update.modified_name = update.name_conversion.replace("|name|", update.docname)
            set_insert(update, producer_site, event_producer)

        elif update.update_type == "Update":
            set_update(update, producer_site, event_producer)

        elif update.update_type == "Delete":
            set_delete(update)

        if in_retry:
            return "Synced"

        log_event_sync(update, event_producer.name, "Synced")

    except Exception:
        if in_retry:
            return "Failed"
        log_event_sync(update, event_producer.name, "Failed", frappe.get_traceback())

    finally:
        frappe.flags.in_event_streaming = False
        frappe.flags.stream_directly_in_db = False

    event_producer.set_last_update(update.creation)
    frappe.db.commit()

def modify_insert_data_based_on_config(update_data, producer_site, event_producer):
    event_streaming_map = get_event_streaming_map(event_producer.producer_url)

    doctype = update_data.get("doctype") if isinstance(update_data, dict) else update_data.doctype
    meta = frappe.get_meta(doctype)
    link_fields = meta.get_link_fields()

    for field in link_fields:
        linked_doctype = field.options
        config = event_streaming_map.get(linked_doctype)
        

        if config and config.get("use_remote_doc") and update_data.get(field.fieldname):
            foreign_doc = producer_site.get_doc(linked_doctype, update_data.get(field.fieldname))
            target_docname = foreign_doc.get("remote_docname")
            update_data[field.fieldname] = target_docname

        elif config and config.get("has_name_conversion") and config.get("name_conversion"):
            current_val = update_data.get(field.fieldname)
            if current_val:
                target_name = config.get("name_conversion").replace("|name|", current_val)
                update_data[field.fieldname] = target_name
        else:
            print(f"No sync config for {field.fieldname} ({linked_doctype})")

    return update_data

def modify_update_data_based_on_config(update_diff, producer_site, target_doctype, event_producer):
    event_streaming_map = get_event_streaming_map(event_producer.producer_url)

    meta = frappe.get_meta(target_doctype)
    link_fields = meta.get_link_fields()

    for section in ["changed", "added"]:
        section_data = update_diff.get(section) or {}

        for field in link_fields:
            fieldname = field.fieldname
            linked_doctype = field.options

            if fieldname not in section_data:
                continue

            config = event_streaming_map.get(linked_doctype)

            current_val = section_data.get(fieldname)

            if not current_val:
                continue

            if config and config.get("use_remote_doc"):
                foreign_doc = producer_site.get_doc(linked_doctype, current_val)
                target_docname = foreign_doc.get("remote_docname")
                section_data[fieldname] = target_docname

            elif config and config.get("has_name_conversion") and config.get("name_conversion"):
                target_name = config.get("name_conversion").replace("|name|", current_val)
                section_data[fieldname] = target_name

            else:
                print(f"No sync config for {fieldname} ({linked_doctype})")

    return update_diff
def set_insert(update, producer_site, event_producer):
    """Sync insert type update"""
    event_streaming_map = get_event_streaming_map(event_producer.producer_url)

    if update.use_same_name and frappe.db.get_value(update.ref_doctype, update.docname):
        set_update(update, producer_site, event_producer)
        return

    if not update.use_same_name and update.has_name_conversion:
        update.data["name"] = update.modified_name

    current_update_data = update.data
    modified_update_data = modify_insert_data_based_on_config(
        current_update_data, producer_site, event_producer
    )

    doc = frappe.get_doc(modified_update_data)
    meta = frappe.get_meta(doc.doctype)
    link_fields = meta.get_link_fields()


    if update.mapping:
        if update.get("dependencies"):
            dependencies_created = sync_mapped_dependencies(
                update.dependencies, producer_site
            )
            for fieldname, value in dependencies_created.items():
                doc.update({fieldname: value})
    else:
        sync_dependencies(doc, producer_site)

    if update.use_same_name:
        insert_doc_without_workflow(doc, set_name=update.docname, set_child_names=False)
    else:
        doc.remote_docname = update.docname
        doc.remote_site_name = event_producer.producer_url

        if update.has_name_conversion:
            doc.name = str(update.modified_name)

        insert_doc_without_workflow(doc, set_child_names=False, set_name=doc.name)

def set_update(update, producer_site, event_producer):
	"""Sync update type update"""
	local_doc = get_local_doc(update, producer_site)
	if local_doc:
		current_data = update.data
		modified_data = modify_update_data_based_on_config(current_data, producer_site, update.ref_doctype, event_producer)
		data = frappe._dict(modified_data)

		if data.changed:
			local_doc.update(data.changed)
		if data.removed:
			local_doc = update_row_removed(local_doc, data.removed)
		if data.row_changed:
			update_row_changed(local_doc, data.row_changed)
		if data.added:
			local_doc = update_row_added(local_doc, data.added)

		if update.mapping:
			if update.get("dependencies"):
				dependencies_created = sync_mapped_dependencies(update.dependencies, producer_site)
				for fieldname, value in dependencies_created.items():
					local_doc.update({fieldname: value})
		else:
			sync_dependencies(local_doc, producer_site)

		if frappe.flags.get("stream_directly_in_db"):
			update_doc_directly(local_doc, data)
		elif local_doc.docstatus == 1:
			local_doc.db_update_all()
		else:
			local_doc.save()
			local_doc.db_update_all()


def update_doc_directly(local_doc, data):
	if data.changed:
		changed = {k: v for k, v in data.changed.items() if v is not None}
		if changed:
			frappe.db.set_value(local_doc.doctype, local_doc.name, changed, update_modified=False)

	if data.removed:
		for tablename, rownames in data.removed.items():
			child_doctype = local_doc.get_table_field_doctype(tablename)
			for rowname in rownames:
				frappe.db.delete(child_doctype, {"name": rowname, "parent": local_doc.name})

	if data.row_changed:
		for tablename, rows in data.row_changed.items():
			child_doctype = local_doc.get_table_field_doctype(tablename)
			for row in rows:
				row_name = row.get("name") if isinstance(row, dict) else row["name"]
				row_fields = {k: v for k, v in row.items() if k != "name" and v is not None}
				if row_name and row_fields:
					frappe.db.set_value(child_doctype, row_name, row_fields, update_modified=False)

	if data.added:
		for tablename, rows in data.added.items():
			child_doctype = local_doc.get_table_field_doctype(tablename)
			child_meta = frappe.get_meta(child_doctype)
			valid_fields = (
				{cdf.fieldname for cdf in child_meta.fields}
				| {"name", "parent", "parenttype", "parentfield", "idx", "docstatus",
				   "creation", "modified", "modified_by", "owner"}
			)
			for row in rows:
				row_dict = row.as_dict() if hasattr(row, "as_dict") else dict(row)
				row_dict.update({
					"parent": local_doc.name,
					"parenttype": local_doc.doctype,
					"parentfield": tablename,
				})
				if not row_dict.get("name"):
					row_dict["name"] = frappe.generate_hash(length=10)
				for user_field in ("owner", "modified_by"):
					if not row_dict.get(user_field):
						row_dict[user_field] = frappe.session.user or "Administrator"
				for dt_field in ("creation", "modified"):
					if not row_dict.get(dt_field):
						row_dict[dt_field] = frappe.utils.now()
				row_dict = {k: v for k, v in row_dict.items() if k in valid_fields}
				columns = ", ".join(f"`{k}`" for k in row_dict)
				placeholders = ", ".join(["%s"] * len(row_dict))
				frappe.db.sql(
					f"INSERT INTO `tab{child_doctype}` ({columns}) VALUES ({placeholders})",
					list(row_dict.values()),
				)


def update_row_removed(local_doc, removed):
	"""Sync child table row deletion type update"""
	for tablename, rownames in removed.items():
		table = local_doc.get_table_field_doctype(tablename)
		for row in rownames:
			table_rows = local_doc.get(tablename)
			child_table_row = get_child_table_row(table_rows, row)
			table_rows.remove(child_table_row)
			local_doc.set(tablename, table_rows)
	return local_doc


def get_child_table_row(table_rows, row):
	for entry in table_rows:
		if entry.get("name") == row:
			return entry


def update_row_changed(local_doc, changed):
	"""Sync child table row updation type update"""
	for tablename, rows in changed.items():
		old = local_doc.get(tablename)
		for doc in old:
			for row in rows:
				if row["name"] == doc.get("name"):
					doc.update(row)


def update_row_added(local_doc, added):
	"""Sync child table row addition type update"""
	for tablename, rows in added.items():
		local_doc.extend(tablename, rows)
		for child in rows:
			child_doc = frappe.get_doc(child)
			child_doc.parent = local_doc.name
			child_doc.parenttype = local_doc.doctype
			insert_doc_without_workflow(child_doc, set_name=child_doc.name)
	return local_doc


def set_delete(update):
	"""Sync delete type update"""
	local_doc = get_local_doc(update)
	if local_doc:
		local_doc.delete()


def get_updates(producer_site, last_update, doctypes):
	"""Get all updates generated after the last update timestamp"""
	docs = producer_site.post_request(
		{
			"cmd": "event_streaming.event_streaming.doctype.event_update_log.event_update_log.get_update_logs_for_consumer",
			"event_consumer": get_url(),
			"doctypes": frappe.as_json(doctypes),
			"last_update": last_update,
		}
	)
	return [frappe._dict(d) for d in (docs or [])]


def get_local_doc(update, producer_site=None):
	"""Get the local document if created with a different name"""
	try:
		if update.use_remote_doc and producer_site:
			foreign_doc = producer_site.get_doc(update.ref_doctype, update.docname)
			target_docname = foreign_doc.get("remote_docname")
			update.local_document_name = target_docname
			return frappe.get_doc(update.ref_doctype, target_docname)
		if not update.use_same_name:
			return frappe.get_doc(update.ref_doctype, {"remote_docname": update.docname})
		return frappe.get_doc(update.ref_doctype, update.docname)
	except frappe.DoesNotExistError:
		return None

def get_event_streaming_map(producer_url):
    cache_key = f"{EVENT_STREAMING_CACHE_KEY_PREFIX}_{frappe.scrub(producer_url)}"
    cache = frappe.cache()

    event_streaming_map = cache.get_value(cache_key)

    if event_streaming_map is None:
        producer = frappe.get_doc("Event Producer", {"producer_url": producer_url})
        producer.rebuild_cache()
        event_streaming_map = cache.get_value(cache_key)

    return event_streaming_map or {}


def _get_child_row_dict(row, parent_name, parent_doctype, parent_field, idx):
    """Build a sanitised dict for a child table row, ready for raw SQL insertion."""
    row_dict = row.as_dict() if hasattr(row, "as_dict") else dict(row)
    row_dict.update({
        "parent": parent_name,
        "parenttype": parent_doctype,
        "parentfield": parent_field,
        "idx": idx,
    })
    if not row_dict.get("name"):
        row_dict["name"] = frappe.generate_hash(length=10)
    for ts_field in ("owner", "modified_by"):
        if not row_dict.get(ts_field):
            row_dict[ts_field] = frappe.session.user or "Administrator"
    for dt_field in ("creation", "modified"):
        if not row_dict.get(dt_field):
            row_dict[dt_field] = frappe.utils.now()
    child_meta = frappe.get_meta(row_dict.get("doctype") or row.doctype)
    valid_fields = (
        {cdf.fieldname for cdf in child_meta.fields}
        | {"name", "parent", "parenttype", "parentfield", "idx",
           "docstatus", "creation", "modified", "modified_by", "owner"}
    )
    return {k: v for k, v in row_dict.items() if k in valid_fields}


def _insert_child_rows_directly(doc, meta):
    """Insert all child table rows for *doc* using raw SQL (no hooks)."""
    for df in meta.fields:
        if df.fieldtype not in ("Table", "Table MultiSelect"):
            continue
        for idx, row in enumerate(doc.get(df.fieldname) or [], start=1):
            row_dict = _get_child_row_dict(row, doc.name, doc.doctype, df.fieldname, idx)
            columns = ", ".join(f"`{k}`" for k in row_dict)
            placeholders = ", ".join(["%s"] * len(row_dict))
            frappe.db.sql(
                f"INSERT INTO `tab{df.options}` ({columns}) VALUES ({placeholders})",
                list(row_dict.values()),
            )


def insert_doc_directly(doc, **kwargs):
    if isinstance(doc, dict):
        doc = frappe.get_doc(doc)

    set_name = kwargs.get("set_name")
    if set_name:
        doc.name = set_name

    for user_field in ("owner", "modified_by"):
        if not doc.get(user_field):
            doc.set(user_field, frappe.session.user or "Administrator")
    for dt_field in ("creation", "modified"):
        if not doc.get(dt_field):
            doc.set(dt_field, frappe.utils.now())

    workflow_name = frappe.db.get_value(
        "Workflow", {"document_type": doc.doctype, "is_active": 1}, "name"
    )
    workflow_state_field = (
        frappe.db.get_value("Workflow", workflow_name, "workflow_state_field")
        if workflow_name
        else None
    )
    actual_state = doc.get(workflow_state_field) if workflow_state_field else None
    if workflow_state_field and actual_state:
        doc.set(workflow_state_field, None)

    meta = frappe.get_meta(doc.doctype)

    try:
        doc.db_insert()
        _insert_child_rows_directly(doc, meta)
    except frappe.DuplicateEntryError:
        non_table_fields = {
            df.fieldname: doc.get(df.fieldname)
            for df in meta.fields
            if df.fieldtype not in (
                "Table", "Table MultiSelect", "Section Break",
                "Column Break", "Tab Break", "HTML", "Button",
            )
            and not df.get("is_virtual")
        }
        frappe.db.set_value(doc.doctype, doc.name, non_table_fields)

        for df in meta.fields:
            if df.fieldtype in ("Table", "Table MultiSelect"):
                frappe.db.delete(df.options, {"parent": doc.name, "parenttype": doc.doctype})
        _insert_child_rows_directly(doc, meta)

    if workflow_state_field and actual_state:
        frappe.db.set_value(doc.doctype, doc.name, workflow_state_field, actual_state)
        frappe.db.commit()

    return doc


def insert_doc_without_workflow(doc, **kwargs):
    if frappe.flags.get("stream_directly_in_db"):
        return insert_doc_directly(doc, **kwargs)

    if isinstance(doc, dict):
        doc = frappe.get_doc(doc)

    workflow_name = frappe.db.get_value("Workflow", {"document_type": doc.doctype, "is_active": 1}, "name")
    workflow_state_field = frappe.db.get_value("Workflow", workflow_name, "workflow_state_field") if workflow_name else None
    actual_state = doc.get(workflow_state_field) if workflow_state_field else None

    if workflow_state_field and actual_state:
        doc.set(workflow_state_field, None)
    doc.flags.ignore_validate = True
    try:
        doc.insert(**kwargs)
    except frappe.DuplicateEntryError:
        meta = frappe.get_meta(doc.doctype)

        non_table_fields = {
            df.fieldname: doc.get(df.fieldname)
            for df in meta.fields
            if df.fieldtype not in ("Table", "Table MultiSelect", "Section Break", "Column Break", "Tab Break", "HTML", "Button")
            and not df.get("is_virtual")
        }
        frappe.db.set_value(doc.doctype, doc.name, non_table_fields)

        # delete and reinsert child table rows at db level
        for df in meta.fields:
            if df.fieldtype in ("Table", "Table MultiSelect"):
                frappe.db.delete(df.options, {"parent": doc.name, "parenttype": doc.doctype})
                for row in (doc.get(df.fieldname) or []):
                    row_dict = row.as_dict() if hasattr(row, "as_dict") else dict(row)
                    row_dict.update({
                        "parent": doc.name,
                        "parenttype": doc.doctype,
                        "parentfield": df.fieldname,
                    })
                    child_meta = frappe.get_meta(df.options)
                    valid_fields = {cdf.fieldname for cdf in child_meta.fields} | {"name", "parent", "parenttype", "parentfield", "idx", "docstatus", "creation", "modified", "modified_by", "owner"}
                    row_dict = {k: v for k, v in row_dict.items() if k in valid_fields}
                    columns = ", ".join(f"`{k}`" for k in row_dict)
                    values = ", ".join(["%s"] * len(row_dict))
                    frappe.db.sql(
                        f"INSERT INTO `tab{df.options}` ({columns}) VALUES ({values})",
                        list(row_dict.values())
                    )

    if workflow_state_field and actual_state:
        frappe.db.set_value(doc.doctype, doc.name, workflow_state_field, actual_state)
        frappe.db.commit()


def sync_dependencies(document, producer_site):
    
    def sync_doc_dependencies(doc, producer_site, visited=None):
        if visited is None:
            visited = set()

        doctype = doc.doctype if hasattr(doc, "doctype") else doc.get("doctype")
        docname = doc.name if hasattr(doc, "name") else doc.get("name")
        key = f"{doctype}::{docname}"

        if key in visited:
            return
        visited.add(key)

        meta = frappe.get_meta(doctype)

        # sync link field dependencies first
        for df in meta.get_link_fields():
            linked_docname = doc.get(df.fieldname)
            linked_doctype = df.get_link_doctype()
            if linked_docname and not frappe.db.exists(linked_doctype, linked_docname):
                master_doc = producer_site.get_doc(linked_doctype, linked_docname)
                if master_doc:
                    master_doc = frappe.get_doc(master_doc)
                    sync_doc_dependencies(master_doc, producer_site, visited)
                    insert_doc_without_workflow(master_doc, set_name=linked_docname)
                    frappe.db.commit()

        # sync dynamic link field dependencies
        for df in meta.get_dynamic_link_fields():
            linked_docname = doc.get(df.fieldname)
            linked_doctype = doc.get(df.options)
            if linked_docname and linked_doctype and not frappe.db.exists(linked_doctype, linked_docname):
                master_doc = producer_site.get_doc(linked_doctype, linked_docname)
                if master_doc:
                    master_doc = frappe.get_doc(master_doc)
                    sync_doc_dependencies(master_doc, producer_site, visited)
                    insert_doc_without_workflow(master_doc, set_name=linked_docname)
                    frappe.db.commit()

        # sync child table link dependencies
        for df in meta.get_table_fields():
            for entry in (doc.get(df.fieldname) or []):
                child_meta = frappe.get_meta(entry.doctype if hasattr(entry, "doctype") else entry.get("doctype"))
                for child_df in child_meta.get_link_fields():
                    linked_docname = entry.get(child_df.fieldname)
                    linked_doctype = child_df.get_link_doctype()
                    if linked_docname and not frappe.db.exists(linked_doctype, linked_docname):
                        master_doc = producer_site.get_doc(linked_doctype, linked_docname)
                        if master_doc:
                            master_doc = frappe.get_doc(master_doc)
                            sync_doc_dependencies(master_doc, producer_site, visited)
                            insert_doc_without_workflow(master_doc, set_name=linked_docname)
                            frappe.db.commit()

    sync_doc_dependencies(document, producer_site)


def sync_mapped_dependencies(dependencies, producer_site):
	dependencies_created = {}
	for entry in dependencies:
		doc = frappe._dict(json.loads(entry[1]))
		docname = frappe.db.exists(doc.doctype, doc.name)
		if not docname:
			doc = insert_doc_without_workflow(frappe.get_doc(doc), set_child_names=False)
			dependencies_created[entry[0]] = doc.name
		else:
			dependencies_created[entry[0]] = docname

	return dependencies_created


def log_event_sync(update, event_producer, sync_status, error=None):
	"""Log event update received with the sync_status as Synced or Failed"""
	doc = frappe.new_doc("Event Sync Log")
	doc.update_type = update.update_type
	doc.ref_doctype = update.ref_doctype
	doc.status = sync_status
	doc.event_producer = event_producer
	doc.producer_doc = update.docname
	doc.data = frappe.as_json(update.data)
	doc.use_same_name = update.use_same_name
	doc.mapping = update.mapping if update.mapping else None
	if update.use_same_name:
		doc.docname = update.docname
	elif not update.use_same_name and update.has_name_conversion and update.update_type == "Create" and not update.use_remote_doc:
		doc.docname = update.modified_name
	elif not update.use_same_name and update.use_remote_doc:
		doc.docname = update.local_document_name
	else:
		doc.docname = frappe.db.get_value(update.ref_doctype, {"remote_docname": update.docname}, "name")
	if error:
		doc.error = error
	doc.insert()


def get_mapped_update(update, producer_site):
	"""get the new update document with mapped fields"""
	mapping = frappe.get_doc("Document Type Mapping", update.mapping)
	if update.update_type == "Create":
		doc = frappe._dict(json.loads(update.data))
		mapped_update = mapping.get_mapping(doc, producer_site, update.update_type)
		update.data = mapped_update.get("doc")
		update.dependencies = mapped_update.get("dependencies", None)
	elif update.update_type == "Update":
		mapped_update = mapping.get_mapped_update(update, producer_site)
		update.data = mapped_update.get("doc")
		update.dependencies = mapped_update.get("dependencies", None)

	update["ref_doctype"] = mapping.local_doctype
	return update


@frappe.whitelist()
def new_event_notification(producer_url):
	"""Pull data from producer when notified"""
	enqueued_method = "event_streaming.event_streaming.doctype.event_producer.event_producer.pull_from_node"
	jobs = get_jobs()
	if not jobs or enqueued_method not in jobs[frappe.local.site]:
		frappe.enqueue(enqueued_method, queue="default", **{"event_producer": producer_url})


@frappe.whitelist()
def resync(update):
	"""Retry syncing update if failed"""
	update = frappe._dict(json.loads(update))
	producer_site = get_producer_site(update.event_producer)
	event_producer = frappe.get_doc("Event Producer", update.event_producer)
	if update.mapping:
		update = get_mapped_update(update, producer_site)
		update.data = json.loads(update.data)
	return sync(update, producer_site, event_producer, in_retry=True)
