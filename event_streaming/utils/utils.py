import frappe
from frappe.utils.data import get_url as frappe_get_url


def get_url():
    try:
        settings = frappe.get_single("Event Streaming Settings")
        if settings.default_site_url:
            return settings.default_site_url.rstrip("/")
    except Exception:
        pass

    return frappe_get_url()
