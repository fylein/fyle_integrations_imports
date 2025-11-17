from datetime import datetime
from fyle_accounting_mappings.models import FyleSyncTimestamp


def get_resource_timestamp(fyle_sync_timestamp: FyleSyncTimestamp, resource_name: str) -> datetime:
    """
    Get timestamp for a particular resource from FyleSyncTimestamp
    :param fyle_sync_timestamp: FyleSyncTimestamp object
    :param resource_name: Resource name (e.g., 'employees', 'categories', etc.)
    :return: timestamp or None
    """
    return getattr(fyle_sync_timestamp, f'{resource_name}_synced_at', None)
