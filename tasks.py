from django.utils.module_loading import import_string
from fyle_integrations_imports.models import ImportLog
from fyle_integrations_imports.modules.projects import Project
from fyle_integrations_imports.modules.categories import Category
from typing import Type
from django.db import models


SOURCE_FIELD_CLASS_MAP = {
    'PROJECT': Project,
    'CATEGORY': Category
}


# TODO: When we need to assign multiple type to credentials we can use this Union[type1, type2, ...]
def trigger_import_via_schedule(
        workspace_id: int,
        destination_field: str,
        source_field: str,
        sdk_connection_string: str,
        credentials: Type[models.Model],
        destination_sync_method: str = None,
        is_auto_sync_enabled: bool = False,
        is_3d_mapping: bool = False,
        is_custom: bool = False
    ):
    """
    Trigger import via schedule
    :param workspace_id: Workspace id
    :param destination_field: Destination field
    :param source_field: Type of attribute (e.g., 'PROJECT', 'CATEGORY', 'COST_CENTER')
    """

    import_log = ImportLog.objects.filter(workspace_id=workspace_id, attribute_type=source_field).first()
    sync_after = import_log.last_successful_run_at if import_log else None

    sdk_connection = import_string(sdk_connection_string)(credentials, workspace_id)

    module_class = SOURCE_FIELD_CLASS_MAP[source_field]
    if source_field == 'CATEGORY':
        item = module_class(workspace_id, destination_field, sync_after, sdk_connection, destination_sync_method, is_auto_sync_enabled, is_3d_mapping)
    else:
        item = module_class(workspace_id, destination_field, sync_after, sdk_connection, destination_sync_method, is_auto_sync_enabled)

    item.trigger_import()
