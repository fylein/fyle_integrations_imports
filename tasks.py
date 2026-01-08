import logging
import re
from typing import Type, List
from datetime import timedelta
from datetime import datetime

from django.db import models
from django.db.models import F
from django.utils.module_loading import import_string

from fyle_integrations_platform_connector import PlatformConnector
from fyle.platform.exceptions import InvalidTokenError
from fyle_accounting_mappings.models import (
    DestinationAttribute,
    ExpenseAttribute,
    CategoryMapping,
    FyleSyncTimestamp,
    Mapping
)

from fyle_integrations_imports.models import ImportLog
from fyle_integrations_imports.modules.projects import Project
from fyle_integrations_imports.modules.categories import Category
from fyle_integrations_imports.modules.cost_centers import CostCenter
from fyle_integrations_imports.modules.tax_groups import TaxGroup
from fyle_integrations_imports.modules.merchants import Merchant
from fyle_integrations_imports.modules.expense_custom_fields import ExpenseCustomField

from apps.workspaces.models import FyleCredential


logger = logging.getLogger(__name__)
logger.level = logging.INFO


SOURCE_FIELD_CLASS_MAP = {
    'PROJECT': Project,
    'CATEGORY': Category,
    'COST_CENTER': CostCenter,
    'TAX_GROUP': TaxGroup,
    'MERCHANT': Merchant,
}


# TODO: When we need to assign multiple type to credentials we can use this Union[type1, type2, ...]
def trigger_import_via_schedule(
        workspace_id: int,
        destination_field: str | list[str],
        source_field: str,
        sdk_connection_string: str,
        credentials: Type[models.Model],
        destination_sync_methods: List[str] = None,
        is_auto_sync_enabled: bool = False,
        is_3d_mapping: bool = False,
        charts_of_accounts: List[str] = None,
        is_custom: bool = False,
        use_mapping_table: bool = True,
        prepend_code_to_name: bool = False,
        import_without_destination_id: bool = False,
        project_billable_field_detail_key: str = None
):
    """
    Trigger import via schedule
    :param workspace_id: Workspace id
    :param destination_field: Destination field
    :param source_field: Type of attribute (e.g., 'PROJECT', 'CATEGORY', 'COST_CENTER')
    """

    import_log = ImportLog.objects.filter(workspace_id=workspace_id, attribute_type=source_field).first()
    sync_after = import_log.last_successful_run_at if import_log else None
    sdk_connection = None
    app_name = import_string('apps.workspaces.helpers.get_app_name')()

    try:
        if sdk_connection_string:
            sdk_connection = import_string(sdk_connection_string)(credentials, workspace_id)
    except Exception as e:
        logger.info(f"Failed to get sdk connection in workspace_id {workspace_id}. Error: {str(e)}")

    # This is for QBD-Direct-Integration, where we need to increase the import window
    if sdk_connection is None and sdk_connection_string == '' and sync_after:
        sync_after = sync_after - timedelta(minutes=20)

    module_class = SOURCE_FIELD_CLASS_MAP[source_field] if source_field in SOURCE_FIELD_CLASS_MAP else ExpenseCustomField

    args = {
        'workspace_id': workspace_id,
        'destination_field': destination_field,
        'sync_after': sync_after,
        'sdk_connection': sdk_connection,
        'destination_sync_methods': destination_sync_methods,
        'prepend_code_to_name': prepend_code_to_name,
        'is_auto_sync_enabled': is_auto_sync_enabled
    }

    if is_custom:
        args['source_field'] = source_field

    if source_field in ['PROJECT', 'CATEGORY']:
        args['import_without_destination_id'] = import_without_destination_id

    if app_name == 'SAGE_INTACCT' and source_field == 'PROJECT' and destination_field == 'PROJECT':
        args['project_billable_field_detail_key'] = project_billable_field_detail_key

    if source_field == 'CATEGORY':
        args['is_3d_mapping'] = is_3d_mapping
        args['charts_of_accounts'] = charts_of_accounts
        args['use_mapping_table'] = use_mapping_table

    item = module_class(**args)
    item.trigger_import()


def delete_items_mapping(workspace_id: int, destination_attribute_ids: List[int], app_name: str):
    """
    Delete items mapping
    :param workspace_id: Workspace Id
    :param destination_attribute_ids: Destination Attribute Ids
    """
    if app_name == 'NETSUITE':
        CategoryMapping.objects.filter(
            destination_account__id__in=destination_attribute_ids,
            workspace_id=workspace_id
        ).delete()
    elif app_name in ['QUICKBOOKS', 'QBD_CONNECTOR']:
        Mapping.objects.filter(
            destination_id__in=destination_attribute_ids,
            workspace_id=workspace_id,
            source_type='CATEGORY'
        ).delete()


def disable_items(workspace_id: int, is_import_enabled: bool = True):
    """
    Disable and Enable Items Mapping in batches of 200 from the DB
    :param workspace_id: Workspace Id
    :param is_enabled: Boolean indicating if items should be enabled or disabled
    """
    try:
        filters = {}
        expense_attribute_filters = {}
        destination_id_f_path = ''

        app_name = import_string('apps.workspaces.helpers.get_app_name')()

        if app_name == 'NETSUITE':
            filters = {
                'destination_account__isnull': False
            }

        elif app_name in ['QUICKBOOKS', 'QBD_CONNECTOR']:
            filters = {
                'mapping__source_type': 'CATEGORY',
                'mapping__isnull': False,
            }

        if is_import_enabled:
            filters['active'] = False

        destination_attribute_ids = DestinationAttribute.objects.filter(
            **filters,
            workspace_id=workspace_id,
            attribute_type='ACCOUNT',
            display_name='Item',
        ).values_list('id', flat=True)

        fyle_credentials = FyleCredential.objects.get(workspace_id=workspace_id)
        platform = PlatformConnector(fyle_credentials)

        offset = 0
        batch_size = 200

        if app_name == 'NETSUITE':
            destination_id_f_path = 'categorymapping__destination_account__id'
            expense_attribute_filters = {
                'categorymapping__destination_account__id__in': destination_attribute_ids
            }

        elif app_name in ['QUICKBOOKS', 'QBD_CONNECTOR']:
            destination_id_f_path = 'mapping__destination__destination_id'
            expense_attribute_filters = {
                'mapping__destination_id__in': destination_attribute_ids
            }

        while True:
            exepense_attributes = ExpenseAttribute.objects.filter(
                **expense_attribute_filters,
                workspace_id=workspace_id,
                attribute_type='CATEGORY',
                active=True
            ).annotate(
                destination_id=F(destination_id_f_path)
            ).order_by('id')[offset:offset + batch_size]

            if not exepense_attributes:
                break

            process_batch(platform, workspace_id, exepense_attributes)
            offset += batch_size

        platform.categories.sync()
        if not is_import_enabled:
            delete_items_mapping(workspace_id, destination_attribute_ids, app_name)
    except InvalidTokenError:
        logger.info("Invalid Token for Fyle in workspace_id: %s", workspace_id)

    except Exception as e:
        logger.exception("Error disabling items for workspace_id: %s | Error: %s", workspace_id, str(e))


def process_batch(platform: PlatformConnector, workspace_id: int, expense_attributes_batch: list) -> None:
    fyle_payload = []

    for expense_attribute in expense_attributes_batch:
        category = {
            'id': expense_attribute.source_id,
            'name': expense_attribute.value,
            'is_enabled': False
        }
        fyle_payload.append(category)

    if fyle_payload:
        try:
            logger.info(f'Posting items batch for disabling in workspace_id {workspace_id}')
            platform.categories.post_bulk(fyle_payload)
        except Exception as e:
            logger.error(f"Failed to post items batch in workspace_id {workspace_id}. Payload: {fyle_payload}. Error: {str(e)}")


def get_resource_timestamp(fyle_sync_timestamp: FyleSyncTimestamp, resource_name: str) -> datetime:
    """
    Get timestamp for a particular resource from FyleSyncTimestamp
    :param fyle_sync_timestamp: FyleSyncTimestamp object
    :param resource_name: Resource name (e.g., 'employees', 'categories', etc.)
    :return: timestamp or None
    """
    return getattr(fyle_sync_timestamp, f'{resource_name}_synced_at', None)


def clean_options(options: List[str]) -> List[str]:
    """
    Clean options
    :param options: List of options
    :return: List of cleaned options
    """
    cleaned = []
    seen = set()
    rejoined = []
    buffer = None
    for option in options:
        if buffer is not None:
            buffer += ', ' + option
            if option.endswith('"'):
                rejoined.append(buffer[1:-1])
                buffer = None
        elif option.startswith('"') and not option.endswith('"'):
            buffer = option
        elif option.startswith('"') and option.endswith('"') and len(option) > 1:
            rejoined.append(option[1:-1])
        else:
            rejoined.append(option)

    if buffer is not None:
        rejoined.append(buffer)

    for option in rejoined:
        option = re.sub(r'\\+', '', option)
        option = option.replace("\\'", "'")
        if option not in seen:
            cleaned.append(option)
            seen.add(option)
    return cleaned
