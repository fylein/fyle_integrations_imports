import logging

from django.utils.module_loading import import_string
from datetime import datetime, timedelta, timezone
from fyle_integrations_imports.models import ImportLog
from fyle_integrations_imports.modules.projects import Project
from fyle_integrations_imports.modules.categories import Category
from fyle_integrations_imports.modules.cost_centers import CostCenter
from fyle_integrations_imports.modules.tax_groups import TaxGroup
from fyle_integrations_imports.modules.merchants import Merchant
from fyle_integrations_imports.modules.expense_custom_fields import ExpenseCustomField
from fyle_accounting_mappings.models import (
    DestinationAttribute,
    ExpenseAttribute
)
from apps.workspaces.models import FyleCredential
from fyle_integrations_platform_connector import PlatformConnector

from typing import Type, List
from django.db import models
from django.db.models import F

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
        destination_field: str,
        source_field: str,
        sdk_connection_string: str,
        credentials: Type[models.Model],
        destination_sync_methods: List[str] = None,
        is_auto_sync_enabled: bool = False,
        is_3d_mapping: bool = False,
        charts_of_accounts: List[str] = None,
        is_custom: bool = False,
        use_mapping_table: bool = True,
        prepend_code_to_name: bool = False
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

    module_class = SOURCE_FIELD_CLASS_MAP[source_field] if source_field in SOURCE_FIELD_CLASS_MAP else ExpenseCustomField

    args = {
        'workspace_id': workspace_id,
        'destination_field': destination_field,
        'sync_after': sync_after,
        'sdk_connection': sdk_connection,
        'destination_sync_methods': destination_sync_methods,
    }

    if is_custom:
        args['source_field'] = source_field

    if source_field in ['PROJECT', 'CATEGORY']:
        args['is_auto_sync_enabled'] = is_auto_sync_enabled

    if source_field == 'CATEGORY':
        args['is_3d_mapping'] = is_3d_mapping
        args['charts_of_accounts'] = charts_of_accounts
        args['use_mapping_table'] = use_mapping_table
        args['prepend_code_to_name'] = prepend_code_to_name

    item = module_class(**args)
    item.trigger_import()


def disable_category_for_items_mapping(
        workspace_id: int,
        sdk_connection_string: str,
        credentials: Type[models.Model]
):
    """
    Disable Category for Items Mapping
    :param workspace_id: Workspace Id
    :return: None
    """
    destination_attribute_ids = DestinationAttribute.objects.filter(
        workspace_id=workspace_id,
        mapping__isnull=False,
        attribute_type='ACCOUNT',
        mapping__source_type='CATEGORY',
        display_name='Item',
        active=True
    ).values_list('id', flat=True)

    expense_attributes_to_disable = ExpenseAttribute.objects.filter(
        attribute_type='CATEGORY',
        mapping__destination_id__in=destination_attribute_ids,
        active=True
    )

    if expense_attributes_to_disable:
        import_log, is_created = ImportLog.objects.get_or_create(
            workspace_id=workspace_id,
            attribute_type='CATEGORY',
            defaults={
                'status': 'IN_PROGRESS'
            }
        )

        last_successful_run_at = None
        if import_log and import_log.status != 'IN_PROGRESS' and not is_created:
            last_successful_run_at = import_log.last_successful_run_at if import_log.last_successful_run_at else None
            time_difference = datetime.now() - timedelta(minutes=32)
            offset_aware_time_difference = time_difference.replace(tzinfo=timezone.utc)

            # if the import_log is present and the last_successful_run_at is less than 30mins then we need to update it
            # so that the schedule can run
            if last_successful_run_at and offset_aware_time_difference\
                and (offset_aware_time_difference < last_successful_run_at):
                import_log.last_successful_run_at = offset_aware_time_difference
                import_log.save()

        trigger_import_via_schedule(
            workspace_id,
            'ACCOUNT',
            'CATEGORY',
            sdk_connection_string,
            credentials,
            ['items'],
            True
        )

        # setting the import_log.last_successful_run_at to None value so that import_categories works perfectly
        # and none of the values are missed . It will be a full run.
        import_log = ImportLog.objects.filter(workspace_id=workspace_id, attribute_type='CATEGORY').first()
        if import_log.last_successful_run_at and last_successful_run_at:
            import_log.last_successful_run_at = None
            import_log.save()
    else:
        return


def disable_or_enable_items_mapping(workspace_id: int, is_enabled: bool):
    """
    Disable and Enable Items Mapping in batches of 200 from the DB
    :param workspace_id: Workspace Id
    :param is_enabled: Boolean indicating if items should be enabled or disabled
    """
    destination_attribute_ids = DestinationAttribute.objects.filter(
        workspace_id=workspace_id,
        mapping__isnull=False,
        attribute_type='ACCOUNT',
        mapping__source_type='CATEGORY',
        display_name='Item',
        active=True
    ).values_list('id', flat=True)

    fyle_credentials = FyleCredential.objects.get(workspace_id=workspace_id)
    platform = PlatformConnector(fyle_credentials)

    # Function to process a batch of expense attributes
    def process_batch(expense_attributes_batch):
        fyle_payload = []

        for expense_attribute in expense_attributes_batch:
            category = {
                'name': expense_attribute.value,
                'code': expense_attribute.destination_id,
                'is_enabled': is_enabled
            }
            fyle_payload.append(category)

        if fyle_payload:
            try:
                platform.categories.post_bulk(fyle_payload)
            except Exception as e:
                logger.error(f"Failed to post items batch in workspace_id {workspace_id}. Payload: {fyle_payload}. Error: {str(e)}")

    offset = 0
    batch_size = 200

    while True:
        exepense_attributes = ExpenseAttribute.objects.filter(
            attribute_type='CATEGORY',
            mapping__destination_id__in=destination_attribute_ids,
            active=not is_enabled
        ).annotate(
            destination_id=F('mapping__destination__destination_id')
        ).order_by('id')[offset:offset + batch_size]

        if not exepense_attributes:
            break

        process_batch(exepense_attributes)
        offset += batch_size

    platform.categories.sync()
