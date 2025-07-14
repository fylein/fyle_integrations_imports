import logging
from datetime import datetime
from typing import Dict, List, Type, TypeVar

from django.utils.module_loading import import_string
from django.db.models import Count

from fyle_integrations_platform_connector import PlatformConnector
from fyle_accounting_mappings.models import ExpenseAttribute, DestinationAttribute

from fyle_integrations_imports.modules.base import Base
from fyle_integrations_imports.models import ImportLog
from apps.mappings.exceptions import handle_import_exceptions_v2
from apps.workspaces.models import FyleCredential

T = TypeVar('T')

logger = logging.getLogger(__name__)
logger.level = logging.INFO


class Merchant(Base):
    """
    Class for Merchant module
    """
    def __init__(
        self,
        workspace_id: int,
        destination_field: str,
        sync_after: datetime,
        sdk_connection: Type[T],
        destination_sync_methods: List[str],
        prepend_code_to_name: bool = False,
        is_auto_sync_enabled: bool = True
    ):
        self.is_auto_sync_enabled = is_auto_sync_enabled
        super().__init__(
            workspace_id=workspace_id,
            source_field='MERCHANT',
            destination_field=destination_field,
            platform_class_name='merchants',
            sync_after=sync_after,
            sdk_connection=sdk_connection,
            destination_sync_methods=destination_sync_methods,
            prepend_code_to_name=prepend_code_to_name,
        )

    def trigger_import(self):
        """
        Trigger import for Merchant module
        """
        self.check_import_log_and_start_import()

    def construct_fyle_payload(
        self,
        paginated_destination_attributes: List[DestinationAttribute],
        existing_fyle_attributes_map: List[ExpenseAttribute]
    ):
        """
        Construct Fyle payload for Merchant module
        :param paginated_destination_attributes: List of paginated destination attributes
        :param existing_fyle_attributes_map: Existing Fyle attributes map
        :return: Fyle payload
        """
        destination_attribute_value_list = [attribute.value for attribute in paginated_destination_attributes]
        destination_attribute_inactive_list = [attribute.value for attribute in paginated_destination_attributes if not attribute.active]

        existing_fyle_values = [attribute.value for attribute in existing_fyle_attributes_map]
        existing_fyle_values_inactive = [attribute.value for attribute in existing_fyle_attributes_map if not attribute.active]

        combined_values = (
            set(destination_attribute_value_list) | set(existing_fyle_values)
        ) - set(destination_attribute_inactive_list) - set(existing_fyle_values_inactive)

        # Remove duplicates case-insensitively but keep original casing
        seen_lower = set()
        payload = []
        for value in combined_values:
            value_lower = value.lower()
            if value_lower not in seen_lower:
                seen_lower.add(value_lower)
                payload.append(value)

        return payload

    # construct_payload_and_import_to_fyle method is overridden
    def construct_payload_and_import_to_fyle(
        self,
        platform: PlatformConnector,
        import_log: ImportLog,
        source_placeholder: str = None
    ):
        """
        Construct Payload and Import to fyle in Batches
        """
        filters = self.construct_attributes_filter(self.destination_field, is_auto_sync_enabled=self.is_auto_sync_enabled)

        destination_attributes_count = DestinationAttribute.objects.filter(**filters).count()

        # If there are no destination attributes, mark the import as complete
        if destination_attributes_count == 0:
            import_log.status = 'COMPLETE'
            import_log.last_successful_run_at = datetime.now()
            import_log.error_log = []
            import_log.total_batches_count = 0
            import_log.processed_batches_count = 0
            import_log.save()
            return
        else:
            import_log.total_batches_count = 1
            import_log.save()

        destination_attributes = DestinationAttribute.objects.filter(**filters)
        destination_attributes_without_duplicates = self.remove_duplicate_attributes(destination_attributes)
        platform_class = self.get_platform_class(platform)

        fyle_payload = self.setup_fyle_payload_creation(
            paginated_destination_attributes=destination_attributes_without_duplicates
        )

        self.post_to_fyle_and_sync(
            fyle_payload=fyle_payload,
            resource_class=platform_class,
            is_last_batch=True,
            import_log=import_log
        )

        return destination_attributes_without_duplicates

    # import_destination_attribute_to_fyle method is overridden
    @handle_import_exceptions_v2
    def import_destination_attribute_to_fyle(self, import_log: ImportLog):
        """
        Import destiantion_attributes field to Fyle and Auto Create Mappings
        :param import_log: ImportLog object
        """
        fyle_credentials = FyleCredential.objects.get(workspace_id=self.workspace_id)
        platform = PlatformConnector(fyle_credentials=fyle_credentials)

        self.sync_expense_attributes(platform)

        self.sync_destination_attributes()

        self.construct_payload_and_import_to_fyle(platform, import_log)

        self.sync_expense_attributes(platform)

    def get_existing_fyle_attributes(self, paginated_destination_attribute_values: List[str]):
        """
        Get existing Fyle attributes
        :param paginated_destination_attribute_values: List of paginated destination attribute values
        :return: Existing Fyle attributes
        """
        filters = {
            'workspace_id': self.workspace_id,
            'attribute_type': 'MERCHANT'
        }

        existing_fyle_attributes = ExpenseAttribute.objects.filter(**filters)

        return existing_fyle_attributes


def disable_merchants(workspace_id: int, merchants_to_disable: Dict, is_import_to_fyle_enabled: bool = False, attribute_type: str = None, *args, **kwargs):
    """
    merchants_to_disable object format:
    {
        'destination_id': {
            'value': 'old_merchant_name',
            'updated_value': 'new_merchant_name',
            'code': 'old_code',
            'updated_code': 'new_code' ---- if the code is updated else same as code
        }
    }
    """
    if not is_import_to_fyle_enabled or len(merchants_to_disable) == 0:
        logger.info("Skipping disabling merchants in Fyle | WORKSPACE_ID: %s", workspace_id)
        return

    fyle_credentials = FyleCredential.objects.get(workspace_id=workspace_id)
    platform = PlatformConnector(fyle_credentials=fyle_credentials)

    configuration_model_path = import_string('apps.workspaces.helpers.get_import_configuration_model_path')()
    Configuration = import_string(configuration_model_path)

    use_code_in_naming = False
    columns = Configuration._meta.get_fields()
    if 'import_code_fields' in [field.name for field in columns]:
        use_code_in_naming = Configuration.objects.filter(workspace_id = workspace_id, import_code_fields__contains=['VENDOR']).exists()

    merchant_values = []
    for merchant_map in merchants_to_disable.values():
        if not use_code_in_naming and merchant_map['value'] == merchant_map['updated_value']:
            continue
        elif use_code_in_naming and (merchant_map['value'] == merchant_map['updated_value'] and merchant_map['code'] == merchant_map['updated_code']):
            continue

        merchant_name = import_string('apps.mappings.helpers.prepend_code_to_name')(prepend_code_in_name=use_code_in_naming, value=merchant_map['value'], code=merchant_map['code'])
        merchant_values.append(merchant_name)

    if not use_code_in_naming:
        unique_values = DestinationAttribute.objects.filter(
            workspace_id=workspace_id,
            attribute_type=attribute_type,
            value__in=merchant_values,
        ).values('value').annotate(
            value_count=Count('id')
        ).filter(value_count=1)

        merchant_values = [item['value'] for item in unique_values]

    filters = {
        'workspace_id': workspace_id,
        'attribute_type': 'MERCHANT',
        'value__in': merchant_values,
        'active': True
    }

    bulk_payload = ExpenseAttribute.objects.filter(**filters).values_list('value', flat=True)

    if bulk_payload:
        logger.info(f"Disabling Merchants in Fyle | WORKSPACE_ID: {workspace_id} | COUNT: {len(bulk_payload)}")
        platform.merchants.post(bulk_payload, delete_merchants=True)
        ExpenseAttribute.objects.filter(**filters).update(active=False, updated_at=datetime.now())
    else:
        logger.info(f"No Merchants to Disable in Fyle | WORKSPACE_ID: {workspace_id}")

    return bulk_payload
