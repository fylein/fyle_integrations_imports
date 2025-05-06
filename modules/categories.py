import math
import copy
import logging
from typing import Dict, List, Type, TypeVar
from datetime import datetime

from django.db.models import Q
from django.utils.module_loading import import_string

from fyle_integrations_platform_connector import PlatformConnector
from fyle_accounting_mappings.models import (
    DestinationAttribute,
    ExpenseAttribute,
    CategoryMapping
)

from fyle_integrations_imports.modules.base import Base
from fyle_integrations_imports.models import ImportLog
from apps.workspaces.models import FyleCredential

T = TypeVar('T')

logger = logging.getLogger(__name__)
logger.level = logging.INFO


class Category(Base):
    """
    Class for Category module
    """
    def __init__(
            self,
            workspace_id: int,
            destination_field: str,
            sync_after: datetime,
            sdk_connection: Type[T],
            destination_sync_methods: List[str],
            is_auto_sync_enabled: bool,
            is_3d_mapping: bool,
            charts_of_accounts: List[str],
            use_mapping_table: bool = True,
            prepend_code_to_name: bool = False,
            import_without_destination_id: bool = False
    ):
        self.is_auto_sync_enabled = is_auto_sync_enabled
        self.is_3d_mapping = is_3d_mapping
        self.charts_of_accounts = charts_of_accounts
        self.use_mapping_table = use_mapping_table

        super().__init__(
            workspace_id=workspace_id,
            source_field='CATEGORY',
            destination_field=destination_field,
            platform_class_name='categories',
            sync_after=sync_after,
            sdk_connection=sdk_connection,
            destination_sync_methods=destination_sync_methods,
            prepend_code_to_name=prepend_code_to_name,
            import_without_destination_id=import_without_destination_id
        )

    def trigger_import(self):
        """
        Trigger import for Category module
        """
        self.check_import_log_and_start_import()

    def construct_attributes_filter(self, attribute_type: str, is_destination_type: bool = True, paginated_destination_attribute_values: List[str] = []):
        """
        Construct the attributes filter
        :param attribute_type: attribute type
        :param paginated_destination_attribute_values: paginated destination attribute values
        :return: dict
        """
        filters = Q(attribute_type=attribute_type, workspace_id=self.workspace_id)

        if self.sync_after and self.platform_class_name != 'expense_custom_fields' and is_destination_type:
            filters &= Q(updated_at__gte=self.sync_after)

        if paginated_destination_attribute_values:
            filters &= Q(value__in=paginated_destination_attribute_values)

        if not self.sync_after and is_destination_type:
            filters &= Q(active=True)

        account_filters = copy.deepcopy(filters)

        if attribute_type != 'CATEGORY':
            if 'accounts' in self.destination_sync_methods:
                account_filters = filters & Q(display_name='Account')
                if hasattr(self, 'charts_of_accounts') and len(self.charts_of_accounts) > 0:
                    account_filters &= Q(detail__account_type__in=self.charts_of_accounts)

            if 'items' in self.destination_sync_methods:
                item_filter = filters & Q(display_name='Item')
                filters = account_filters | item_filter if 'accounts' in self.destination_sync_methods else item_filter

            if 'expense_categories' in self.destination_sync_methods:
                expense_category_filter = filters & Q(display_name='Expense Category')
                filters = account_filters | expense_category_filter if 'accounts' in self.destination_sync_methods else expense_category_filter

            if 'items' not in self.destination_sync_methods:
                filters = account_filters

        return filters

    def construct_payload_and_import_to_fyle(
        self,
        platform: PlatformConnector,
        import_log: ImportLog
    ):
        """
        Construct Payload and Import to fyle in Batches
        """
        filters = self.construct_attributes_filter(self.destination_field, True)
        destination_attributes_count = DestinationAttribute.objects.filter(filters).count()

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
            import_log.total_batches_count = math.ceil(destination_attributes_count / 200)
            import_log.save()

        destination_attributes_generator = self.get_destination_attributes_generator(destination_attributes_count, filters)
        platform_class = self.get_platform_class(platform)
        posted_destination_attributes = []
        for paginated_destination_attributes, is_last_batch in destination_attributes_generator:
            fyle_payload = self.setup_fyle_payload_creation(
                paginated_destination_attributes=paginated_destination_attributes
            )

            self.post_to_fyle_and_sync(
                fyle_payload=fyle_payload,
                resource_class=platform_class,
                is_last_batch=is_last_batch,
                import_log=import_log
            )

            posted_destination_attributes.extend(paginated_destination_attributes)

        return posted_destination_attributes

    def get_destination_attributes_generator(self, destination_attributes_count: int, filters: dict):
        """
        Get destination attributes generator
        :param destination_attributes_count: Destination attributes count
        :param filters: dict
        :return: Generator of destination_attributes
        """

        for offset in range(0, destination_attributes_count, 200):
            limit = offset + 200
            paginated_destination_attributes = DestinationAttribute.objects.filter(filters).order_by('value', 'id')[offset:limit]
            paginated_destination_attributes_without_duplicates = self.remove_duplicate_attributes(paginated_destination_attributes)
            is_last_batch = True if limit >= destination_attributes_count else False

            yield paginated_destination_attributes_without_duplicates, is_last_batch

    def get_existing_fyle_attributes(self, paginated_destination_attribute_values: List[str]):
        """
        Get Existing Fyle Attributes
        :param paginated_destination_attribute_values: List of DestinationAttribute values
        :return: Map of attribute value to attribute source_id
        """
        filters = self.construct_attributes_filter(self.source_field, False, paginated_destination_attribute_values)
        existing_expense_attributes_values = ExpenseAttribute.objects.filter(filters).values('value', 'source_id')
        # This is a map of attribute name to attribute source_id
        return {attribute['value'].lower(): attribute['source_id'] for attribute in existing_expense_attributes_values}

    def construct_fyle_payload(
        self,
        paginated_destination_attributes: List[DestinationAttribute],
        existing_fyle_attributes_map: object
    ):
        """
        Construct Fyle payload for Category module
        :param paginated_destination_attributes: List of paginated destination attributes
        :param existing_fyle_attributes_map: Existing Fyle attributes map
        :param is_auto_sync_status_allowed: Is auto sync status allowed
        :return: Fyle payload
        """
        payload = []

        for attribute in paginated_destination_attributes:
            category = {
                'name': attribute.value,
                'code': attribute.destination_id if not self.import_without_destination_id else None,
                'is_enabled': attribute.active if attribute.value != 'Unspecified' else True
            }

            # Create a new category if it does not exist in Fyle
            if attribute.value.lower() not in existing_fyle_attributes_map:
                payload.append(category)
            # Disable the existing category in Fyle if auto-sync status is allowed and the destination_attributes is inactive
            elif self.is_auto_sync_enabled and not attribute.active:
                category['id'] = existing_fyle_attributes_map[attribute.value.lower()]
                payload.append(category)

        return payload

    def create_category_mappings(self):
        """
        Create Category mappings
        :return: None
        """
        filters = {
            'workspace_id': self.workspace_id,
            'attribute_type': self.destination_field
        }
        if self.destination_field in ['EXPENSE_CATEGORY', 'EXPENSE_TYPE']:
            filters['destination_expense_head__isnull'] = True
        elif self.destination_field == 'ACCOUNT':
            filters['destination_account__isnull'] = True

        # get all the destination attributes that have category mappings as null
        destination_attributes: List[DestinationAttribute] = DestinationAttribute.objects.filter(**filters)

        destination_attributes_without_duplicates = []
        destination_attributes_without_duplicates = self.remove_duplicate_attributes(destination_attributes)

        CategoryMapping.bulk_create_mappings(
            destination_attributes_without_duplicates,
            self.destination_field,
            self.workspace_id
        )

    def get_mapped_attributes_ids(self, errored_attribute_ids: List[int]):
        """
        Get mapped attributes ids
        :param errored_attribute_ids: list[int]
        :return: list[int]
        """
        mapped_attribute_ids = []
        if self.source_field == "CATEGORY":
            params = {
                'source_category_id__in': errored_attribute_ids,
            }

            if self.destination_field in ['EXPENSE_CATEGORY', 'EXPENSE_TYPE']:
                params['destination_expense_head_id__isnull'] = False
            else:
                params['destination_account_id__isnull'] = False

            mapped_attribute_ids: List[int] = CategoryMapping.objects.filter(
                **params
            ).values_list('source_category_id', flat=True)

        return mapped_attribute_ids


def disable_categories(workspace_id: int, categories_to_disable: Dict, is_import_to_fyle_enabled: bool = False, *args, **kwargs):
    """
    categories_to_disable object format:
    {
        'destination_id': {
            'value': 'old_category_name',
            'updated_value': 'new_category_name',
            'code': 'old_code',
            'updated_code': 'new_code' ---- if the code is updated else same as code
        }
    }
    """
    if not is_import_to_fyle_enabled or len(categories_to_disable) == 0:
        logger.info("Skipping disabling categories in Fyle | WORKSPACE_ID: %s", workspace_id)
        return

    fyle_credentials = FyleCredential.objects.get(workspace_id=workspace_id)
    platform = PlatformConnector(fyle_credentials=fyle_credentials)

    configuration_model_path = import_string('apps.workspaces.helpers.get_import_configuration_model_path')()
    Configuration = import_string(configuration_model_path)

    use_code_in_naming = False
    columns = Configuration._meta.get_fields()
    if 'import_code_fields' in [field.name for field in columns]:
        use_code_in_naming = Configuration.objects.filter(workspace_id=workspace_id, import_code_fields__contains=['ACCOUNT']).exists()

    category_values = []
    for category_map in categories_to_disable.values():
        if not use_code_in_naming and category_map['value'] == category_map['updated_value']:
            continue
        elif use_code_in_naming and (category_map['value'] == category_map['updated_value'] and category_map['code'] == category_map['updated_code']):
            continue

        category_name = import_string('apps.mappings.helpers.prepend_code_to_name')(prepend_code_in_name=use_code_in_naming, value=category_map['value'], code=category_map['code'])
        category_values.append(category_name)

    filters = {
        'workspace_id': workspace_id,
        'attribute_type': 'CATEGORY',
        'value__in': category_values,
        'active': True
    }

    expense_attribute_value_map = {}
    for destination_id, v in categories_to_disable.items():
        category_name = v['value']
        category_name = import_string('apps.mappings.helpers.prepend_code_to_name')(prepend_code_in_name=use_code_in_naming, value=v['value'], code=v['code'])
        expense_attribute_value_map[category_name] = destination_id

    bulk_payload = []

    expense_attributes = ExpenseAttribute.objects.filter(**filters)

    for expense_attribute in expense_attributes:
        code = expense_attribute_value_map.get(expense_attribute.value, None)
        if code:
            payload = {
                'name': expense_attribute.value,
                'code': code,
                'is_enabled': False,
                'id': expense_attribute.source_id
            }
        else:
            logger.error(f"Category with value {expense_attribute.value} not found | WORKSPACE_ID: {workspace_id}")

        bulk_payload.append(payload)

    if bulk_payload:
        logger.info(f"Disabling Category in Fyle | WORKSPACE_ID: {workspace_id} | COUNT: {len(bulk_payload)}")
        platform.categories.post_bulk(bulk_payload)
    else:
        logger.info(f"No Category to Disable in Fyle | WORKSPACE_ID: {workspace_id}")

    return bulk_payload
