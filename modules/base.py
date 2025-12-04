import math
import logging
from typing import List, Type, TypeVar
from datetime import datetime, timedelta, timezone

from django.db.models.functions import Lower
from django.utils.module_loading import import_string

from fyle_integrations_platform_connector import PlatformConnector
from fyle_accounting_mappings.models import (
    Mapping,
    DestinationAttribute,
    ExpenseAttribute,
    FyleSyncTimestamp,
    CategoryMapping
)

from apps.workspaces.helpers import get_app_name
from apps.workspaces.models import FeatureConfig, FyleCredential
from fyle_integrations_imports.models import ImportLog
from apps.mappings.exceptions import handle_import_exceptions_v2

T = TypeVar('T')

logger = logging.getLogger(__name__)
logger.level = logging.INFO

RESOURCE_NAME_MAP = {
    'employees': 'employee',
    'categories': 'category',
    'projects': 'project',
    'cost_centers': 'cost_center',
    'expense_custom_fields': 'expense_field',
    'corporate_cards': 'corporate_card',
    'dependent_fields': 'dependent_field',
    'tax_groups': 'tax_group',
}


class Base:
    """
    The Base class for all the modules
    """
    def __init__(
            self,
            workspace_id: int,
            source_field: str,
            destination_field: str,
            platform_class_name: str,
            sync_after:datetime,
            sdk_connection: Type[T],
            destination_sync_methods: List[str],
            prepend_code_to_name: bool = False,
            import_without_destination_id: bool = False
    ):
        self.workspace_id = workspace_id
        self.source_field = source_field
        self.destination_field = destination_field
        self.platform_class_name = platform_class_name
        self.sync_after = sync_after
        self.sdk_connection = sdk_connection
        self.destination_sync_methods = destination_sync_methods
        self.prepend_code_to_name = prepend_code_to_name
        self.import_without_destination_id = import_without_destination_id

    def resolve_expense_attribute_errors(self):
        """
        Resolve Expense Attribute Errors
        :return: None
        """
        error_model_import_string = import_string('apps.workspaces.helpers.get_error_model_path')()
        
        # for sage file export, we don't have the error model
        if not error_model_import_string:
            return
        
        Error = import_string(error_model_import_string)

        if self.source_field == "CATEGORY":
            errored_attribute_ids: List[int] = Error.objects.filter(
                is_resolved=False,
                workspace_id=self.workspace_id,
                type='{}_MAPPING'.format(self.source_field)
            ).values_list('expense_attribute_id', flat=True)

            if errored_attribute_ids:
                mapped_attribute_ids = None

                if not self.source_field == 'CATEGORY' or self.use_mapping_table:
                    mapped_attribute_ids = Mapping.objects.filter(source_id__in=errored_attribute_ids).values_list('source_id', flat=True)
                elif self.source_field == 'CATEGORY' and not self.use_mapping_table:
                    mapped_attribute_ids = self.get_mapped_attributes_ids(errored_attribute_ids)

                if mapped_attribute_ids:
                    Error.objects.filter(expense_attribute_id__in=mapped_attribute_ids).update(is_resolved=True)

    def get_platform_class(self, platform: PlatformConnector):
        """
        Get the platform class
        :param platform: PlatformConnector object
        :return: platform class
        """
        return getattr(platform, self.platform_class_name)

    def get_code_prepended_name(self, prepend_code_in_name: bool, value: str, code: str = None) -> str:
        """
        Format the attribute name based on the use_code_in_naming flag
        """
        if prepend_code_in_name and code:
            return "{}: {}".format(code, value)
        return value

    def construct_attributes_filter(self, attribute_type: str, is_destination_type: bool = True, paginated_destination_attribute_values: List[str] = [], is_auto_sync_enabled: bool = False):
        """
        Construct the attributes filter
        :param attribute_type: attribute type (can be string or list of strings)
        :param paginated_destination_attribute_values: paginated destination attribute values
        :return: dict
        """
        filters = {
            'workspace_id': self.workspace_id
        }
        
        # Support both single attribute_type and list of attribute_types
        if isinstance(attribute_type, list):
            filters['attribute_type__in'] = attribute_type
        else:
            filters['attribute_type'] = attribute_type

        if (not self.sync_after and is_destination_type) or (not is_auto_sync_enabled):
            filters['active'] = True

        if self.sync_after and self.platform_class_name not in ['expense_custom_fields', 'merchants'] and is_destination_type:
            filters['updated_at__gte'] = self.sync_after

        if self.platform_class_name not in ['expense_custom_fields', 'merchants'] and paginated_destination_attribute_values:
            lower_paginated_attribute_values = [value.lower() for value in paginated_destination_attribute_values]
            filters['value_lower__in'] = lower_paginated_attribute_values

        return filters

    def remove_duplicate_attributes(self, destination_attributes: List[DestinationAttribute], prepend_code: bool = True):
        """
        Remove duplicate attributes
        :param destination_attributes: destination attributes
        :return: list[DestinationAttribute]
        """
        unique_attributes = []
        attribute_values = []

        for destination_attribute in destination_attributes:
            attribute_value = destination_attribute.value
            if prepend_code:
                attribute_value = self.get_code_prepended_name(self.prepend_code_to_name, destination_attribute.value, destination_attribute.code)

            if attribute_value.lower() not in attribute_values:
                destination_attribute.value = attribute_value
                unique_attributes.append(destination_attribute)
                attribute_values.append(attribute_value.lower())

        return unique_attributes

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

        posted_destination_attributes = self.construct_payload_and_import_to_fyle(platform, import_log)

        self.sync_expense_attributes(platform)

        if posted_destination_attributes:
            self.create_mappings(posted_destination_attributes)

        if self.source_field == 'CATEGORY' and self.is_3d_mapping:
            self.create_ccc_mappings()

        self.resolve_expense_attribute_errors()

    def create_mappings(self, posted_destination_attributes: List[DestinationAttribute]):
        """
        Create mappings
        """
        if not self.source_field == 'CATEGORY' or self.use_mapping_table:
            if posted_destination_attributes:
                Mapping.bulk_create_mappings(
                    posted_destination_attributes,
                    self.source_field,
                    self.destination_field,
                    self.workspace_id
                )
        elif self.source_field == 'CATEGORY' and not self.use_mapping_table:
            self.create_category_mappings()

    def create_ccc_mappings(self):
        """
        Create CCC mappings
        :return: None
        """
        CategoryMapping.bulk_create_ccc_category_mappings(self.workspace_id)

    def sync_expense_attributes(self, platform: PlatformConnector):
        """
        Sync expense attributes
        :param platform: PlatformConnector object
        """
        platform_class = self.get_platform_class(platform)
        if self.platform_class_name == 'merchants':
            platform.merchants.sync()
            return

        sync_after = None
        resource_name = RESOURCE_NAME_MAP.get(self.platform_class_name, self.platform_class_name)
        fyle_sync_timestamp = FyleSyncTimestamp.objects.get(workspace_id=self.workspace_id)
        fyle_webhook_sync_enabled = FeatureConfig.get_feature_config(self.workspace_id, 'fyle_webhook_sync_enabled')

        if fyle_webhook_sync_enabled and fyle_sync_timestamp:
            sync_after = import_string('fyle_integrations_imports.tasks.get_resource_timestamp')(fyle_sync_timestamp, resource_name)
            logger.info(f'Syncing {resource_name} for workspace_id {self.workspace_id} with webhook mode | sync_after: {sync_after}')
        else:
            sync_after = self.sync_after if self.sync_after else None
            logger.info(f'Syncing {resource_name} for workspace_id {self.workspace_id} with full sync mode')

        platform_class.sync(sync_after=sync_after)

        if fyle_webhook_sync_enabled and fyle_sync_timestamp:
            fyle_sync_timestamp.update_sync_timestamp(self.workspace_id, resource_name)

    def sync_destination_attributes(self):
        """
        Sync destination attributes
        """
        if self.sdk_connection:
            for destination_sync_method in self.destination_sync_methods:
                sync = getattr(self.sdk_connection, 'sync_{}'.format(destination_sync_method))
                sync()

    def construct_payload_and_import_to_fyle(
        self,
        platform: PlatformConnector,
        import_log: ImportLog
    ):
        """
        Construct Payload and Import to fyle in Batches
        """
        is_auto_sync_enabled = False

        if hasattr(self, 'is_auto_sync_enabled'):
            is_auto_sync_enabled = self.is_auto_sync_enabled

        filters = self.construct_attributes_filter(self.destination_field, True, is_auto_sync_enabled=is_auto_sync_enabled)
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
            paginated_destination_attributes = DestinationAttribute.objects.filter(**filters).order_by('value', 'id')[offset:limit]
            paginated_destination_attributes_without_duplicates = self.remove_duplicate_attributes(paginated_destination_attributes)
            is_last_batch = True if limit >= destination_attributes_count else False

            yield paginated_destination_attributes_without_duplicates, is_last_batch

    def setup_fyle_payload_creation(
        self,
        paginated_destination_attributes: List[DestinationAttribute]
    ):
        """
        Setup Fyle Payload Creation
        :param paginated_destination_attributes: List of DestinationAttribute objects
        :param is_auto_sync_status_allowed: bool
        :return: Fyle Payload
        """
        paginated_destination_attribute_values = [attribute.value for attribute in paginated_destination_attributes]
        existing_expense_attributes_map = self.get_existing_fyle_attributes(paginated_destination_attribute_values)
        return self.construct_fyle_payload(paginated_destination_attributes, existing_expense_attributes_map)

    def get_existing_fyle_attributes(self, paginated_destination_attribute_values: List[str]):
        """
        Get Existing Fyle Attributes
        :param paginated_destination_attribute_values: List of DestinationAttribute values
        :return: Map of attribute value to attribute source_id
        """
        filters = self.construct_attributes_filter(self.source_field, False, paginated_destination_attribute_values)
        filters.pop('active')
        existing_expense_attributes_values = ExpenseAttribute.objects.annotate(value_lower=Lower('value')).filter(**filters).values('value', 'source_id')
        # This is a map of attribute name to attribute source_id
        return {attribute['value'].lower(): attribute['source_id'] for attribute in existing_expense_attributes_values}

    def post_to_fyle_and_sync(self, fyle_payload: List[object], resource_class, is_last_batch: bool, import_log: ImportLog):
        """
        Post to Fyle and Sync
        :param fyle_payload: List of Fyle Payload
        :param resource_class: Platform Class
        :param is_last_batch: bool
        :param import_log: ImportLog object
        """
        logger.info("| Importing {} to Fyle | Content: {{WORKSPACE_ID: {} Fyle Payload count: {} is_last_batch: {}}}".format(self.destination_field, self.workspace_id, len(fyle_payload), is_last_batch))

        if fyle_payload and self.platform_class_name in ['expense_custom_fields']:
            resource_class.post(fyle_payload)
        elif fyle_payload and self.platform_class_name in ['merchants']:
            resource_class.post(fyle_payload, skip_existing_merchants=True)
        elif fyle_payload:
            resource_class.post_bulk(fyle_payload)

        self.update_import_log_post_import(is_last_batch, import_log)

    def update_import_log_post_import(self, is_last_batch: bool, import_log: ImportLog):
        """
        Update Import Log Post Import
        :param is_last_batch: bool
        :param import_log: ImportLog object
        """
        if is_last_batch:
            import_log.last_successful_run_at = datetime.now()
            import_log.processed_batches_count += 1
            import_log.status = 'COMPLETE'
            import_log.error_log = []
        else:
            import_log.processed_batches_count += 1

        import_log.save()

    def check_import_log_and_start_import(self):
        """
        Checks if the import is already in progress and if not, starts the import process
        """
        import_log, is_created = ImportLog.objects.get_or_create(
            workspace_id=self.workspace_id,
            attribute_type=self.source_field,
            defaults={
                'status': 'IN_PROGRESS'
            }
        )

        app_name = get_app_name()

        time_difference = datetime.now() - timedelta(minutes=30)
        offset_aware_time_difference = time_difference.replace(tzinfo=timezone.utc)
        # If the import is already in progress or if the last successful run is within 30 minutes, don't start the import process
        if app_name not in ['Sage File Export'] and ((import_log.status == 'IN_PROGRESS' and not is_created) \
            or (self.sync_after and (self.sync_after > offset_aware_time_difference))):
            return

        # Update the required values since we're beginning the import process
        else:
            import_log.status = 'IN_PROGRESS'
            import_log.processed_batches_count = 0
            import_log.total_batches_count = 0
            import_log.save()

            self.import_destination_attribute_to_fyle(import_log)
