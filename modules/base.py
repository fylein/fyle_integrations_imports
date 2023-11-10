import math
from typing import List
from datetime import (
    datetime,
    timedelta,
    timezone
)
from fyle_integrations_platform_connector import PlatformConnector
from fyle_accounting_mappings.models import (
    Mapping,
    DestinationAttribute,
    ExpenseAttribute,
    CategoryMapping
)
from apps.workspaces.models import FyleCredential
from apps.mappings.models import ImportLog
from apps.workspaces.models import SageIntacctCredential
from apps.sage_intacct.utils import SageIntacctConnector
from apps.mappings.exceptions import handle_import_exceptions
from apps.tasks.models import Error
from apps.workspaces.models import Configuration


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
        ):
        self.workspace_id = workspace_id
        self.source_field = source_field
        self.destination_field = destination_field
        self.platform_class_name = platform_class_name
        self.sync_after = sync_after


    def get_platform_class(self, platform: PlatformConnector):
        """
        Get the platform class
        :param platform: PlatformConnector object
        :return: platform class
        """
        return getattr(platform, self.platform_class_name)
    
    def get_auto_sync_permission(self):
        """
        Get the auto sync permission
        :return: bool
        """
        is_auto_sync_status_allowed = False
        if (self.destination_field == 'PROJECT' and self.source_field == 'PROJECT') or self.source_field == 'CATEGORY':
            is_auto_sync_status_allowed = True

        return is_auto_sync_status_allowed
    
    def construct_attributes_filter(self, attribute_type: str, paginated_destination_attribute_values: List[str] = []):
        """
        Construct the attributes filter
        :param attribute_type: attribute type
        :param paginated_destination_attribute_values: paginated destination attribute values
        :return: dict
        """
        filters = {
            'attribute_type': attribute_type,
            'workspace_id': self.workspace_id
        }

        if self.sync_after and self.platform_class_name != 'expense_custom_fields':
            filters['updated_at__gte'] = self.sync_after

        if paginated_destination_attribute_values:
            filters['value__in'] = paginated_destination_attribute_values

        return filters
    
    def remove_duplicate_attributes(self, destination_attributes: List[DestinationAttribute]):
        """
        Remove duplicate attributes
        :param destination_attributes: destination attributes
        :return: list[DestinationAttribute]
        """
        unique_attributes = []
        attribute_values = []

        for destination_attribute in destination_attributes:
            if destination_attribute.value.lower() not in attribute_values:
                unique_attributes.append(destination_attribute)
                attribute_values.append(destination_attribute.value.lower())

        return unique_attributes

    def __get_mapped_attributes_ids(self, errored_attribute_ids: List[int]):
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

            if self.destination_field == 'EXPENSE_TYPE':
                params['destination_expense_head_id__isnull'] = False
            else:
                params['destination_account_id__isnull'] =  False

            mapped_attribute_ids: List[int] = CategoryMapping.objects.filter(
                **params
            ).values_list('source_category_id', flat=True)

        return mapped_attribute_ids

    def resolve_expense_attribute_errors(self):
        """
        Resolve Expense Attribute Errors
        :return: None
        """
        errored_attribute_ids: List[int] = Error.objects.filter(
            is_resolved=False,
            workspace_id=self.workspace_id,
            type='{}_MAPPING'.format(self.source_field)
        ).values_list('expense_attribute_id', flat=True)

        if errored_attribute_ids:
            mapped_attribute_ids = self.__get_mapped_attributes_ids(errored_attribute_ids)
            if mapped_attribute_ids:
                Error.objects.filter(expense_attribute_id__in=mapped_attribute_ids).update(is_resolved=True)

    def create_ccc_category_mappings(self):
        """
        Create CCC Category Mappings
        :return: None
        """
        configuration = Configuration.objects.filter(workspace_id=self.workspace_id).first()
        if configuration.reimbursable_expenses_object == 'EXPENSE_REPORT' and \
            configuration.corporate_credit_card_expenses_object in ('BILL', 'CHARGE_CARD_TRANSACTION', 'JOURNAL_ENTRY') and\
            self.source_field == 'CATEGORY':
            CategoryMapping.bulk_create_ccc_category_mappings(self.workspace_id)

    @handle_import_exceptions
    def import_destination_attribute_to_fyle(self, import_log: ImportLog):
        """
        Import destiantion_attributes field to Fyle and Auto Create Mappings
        :param import_log: ImportLog object
        """
        fyle_credentials = FyleCredential.objects.get(workspace_id=self.workspace_id)
        platform = PlatformConnector(fyle_credentials=fyle_credentials)

        self.sync_expense_attributes(platform)

        self.sync_destination_attributes(self.destination_field)

        self.construct_payload_and_import_to_fyle(platform, import_log)
        
        self.sync_expense_attributes(platform)

        self.create_mappings()

        self.create_ccc_category_mappings()

        self.resolve_expense_attribute_errors()

    def create_mappings(self):
        """
        Create mappings
        """
        destination_attributes_without_duplicates = []
        destination_attributes = DestinationAttribute.objects.filter(
            workspace_id=self.workspace_id,
            attribute_type=self.destination_field,
            mapping__isnull=True
        ).order_by('value', 'id')
        destination_attributes_without_duplicates = self.remove_duplicate_attributes(destination_attributes)

        if destination_attributes_without_duplicates:
            Mapping.bulk_create_mappings(
                destination_attributes_without_duplicates,
                self.source_field,
                self.destination_field,
                self.workspace_id
            )

    def sync_expense_attributes(self, platform: PlatformConnector):
        """
        Sync expense attributes
        :param platform: PlatformConnector object
        """
        platform_class = self.get_platform_class(platform)
        if self.platform_class_name in ['expense_custom_fields', 'merchants']:
            platform_class.sync()
        else:
            platform_class.sync(sync_after=self.sync_after if self.sync_after else None)

    def sync_destination_attributes(self, sageintacct_attribute_type: str):
        """
        Sync destination attributes
        :param sageintacct_attribute_type: Sage Intacct attribute type
        """
        sage_intacct_credentials = SageIntacctCredential.objects.get(workspace_id=self.workspace_id)
        sage_intacct_connection = SageIntacctConnector(credentials_object=sage_intacct_credentials, workspace_id=self.workspace_id)

        sync_methods = {
            'LOCATION': sage_intacct_connection.sync_locations,
            'PROJECT': sage_intacct_connection.sync_projects,
            'DEPARTMENT': sage_intacct_connection.sync_departments,
            'VENDOR': sage_intacct_connection.sync_vendors,
            'CLASS': sage_intacct_connection.sync_classes,
            'TAX_DETAIL': sage_intacct_connection.sync_tax_details,
            'ITEM': sage_intacct_connection.sync_items,
            'CUSTOMER': sage_intacct_connection.sync_customers,
            'COST_TYPE': sage_intacct_connection.sync_cost_types,
            'EXPENSE_TYPE': sage_intacct_connection.sync_expense_types,
            'ACCOUNT': sage_intacct_connection.sync_accounts,
        }
        
        sync_method = sync_methods.get(sageintacct_attribute_type, sage_intacct_connection.sync_user_defined_dimensions)
        sync_method()

    def construct_payload_and_import_to_fyle(
        self,
        platform: PlatformConnector,
        import_log: ImportLog
    ):
        """
        Construct Payload and Import to fyle in Batches
        """
        is_auto_sync_status_allowed = self.get_auto_sync_permission()

        filters = self.construct_attributes_filter(self.destination_field)

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
            import_log.total_batches_count = math.ceil(destination_attributes_count/200)
            import_log.save()

        destination_attributes_generator = self.get_destination_attributes_generator(destination_attributes_count, filters)
        platform_class = self.get_platform_class(platform)

        for paginated_destination_attributes, is_last_batch in destination_attributes_generator:
            fyle_payload = self.setup_fyle_payload_creation(
                paginated_destination_attributes=paginated_destination_attributes,
                is_auto_sync_status_allowed=is_auto_sync_status_allowed
            )

            self.post_to_fyle_and_sync(
                fyle_payload=fyle_payload,
                resource_class=platform_class,
                is_last_batch=is_last_batch,
                import_log=import_log
            )

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
        paginated_destination_attributes: List[DestinationAttribute],
        is_auto_sync_status_allowed: bool
    ):
        """
        Setup Fyle Payload Creation
        :param paginated_destination_attributes: List of DestinationAttribute objects
        :param is_auto_sync_status_allowed: bool
        :return: Fyle Payload
        """
        paginated_destination_attribute_values = [attribute.value for attribute in paginated_destination_attributes]
        existing_expense_attributes_map = self.get_existing_fyle_attributes(paginated_destination_attribute_values)

        return self.construct_fyle_payload(paginated_destination_attributes, existing_expense_attributes_map, is_auto_sync_status_allowed)

    def get_existing_fyle_attributes(self, paginated_destination_attribute_values: List[str]):
        """
        Get Existing Fyle Attributes
        :param paginated_destination_attribute_values: List of DestinationAttribute values
        :return: Map of attribute value to attribute source_id
        """
        filters = self.construct_attributes_filter(self.source_field, paginated_destination_attribute_values)
        existing_expense_attributes_values = ExpenseAttribute.objects.filter(**filters).values('value', 'source_id')
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
        if fyle_payload and self.platform_class_name in ['expense_custom_fields', 'merchants']:
            resource_class.post(fyle_payload)
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
        time_difference = datetime.now() - timedelta(minutes=30)
        offset_aware_time_difference = time_difference.replace(tzinfo=timezone.utc)

        # If the import is already in progress or if the last successful run is within 30 minutes, don't start the import process
        if (import_log.status == 'IN_PROGRESS' and not is_created) \
            or (self.sync_after and (self.sync_after > offset_aware_time_difference)):
            return

        # Update the required values since we're beginning the import process
        else:
            import_log.status = 'IN_PROGRESS'
            import_log.processed_batches_count = 0
            import_log.total_batches_count = 0
            import_log.save()

            self.import_destination_attribute_to_fyle(import_log)
