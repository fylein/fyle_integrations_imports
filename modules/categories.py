import math
from datetime import datetime
from django.db.models import Q
from typing import List, Type, TypeVar
from fyle_integrations_imports.modules.base import Base
from fyle_integrations_imports.models import ImportLog
from fyle_integrations_platform_connector import PlatformConnector
from fyle_accounting_mappings.models import DestinationAttribute, Mapping

T = TypeVar('T')

class Category(Base):
    """
    Class for Category modulecde234
    """
    def __init__(self, workspace_id: int, destination_field: str, sync_after: datetime,  sdk_connection: Type[T], destination_sync_methods: List[str], is_auto_sync_enabled: bool, is_3d_mapping:bool, charts_of_accounts: List[str]):
        self.is_auto_sync_enabled = is_auto_sync_enabled
        self.is_3d_mapping = is_3d_mapping
        self.charts_of_accounts = charts_of_accounts
        super().__init__(
            workspace_id=workspace_id,
            source_field='CATEGORY',
            destination_field=destination_field,
            platform_class_name='categories',
            sync_after=sync_after,
            sdk_connection=sdk_connection,
            destination_sync_methods=destination_sync_methods # ['accounts']
        )

    def trigger_import(self):
        """
        Trigger import for Category module
        """
        self.check_import_log_and_start_import()

    def construct_attributes_filter(self, attribute_type: str, paginated_destination_attribute_values: List[str] = []):
        """
        Construct the attributes filter
        :param attribute_type: attribute type
        :param paginated_destination_attribute_values: paginated destination attribute values
        :return: dict
        """

        filters = Q(attribute_type=attribute_type, workspace_id=self.workspace_id)

        if self.sync_after and self.platform_class_name != 'expense_custom_fields':
            filters &= Q(updated_at__gte=self.sync_after)
        
        if paginated_destination_attribute_values:
            filters &= Q(value__in=paginated_destination_attribute_values)

        if attribute_type != 'CATEGORY':
            if 'accounts' in self.destination_sync_methods:
                filters = filters & (Q(detail__account_type__in=self.charts_of_accounts, display_name='Account'))
            if 'items' in self.destination_sync_methods:
                item_filter = filters & Q(display_name='Item')
                filters  = filters | item_filter if 'accounts' in self.destination_sync_methods else item_filter

        return filters

    def construct_attributes_filter(self, attribute_type: str, paginated_destination_attribute_values: List[str] = []):
        """
        Construct the attributes filter
        :param attribute_type: attribute type
        :param paginated_destination_attribute_values: paginated destination attribute values
        :return: dict
        """

        print("""

            construct   attributes   filter

        """)
        filters = {
            'attribute_type': attribute_type,
            'workspace_id': self.workspace_id
        }

        if self.sync_after and self.platform_class_name != 'expense_custom_fields':
            filters['updated_at__gte'] = self.sync_after

        if paginated_destination_attribute_values:
            filters['value__in'] = paginated_destination_attribute_values


        if attribute_type != 'CATEGORY':
            # if 'accounts' in self.destination_sync_methods :
            #     filters['display_name'] = 'Account'
            #     if self.charts_of_accounts:
            #         filters['detail__account_type__in'] = self.charts_of_accounts

            # elif 'items' in self.destination_sync_methods:
            #     filters['display_name'] = 'Item'
            filters = None
            if 'accounts' in self.destination_sync_methods:
                filters = Q(workspace_id=self.workspace_id, attribute_type='ACCOUNT', detail__account_type__in=self.charts_of_accounts, display_name='Account')
            if 'items' in self.destination_sync_methods:
                item_filter = Q(workspace_id=self.workspace_id, display_name='Item', attribute_type='ACCOUNT')
                filters  = filters | item_filter if filters else item_filter

        print(filters)
        return filters

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

        print("""

            construct   fyle   payload
        """)

        print(paginated_destination_attributes)

        print(existing_fyle_attributes_map)

        for attribute in paginated_destination_attributes:
            category = {
                'name': attribute.value,
                'code': attribute.destination_id,
                'is_enabled': attribute.active if attribute.value != 'Unspecified' else True
            }

            # Create a new category if it does not exist in Fyle
            if attribute.value.lower() not in existing_fyle_attributes_map:
                payload.append(category)
            # Disable the existing category in Fyle if auto-sync status is allowed and the destination_attributes is inactive
            elif  self.is_auto_sync_enabled and not attribute.active:
                category['id'] = existing_fyle_attributes_map[attribute.value.lower()]
                payload.append(category)

        print(payload)

        return payload

    # create_mappings method is overridden
    def create_mappings(self):
        """
        Create mappings for Category module
        """
        destination_attributes_without_duplicates = []
        # filters = {
        #     'workspace_id': self.workspace_id,
        #     'attribute_type': self.destination_field,
        #     'mapping__isnull': True,
        # }

        # if self.charts_of_accounts and 'accounts' in self.destination_sync_methods :
        #     filters['detail__account_type__in'] = self.charts_of_accounts

        # if 'accounts' in self.destination_sync_methods:
        #     filters['display_name'] = 'Account'

        # if 'items' in self.destination_sync_methods:
        #     filters['display_name'] = 'Item'
        print("""

            create mappings

            """)
        filters = self.construct_attributes_filter(self.destination_field)

        destination_attributes = DestinationAttribute.objects.filter(
            filters, mapping__isnull=True
        ).order_by('value', 'id')
        destination_attributes_without_duplicates = self.remove_duplicate_attributes(destination_attributes)

        print(len(destination_attributes_without_duplicates))

        if destination_attributes_without_duplicates:
            Mapping.bulk_create_mappings(
                destination_attributes_without_duplicates,
                self.source_field,
                self.destination_field,
                self.workspace_id
            )



    # def construct_payload_and_import_to_fyle(
    #     self,
    #     platform: PlatformConnector,
    #     import_log: ImportLog 
    # ):
    #     """
    #     Construct payload and import to Fyle
    #     """
    #     print("""
            
    #         construct_payload_and_import_to_fyle
            
    #     """)
    #     # filters = self.construct_attributes_filter(self.destination_field)

    #     if 'accounts' in self.destination_sync_methods:
    #         filters = Q(workspace_id=5, attribute_type='ACCOUNT', detail__account_type__in=self.charts_of_accounts, display_name='Account')
    #     if 'items' in self.destination_sync_methods:
    #         item_filter = Q(workspace_id=5, display_name='Item', attribute_type='ACCOUNT')
    #         filters  = filters | item_filter if filters else item_filter

    #     destination_attributes_count = DestinationAttribute.objects.filter(**filters).count()

    #     print("destination_attributes_count")
    #     print(destination_attributes_count)

    #     # If there are no destination attributes, mark the import as complete
    #     if destination_attributes_count == 0:
    #         import_log.status = 'COMPLETE'
    #         import_log.last_successful_run_at = datetime.now()
    #         import_log.error_log = []
    #         import_log.total_batches_count = 0
    #         import_log.processed_batches_count = 0
    #         import_log.save()
    #         return
    #     else:
    #         import_log.total_batches_count = math.ceil(destination_attributes_count/200)
    #         import_log.save()

    #     destination_attributes_generator = self.get_destination_attributes_generator(destination_attributes_count, filters)
    #     platform_class = self.get_platform_class(platform)
    #     count= 0
    #     for paginated_destination_attributes, is_last_batch in destination_attributes_generator:
    #         print("paginated_destination_attributes")
    #         print(paginated_destination_attributes) 
    #         print("count")
    #         print(count)
    #         count = count + 1
    #         fyle_payload = self.setup_fyle_payload_creation(
    #             paginated_destination_attributes=paginated_destination_attributes
    #         )

    #         self.post_to_fyle_and_sync(
    #             fyle_payload=fyle_payload,
    #             resource_class=platform_class,
    #             is_last_batch=is_last_batch,
    #             import_log=import_log
    #         )

    # def get_destination_attributes_generator(self, destination_attributes_count: int, filters: dict):
    #     """
    #     Get destination attributes generator
    #     :param destination_attributes_count: Destination attributes count
    #     :param filters: dict
    #     :return: Generator of destination_attributes
    #     """
    #     for offset in range(0, destination_attributes_count, 200):
    #         limit = offset + 200
    #         paginated_destination_attributes = DestinationAttribute.objects.filter(**filters).order_by('value', 'id')[offset:limit]
    #         paginated_destination_attributes_without_duplicates = self.remove_duplicate_attributes(paginated_destination_attributes)
    #         is_last_batch = True if limit >= destination_attributes_count else False

    #         yield paginated_destination_attributes_without_duplicates, is_last_batch
    