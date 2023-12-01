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
    Class for Category module
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
            destination_sync_methods=destination_sync_methods
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
                account_filters = filters & (Q(detail__account_type__in=self.charts_of_accounts, display_name='Account'))

            if 'items' in self.destination_sync_methods:
                item_filter = filters & Q(display_name='Item')
                filters  = account_filters | item_filter if 'accounts' in self.destination_sync_methods else item_filter

            if 'items' not in self.destination_sync_methods:
                filters = account_filters

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

        return payload

    # create_mappings method is overridden
    def create_mappings(self):
        """
        Create mappings for Category module
        """
        destination_attributes_without_duplicates = []
        filters = self.construct_attributes_filter(self.destination_field)

        destination_attributes = DestinationAttribute.objects.filter(
            filters, mapping__isnull=True
        ).order_by('value', 'id')
        destination_attributes_without_duplicates = self.remove_duplicate_attributes(destination_attributes)

        if destination_attributes_without_duplicates:
            Mapping.bulk_create_mappings(
                destination_attributes_without_duplicates,
                self.source_field,
                self.destination_field,
                self.workspace_id
            )
