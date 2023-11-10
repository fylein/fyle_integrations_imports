from datetime import datetime
from typing import List
from apps.mappings.imports.modules.base import Base
from fyle_accounting_mappings.models import DestinationAttribute


class TaxGroup(Base):
    """
    Class for TaxGroup module
    """
    def __init__(self, workspace_id: int, destination_field: str, sync_after: datetime):
        super().__init__(
            workspace_id=workspace_id,
            source_field='TAX_GROUP',
            destination_field=destination_field,
            platform_class_name='tax_groups',
            sync_after=sync_after
        )

    def trigger_import(self):
        """
        Trigger import for TaxGroups module
        """
        self.check_import_log_and_start_import()

    # remove the is_auto_sync_status_allowed parameter
    def construct_fyle_payload(
        self,
        paginated_destination_attributes: List[DestinationAttribute],
        existing_fyle_attributes_map: object,
        is_auto_sync_status_allowed: bool
    ):
        """
        Construct Fyle payload for TaxGroup module
        :param paginated_destination_attributes: List of paginated destination attributes
        :param existing_fyle_attributes_map: Existing Fyle attributes map
        :param is_auto_sync_status_allowed: Is auto sync status allowed
        :return: Fyle payload
        """
        payload = []

        for attribute in paginated_destination_attributes:
            tax_group = {
                'name': attribute.value,
                'is_enabled': True,
                'percentage': round((attribute.detail['tax_rate']/100), 2)
            }

            # Create a new tax-group if it does not exist in Fyle
            if attribute.value.lower() not in existing_fyle_attributes_map:
                payload.append(tax_group)

        return payload
