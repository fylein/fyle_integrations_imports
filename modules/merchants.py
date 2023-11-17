from datetime import datetime
from typing import List
from apps.mappings.imports.modules.base import Base
from fyle_accounting_mappings.models import DestinationAttribute
from apps.mappings.models import ImportLog
from apps.mappings.exceptions import handle_import_exceptions
from apps.workspaces.models import FyleCredential
from fyle_integrations_platform_connector import PlatformConnector


class Merchant(Base):
    """
    Class for Merchant module
    """
    def __init__(self, workspace_id: int, destination_field: str, sync_after: datetime):
        super().__init__(
            workspace_id=workspace_id,
            source_field='MERCHANT',
            destination_field=destination_field,
            platform_class_name='merchants',
            sync_after=sync_after
        )

    def trigger_import(self):
        """
        Trigger import for Merchant module
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
        Construct Fyle payload for Merchant module
        :param paginated_destination_attributes: List of paginated destination attributes
        :param existing_fyle_attributes_map: Existing Fyle attributes map
        :param is_auto_sync_status_allowed: Is auto sync status allowed
        :return: Fyle payload
        """
        payload = []

        for attribute in paginated_destination_attributes:
            # Create a new merchant if it does not exist in Fyle
            if attribute.value.lower() not in existing_fyle_attributes_map:
                payload.append(attribute.value)

        return payload

    # import_destination_attribute_to_fyle method is overridden
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
