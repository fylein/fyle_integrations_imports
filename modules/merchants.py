from datetime import datetime
from typing import List, Type, TypeVar
from fyle_integrations_imports.modules.base import Base
from fyle_accounting_mappings.models import DestinationAttribute
from fyle_integrations_imports.models import ImportLog
from apps.mappings.exceptions import handle_import_exceptions_v2
from apps.workspaces.models import FyleCredential
from fyle_integrations_platform_connector import PlatformConnector

T = TypeVar('T')


class Merchant(Base):
    """
    Class for Merchant module
    """
    def __init__(self, workspace_id: int, destination_field: str, sync_after: datetime,  sdk_connection: Type[T], destination_sync_methods: List[str]):
        super().__init__(
            workspace_id=workspace_id,
            source_field='MERCHANT',
            destination_field=destination_field,
            platform_class_name='merchants',
            sync_after=sync_after,
            sdk_connection=sdk_connection,
            destination_sync_methods=destination_sync_methods
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
        existing_fyle_attributes_map: object
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
