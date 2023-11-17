from datetime import datetime
from typing import List, Dict
from apps.mappings.imports.modules.base import Base
from fyle_accounting_mappings.models import (
    DestinationAttribute,
    ExpenseAttribute
)
from apps.mappings.exceptions import handle_import_exceptions
from apps.mappings.models import ImportLog
from fyle_integrations_platform_connector import PlatformConnector
from apps.workspaces.models import FyleCredential
from apps.mappings.constants import FYLE_EXPENSE_SYSTEM_FIELDS


class ExpenseCustomField(Base):
    """
    Class for ExepenseCustomField module
    """
    def __init__(self, workspace_id: int, source_field: str, destination_field: str, sync_after: datetime):
        super().__init__(
            workspace_id=workspace_id,
            source_field=source_field,
            destination_field=destination_field,
            platform_class_name='expense_custom_fields',
            sync_after=sync_after
        )

    def trigger_import(self):
        """
        Trigger import for ExepenseCustomField module
        """
        self.check_import_log_and_start_import()

    def construct_custom_field_placeholder(self, source_placeholder: str, fyle_attribute: str, existing_attribute: Dict):
        """
        Construct placeholder for custom field
        :param source_placeholder: Placeholder from mapping settings
        :param fyle_attribute: Fyle attribute
        :param existing_attribute: Existing attribute
        """
        new_placeholder = None
        placeholder = None

        if existing_attribute:
            placeholder = existing_attribute['placeholder'] if 'placeholder' in existing_attribute else None

        # Here is the explanation of what's happening in the if-else ladder below
        # source_field is the field that's save in mapping settings, this field user may or may not fill in the custom field form
        # placeholder is the field that's saved in the detail column of destination attributes
        # fyle_attribute is what we're constructing when both of these fields would not be available

        if not (source_placeholder or placeholder):
            # If source_placeholder and placeholder are both None, then we're creating adding a self constructed placeholder
            new_placeholder = 'Select {0}'.format(fyle_attribute)
        elif not source_placeholder and placeholder:
            # If source_placeholder is None but placeholder is not, then we're choosing same place holder as 1 in detail section
            new_placeholder = placeholder
        elif source_placeholder and not placeholder:
            # If source_placeholder is not None but placeholder is None, then we're choosing the placeholder as filled by user in form
            new_placeholder = source_placeholder
        else:
            # Else, we're choosing the placeholder as filled by user in form or None
            new_placeholder = source_placeholder

        return new_placeholder

    def construct_fyle_expense_custom_field_payload(
        self,
        sageintacct_attributes: List[DestinationAttribute],
        platform: PlatformConnector,
        source_placeholder: str = None
    ):
        """
        Construct payload for expense custom fields
        :param sageintacct_attributes: List of destination attributes
        :param platform: PlatformConnector object
        :param source_placeholder: Placeholder from mapping settings
        """
        fyle_expense_custom_field_options = []
        fyle_attribute = self.source_field

        [fyle_expense_custom_field_options.append(sageintacct_attribute.value) for sageintacct_attribute in sageintacct_attributes]

        if fyle_attribute.lower() not in FYLE_EXPENSE_SYSTEM_FIELDS:
            existing_attribute = ExpenseAttribute.objects.filter(
                attribute_type=fyle_attribute, workspace_id=self.workspace_id).values_list('detail', flat=True).first()

            custom_field_id = None

            if existing_attribute is not None:
                custom_field_id = existing_attribute['custom_field_id']

            fyle_attribute = fyle_attribute.replace('_', ' ').title()
            placeholder = self.construct_custom_field_placeholder(source_placeholder, fyle_attribute, existing_attribute)

            expense_custom_field_payload = {
                'field_name': fyle_attribute,
                'type': 'SELECT',
                'is_enabled': True,
                'is_mandatory': False,
                'placeholder': placeholder,
                'options': fyle_expense_custom_field_options,
                'code': None
            }

            if custom_field_id:
                expense_field = platform.expense_custom_fields.get_by_id(custom_field_id)
                expense_custom_field_payload['id'] = custom_field_id
                expense_custom_field_payload['is_mandatory'] = expense_field['is_mandatory']

        return expense_custom_field_payload

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
            import_log.total_batches_count = 1
            import_log.save()

        destination_attributes = DestinationAttribute.objects.filter(**filters)
        destination_attributes_without_duplicates = self.remove_duplicate_attributes(destination_attributes)
        platform_class = self.get_platform_class(platform)

        fyle_payload = self.construct_fyle_expense_custom_field_payload(
            destination_attributes_without_duplicates,
            platform,
            source_placeholder
        )

        self.post_to_fyle_and_sync(
            fyle_payload=fyle_payload,
            resource_class=platform_class,
            is_last_batch=True,
            import_log=import_log
        )

    # import_destination_attribute_to_fyle method is overridden
    @handle_import_exceptions
    def import_destination_attribute_to_fyle(self, import_log: ImportLog):
        """
        Import destiantion_attributes field to Fyle and Auto Create Mappings
        :param import_log: ImportLog object
        """

        fyle_credentials = FyleCredential.objects.get(workspace_id=self.workspace_id)
        platform = PlatformConnector(fyle_credentials=fyle_credentials)

        self.sync_destination_attributes(self.destination_field)

        self.construct_payload_and_import_to_fyle(
            platform=platform,
            import_log=import_log
        )

        self.sync_expense_attributes(platform)

        self.create_mappings()
