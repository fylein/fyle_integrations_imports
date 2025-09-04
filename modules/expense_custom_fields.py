import logging
from datetime import datetime
from typing import List, Type, TypeVar

from django.utils.module_loading import import_string
from django.db.models import Count

from fyle_integrations_platform_connector import PlatformConnector
from fyle_accounting_mappings.models import DestinationAttribute, ExpenseAttribute, MappingSetting

from fyle_integrations_imports.modules.base import Base
from fyle_integrations_imports.models import ImportLog

from apps.workspaces.models import FyleCredential
from apps.mappings.constants import FYLE_EXPENSE_SYSTEM_FIELDS
from apps.mappings.exceptions import handle_import_exceptions_v2

logger = logging.getLogger(__name__)
logger.level = logging.INFO

T = TypeVar('T')


class ExpenseCustomField(Base):
    """
    Class for ExepenseCustomField module
    """
    def __init__(
        self,
        workspace_id: int,
        source_field: str,
        destination_field: str,
        sync_after: datetime,
        sdk_connection: Type[T],
        destination_sync_methods: List[str],
        import_without_destination_id: bool = False,
        prepend_code_to_name: bool = False,
        is_auto_sync_enabled: bool = True
    ):
        self.is_auto_sync_enabled = is_auto_sync_enabled
        super().__init__(
            workspace_id=workspace_id,
            source_field=source_field,
            destination_field=destination_field,
            platform_class_name='expense_custom_fields',
            sync_after=sync_after,
            sdk_connection=sdk_connection,
            destination_sync_methods=destination_sync_methods,
            import_without_destination_id=import_without_destination_id,
            prepend_code_to_name=prepend_code_to_name
        )

    def trigger_import(self):
        """
        Trigger import for ExepenseCustomField module
        """
        self.check_import_log_and_start_import()

    def construct_custom_field_placeholder(self, source_placeholder: str, fyle_attribute: str, existing_attribute: object):
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
        destination_attributes: List[DestinationAttribute],
        platform: PlatformConnector,
        source_placeholder: str = None
    ):
        """
        Construct payload for expense custom fields
        :param destination_attributes: List of destination attributes
        :param platform: PlatformConnector object
        :param source_placeholder: Placeholder from mapping settings
        """
        fyle_expense_custom_field_options = []
        fyle_attribute = self.source_field

        existing_fyle_attributes = ExpenseAttribute.objects.filter(attribute_type=fyle_attribute, workspace_id=self.workspace_id)

        destination_attribute_value_list = [attribute.value for attribute in destination_attributes]
        destination_attribute_inactive_list = [attribute.value for attribute in destination_attributes if not attribute.active]

        existing_fyle_values = [attribute.value for attribute in existing_fyle_attributes]
        existing_fyle_values_inactive = [attribute.value for attribute in existing_fyle_attributes if not attribute.active]

        combined_values = (
            set(destination_attribute_value_list) | set(existing_fyle_values)
        ) - set(destination_attribute_inactive_list) - set(existing_fyle_values_inactive)

        # Remove duplicates case-insensitively but keep original casing
        seen_lower = set()
        fyle_expense_custom_field_options = []
        for value in combined_values:
            value_lower = value.lower()
            if value_lower not in seen_lower:
                seen_lower.add(value_lower)
                fyle_expense_custom_field_options.append(value)

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

        posted_destination_attributes = self.construct_payload_and_import_to_fyle(
            platform=platform,
            import_log=import_log
        )

        self.sync_expense_attributes(platform)

        if posted_destination_attributes:
            self.create_mappings(posted_destination_attributes)


def disable_expense_custom_fields(workspace_id: int, attribute_type: str, attributes_to_disable: dict, *args, **kwargs) -> None:
    """
    Disable expense custom fields in Fyle when the expense custom fields are updated in Accounting.
    This is a callback function that is triggered from accounting_mappings.
    attributes_to_disable object format:
    {
        'destination_id': {
            'value': 'old_expense_custom_field_name',
            'updated_value': 'new_expense_custom_field_name',
            'code': 'old_expense_custom_field_code',
            'updated_code': 'new_expense_custom_field_code'
        }
    }
    """
    app_name = import_string('apps.workspaces.helpers.get_app_name')()
    configuration_model_path = None

    if app_name == 'Sage File Export':
        configuration_model_path = import_string('apps.workspaces.helpers.get_import_configuration_model_path')(workspace_id=workspace_id)
    else:
        configuration_model_path = import_string('apps.workspaces.helpers.get_import_configuration_model_path')()
    Configuration = import_string(configuration_model_path)

    source_field = MappingSetting.objects.filter(workspace_id=workspace_id, destination_field=attribute_type).values_list('source_field', flat=True).first()

    use_code_in_naming = False
    columns = Configuration._meta.get_fields()
    if 'import_code_fields' in [field.name for field in columns]:
        use_code_in_naming = Configuration.objects.filter(workspace_id=workspace_id, import_code_fields__contains=[attribute_type]).exists()

    custom_field_values = []

    for attribute in attributes_to_disable.values():
        if not use_code_in_naming and attribute['value'] == attribute['updated_value']:
            continue
        elif use_code_in_naming and (attribute['value'] == attribute['updated_value'] and attribute['code'] == attribute['updated_code']):
            continue

        custom_field_value = import_string('apps.mappings.helpers.prepend_code_to_name')(prepend_code_in_name=use_code_in_naming, value=attribute['value'], code=attribute['code'])
        custom_field_values.append(custom_field_value)

    if not use_code_in_naming:
        unique_values = DestinationAttribute.objects.filter(
            workspace_id=workspace_id,
            attribute_type=attribute_type,
            value__in=custom_field_values,
        ).values('value').annotate(
            value_count=Count('id')
        ).filter(value_count=1)

        custom_field_values = [item['value'] for item in unique_values]

    filters = {
        'workspace_id': workspace_id,
        'active': True,
        'attribute_type': source_field,
        'value__in': custom_field_values
    }

    count = ExpenseAttribute.objects.filter(**filters).count()

    if count > 0:
        logger.info(f"Disabling {source_field} in Expense Attribute | WORKSPACE_ID: {workspace_id} | COUNT: {count}")
        ExpenseAttribute.objects.filter(**filters).update(active=False, updated_at=datetime.now())
    else:
        logger.info(f"Skipping disabling {source_field} in Expense Attribute | WORKSPACE_ID: {workspace_id}")
