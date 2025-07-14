import logging
from datetime import datetime
from typing import Dict, List, Type, TypeVar

from django.utils.module_loading import import_string
from django.db.models import Count
from django.db.models.functions import Lower

from fyle_integrations_platform_connector import PlatformConnector
from fyle_accounting_mappings.models import (
    MappingSetting,
    ExpenseAttribute,
    DestinationAttribute
)

from apps.workspaces.models import FyleCredential
from fyle_integrations_imports.modules.base import Base

T = TypeVar('T')

logger = logging.getLogger(__name__)
logger.level = logging.INFO


class CostCenter(Base):
    """
    Class for CostCenter module
    """
    def __init__(
        self,
        workspace_id: int,
        destination_field: str,
        sync_after: datetime,
        sdk_connection: Type[T],
        destination_sync_methods: List[str],
        prepend_code_to_name: bool = False,
        is_auto_sync_enabled: bool = True,
        import_without_destination_id: bool = False
    ):
        self.is_auto_sync_enabled = is_auto_sync_enabled
        super().__init__(
            workspace_id=workspace_id,
            source_field='COST_CENTER',
            destination_field=destination_field,
            platform_class_name='cost_centers',
            sync_after=sync_after,
            sdk_connection=sdk_connection,
            destination_sync_methods=destination_sync_methods,
            prepend_code_to_name=prepend_code_to_name,
            import_without_destination_id=import_without_destination_id
        )

    def trigger_import(self):
        """
        Trigger import for CostCenter module
        """
        self.check_import_log_and_start_import()

    # remove the is_auto_sync_status_allowed parameter
    def construct_fyle_payload(
        self,
        paginated_destination_attributes: List[DestinationAttribute],
        existing_fyle_attributes_map: object
    ):
        """
        Construct Fyle payload for CostCenter module
        :param paginated_destination_attributes: List of paginated destination attributes
        :param existing_fyle_attributes_map: Existing Fyle attributes map
        :param is_auto_sync_status_allowed: Is auto sync status allowed
        :return: Fyle payload
        """
        payload = []
        case_insensitive_map = {}


        # This is to handle the case where the value in destination attribute and Fyle doesn't have same case
        unmatched_values = [
            attribute.value for attribute in paginated_destination_attributes
            if attribute.value.lower() not in existing_fyle_attributes_map
        ]

        if unmatched_values:
            lower_unmatched_values = [value.lower() for value in unmatched_values]

            expense_attributes_lower_value_map = (
                ExpenseAttribute.objects
                .filter(workspace_id=self.workspace_id, attribute_type='COST_CENTER')
                .annotate(lower_value=Lower('value'))
                .filter(lower_value__in=lower_unmatched_values)
                .values('value', 'source_id', 'lower_value')
            )

            case_insensitive_map = {
                ea['lower_value']: (ea['value'], ea['source_id']) for ea in expense_attributes_lower_value_map
            }

        for attribute in paginated_destination_attributes:
            cost_center = {
                'name': attribute.value,
                'code': attribute.destination_id if not self.import_without_destination_id else None,
                'is_enabled': True if attribute.active is None else attribute.active,
                'description': 'Cost Center - {0}, Id - {1}'.format(
                    attribute.value,
                    attribute.destination_id
                )
            }

            lower_value = attribute.value.lower()
            existing_source_id = existing_fyle_attributes_map.get(lower_value)
            if existing_source_id:
                cost_center['name'] = case_insensitive_map.get(lower_value, (attribute.value,))[0]
                cost_center['description'] = 'Cost Center - {0}, Id - {1}'.format(
                    cost_center['name'],
                    existing_source_id
                )
            else:
                value_map = case_insensitive_map.get(lower_value)
                if value_map:
                    cost_center['name'], existing_source_id = value_map
                    cost_center['description'] = 'Cost Center - {0}, Id - {1}'.format(
                        cost_center['name'],
                        existing_source_id
                    )

            # Create a new cost-center if it does not exist in Fyle
            if not existing_source_id and attribute.active:
                payload.append(cost_center)

            # Disable the cost-center if it is not active and exists in Fyle
            elif self.is_auto_sync_enabled and not attribute.active and existing_source_id:
                cost_center['id'] = existing_source_id
                payload.append(cost_center)

        return payload


def disable_cost_centers(workspace_id: int, attributes_to_disable: Dict, is_import_to_fyle_enabled: bool = False, attribute_type: str = None, *args, **kwargs):
    """
    cost_centers_to_disable object format:
    {
        'destination_id': {
            'value': 'old_cost_center_name',
            'updated_value': 'new_cost_center_name',
            'code': 'old_code',
            'updated_code': 'new_code' ---- if the code is updated else same as code
        }
    }
    """
    if not is_import_to_fyle_enabled or len(attributes_to_disable) == 0:
        logger.info("Skipping disabling cost centers in Fyle | WORKSPACE_ID: %s", workspace_id)
        return
    
    app_name = import_string('apps.workspaces.helpers.get_app_name')()

    destination_type = MappingSetting.objects.get(workspace_id=workspace_id, source_field='COST_CENTER').destination_field

    configuration_model_path = import_string('apps.workspaces.helpers.get_import_configuration_model_path')()
    Configuration = import_string(configuration_model_path)

    use_code_in_naming = False
    columns = Configuration._meta.get_fields()
    if 'import_code_fields' in [field.name for field in columns]:
        use_code_in_naming = Configuration.objects.filter(workspace_id=workspace_id, import_code_fields__contains=[destination_type]).exists()

    fyle_credentials = FyleCredential.objects.get(workspace_id=workspace_id)
    platform = PlatformConnector(fyle_credentials=fyle_credentials)

    cost_center_values = []
    for cost_center_map in attributes_to_disable.values():
        if not use_code_in_naming and cost_center_map['value'] == cost_center_map['updated_value']:
            continue
        elif use_code_in_naming and (cost_center_map['value'] == cost_center_map['updated_value'] and cost_center_map['code'] == cost_center_map['updated_code']):
            continue

        cost_center_name = import_string('apps.mappings.helpers.prepend_code_to_name')(prepend_code_in_name=use_code_in_naming, value=cost_center_map['value'], code=cost_center_map['code'])
        cost_center_values.append(cost_center_name)

    if not use_code_in_naming:
        unique_values = DestinationAttribute.objects.filter(
            workspace_id=workspace_id,
            attribute_type=attribute_type,
            value__in=cost_center_values,
        ).values('value').annotate(
            value_count=Count('id')
        ).filter(value_count=1)

        cost_center_values = [item['value'] for item in unique_values]

    filters = {
        'workspace_id': workspace_id,
        'attribute_type': 'COST_CENTER',
        'value__in': cost_center_values,
        'active': True
    }

    expense_attribute_value_map = {}
    for destination_id, v in attributes_to_disable.items():
        cost_center_name = import_string('apps.mappings.helpers.prepend_code_to_name')(prepend_code_in_name=use_code_in_naming, value=v['value'], code=v['code'])
        expense_attribute_value_map[cost_center_name] = destination_id

    expense_attributes = ExpenseAttribute.objects.filter(**filters)

    bulk_payload = []
    for expense_attribute in expense_attributes:
        code = expense_attribute_value_map.get(expense_attribute.value, None)
        if code:
            payload = {
                'name': expense_attribute.value,
                'code': code if not app_name in ['QBD_CONNECTOR', 'SAGE300'] else None,
                'is_enabled': False,
                'id': expense_attribute.source_id,
                'description': 'Cost Center - {0}, Id - {1}'.format(
                    expense_attribute.value,
                    code
                )
            }
            bulk_payload.append(payload)
        else:
            logger.error(f"Cost Center with value {expense_attribute.value} not found | WORKSPACE_ID: {workspace_id}")

    if bulk_payload:
        logger.info(f"Disabling Cost Center in Fyle | WORKSPACE_ID: {workspace_id} | COUNT: {len(bulk_payload)}")
        platform.cost_centers.post_bulk(bulk_payload)
    else:
        logger.info(f"No Cost Center to Disable in Fyle | WORKSPACE_ID: {workspace_id}")

    return bulk_payload
