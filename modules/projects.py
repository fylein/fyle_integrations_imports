import logging
from datetime import datetime
from typing import Dict, List, Type, TypeVar

from django.utils.module_loading import import_string
from django.db.models import Count

from fyle_integrations_platform_connector import PlatformConnector
from fyle_accounting_mappings.models import ExpenseAttribute, DestinationAttribute, MappingSetting

from apps.workspaces.models import FyleCredential
from fyle_integrations_imports.modules.base import Base


T = TypeVar('T')

logger = logging.getLogger(__name__)
logger.level = logging.INFO


class Project(Base):
    """
    Class for Projects module
    """
    def __init__(
        self,
        workspace_id: int,
        destination_field: str,
        sync_after: datetime,
        sdk_connection: Type[T],
        destination_sync_methods: List[str],
        is_auto_sync_enabled: bool,
        import_without_destination_id: bool = False,
        prepend_code_to_name: bool = False
    ):
        self.is_auto_sync_enabled = is_auto_sync_enabled
        super().__init__(
            workspace_id=workspace_id,
            source_field='PROJECT',
            destination_field=destination_field,
            platform_class_name='projects',
            sync_after=sync_after,
            sdk_connection=sdk_connection,
            destination_sync_methods=destination_sync_methods,
            import_without_destination_id=import_without_destination_id,
            prepend_code_to_name=prepend_code_to_name
        )

    def trigger_import(self):
        """
        Trigger import for Projects module
        """
        self.check_import_log_and_start_import()

    def construct_fyle_payload(
        self,
        paginated_destination_attributes: List[DestinationAttribute],
        existing_fyle_attributes_map: object
    ):
        """
        Construct Fyle payload for Projects module
        :param paginated_destination_attributes: List of paginated destination attributes
        :param existing_fyle_attributes_map: Existing Fyle attributes map
        :param is_auto_sync_status_allowed: Is auto sync status allowed
        :return: Fyle payload
        """
        payload = []

        for attribute in paginated_destination_attributes:
            project = {
                'name': attribute.value,
                'code': attribute.destination_id if not self.import_without_destination_id else None,
                'description': 'Project - {0}, Id - {1}'.format(
                    attribute.value,
                    attribute.destination_id
                ),
                'is_enabled': True if attribute.active is None else attribute.active
            }

            # Create a new project if it does not exist in Fyle and the destination_attributes is active
            if attribute.value.lower() not in existing_fyle_attributes_map and attribute.active:
                payload.append(project)
            # Disable the existing project in Fyle if auto-sync status is allowed and the destination_attributes is inactive
            elif self.is_auto_sync_enabled and not attribute.active and attribute.value.lower() in existing_fyle_attributes_map:
                project['id'] = existing_fyle_attributes_map[attribute.value.lower()]
                payload.append(project)

        return payload


def disable_projects(workspace_id: int, projects_to_disable: Dict, is_import_to_fyle_enabled: bool = False, attribute_type: str = None, *args, **kwargs):
    """
    Disable projects in Fyle when the projects are updated in Accounting.
    This is a callback function that is triggered from accounting_mappings.
    projects_to_disable object format:
    {
        'destination_id': {
            'value': 'old_project_name',
            'updated_value': 'new_project_name',
            'code': 'old_project_code',
            'updated_code': 'new_project_code'
        }
    }

    """
    if not is_import_to_fyle_enabled or len(projects_to_disable) == 0:
        logger.info("Skipping disabling projects in Fyle | WORKSPACE_ID: %s", workspace_id)
        return

    app_name = import_string('apps.workspaces.helpers.get_app_name')()

    fyle_credentials = FyleCredential.objects.get(workspace_id=workspace_id)
    platform = PlatformConnector(fyle_credentials=fyle_credentials)
    platform.projects.sync()

    configuration_model_path = import_string('apps.workspaces.helpers.get_import_configuration_model_path')()
    Configuration = import_string(configuration_model_path)

    destination_type_list = []

    project_mapping_settings = MappingSetting.objects.filter(workspace_id=workspace_id, source_field='PROJECT').first()
    if project_mapping_settings:
        destination_type_list.append(project_mapping_settings.destination_field)

    use_code_in_naming = False
    columns = Configuration._meta.get_fields()
    if 'import_code_fields' in [field.name for field in columns] and destination_type_list:
        use_code_in_naming = Configuration.objects.filter(
            workspace_id = workspace_id,
            import_code_fields__contains=destination_type_list
        ).exists()

    project_values = []
    for projects_map in projects_to_disable.values():
        if not use_code_in_naming and projects_map['value'] == projects_map['updated_value']:
            continue
        elif use_code_in_naming and (projects_map['value'] == projects_map['updated_value'] and projects_map['code'] == projects_map['updated_code']):
            continue

        project_name = import_string('apps.mappings.helpers.prepend_code_to_name')(prepend_code_in_name=use_code_in_naming, value=projects_map['value'], code=projects_map['code'])
        project_values.append(project_name)

    if not use_code_in_naming:
        unique_values = DestinationAttribute.objects.filter(
            workspace_id=workspace_id,
            attribute_type=attribute_type,
            value__in=project_values,
        ).values('value').annotate(
            value_count=Count('id')
        ).filter(value_count=1)

        project_values = [item['value'] for item in unique_values]

    filters = {
        'workspace_id': workspace_id,
        'attribute_type': 'PROJECT',
        'value__in': project_values,
        'active': True
    }

    # Expense attribute value map is as follows: {old_project_name: destination_id}
    expense_attribute_value_map = {}
    for destination_id, v in projects_to_disable.items():
        project_name = import_string('apps.mappings.helpers.prepend_code_to_name')(prepend_code_in_name=use_code_in_naming, value=v['value'], code=v['code'])
        expense_attribute_value_map[project_name] = destination_id

    expense_attributes = ExpenseAttribute.objects.filter(**filters)

    bulk_payload = []
    for expense_attribute in expense_attributes:
        code = expense_attribute_value_map.get(expense_attribute.value, None)
        if code:
            payload = {
                'name': expense_attribute.value,
                'code': code if not app_name in ['QBD_CONNECTOR', 'SAGE300'] else None,
                'description': 'Project - {0}, Id - {1}'.format(
                    expense_attribute.value,
                    code
                ),
                'is_enabled': False,
                'id': expense_attribute.source_id
            }
            bulk_payload.append(payload)
        else:
            logger.error(f"Project with value {expense_attribute.value} not found | WORKSPACE_ID: {workspace_id}")

    if bulk_payload:
        logger.info(f"Disabling Projects in Fyle | WORKSPACE_ID: {workspace_id} | COUNT: {len(bulk_payload)}")
        platform.projects.post_bulk(bulk_payload)

        if app_name in ['SAGE300', 'INTACCT']:
            update_and_disable_cost_code_path = import_string('apps.workspaces.helpers.get_cost_code_update_method_path')()
            import_string(update_and_disable_cost_code_path)(workspace_id, projects_to_disable, platform, use_code_in_naming)
        platform.projects.sync()
    else:
        logger.info(f"No Projects to Disable in Fyle | WORKSPACE_ID: {workspace_id}")

    return bulk_payload
