from django_q.tasks import Chain
from django.utils.module_loading import import_string

from fyle_integrations_imports.dataclasses import TaskSetting


def chain_import_fields_to_fyle(workspace_id, task_settings: TaskSetting):
    """
    Chain import fields to Fyle
    :param workspace_id: Workspace Id
    """
    app_name = import_string('apps.workspaces.helpers.get_app_name')()

    cluster = 'default' if app_name in ['SAGE300', 'QBD_CONNECTOR'] else 'import'

    chain = Chain()

    custom_properties_tasks = task_settings.get('custom_properties', None)

    if custom_properties_tasks is not None:
        chain.append(
            task_settings['custom_properties']['func'],
            **task_settings['custom_properties']['args'],
            q_options={
                'cluster': cluster
            }
        )

    if task_settings['import_categories']:
        chain.append(
            'fyle_integrations_imports.tasks.trigger_import_via_schedule',
            workspace_id=workspace_id,
            destination_field=task_settings['import_categories']['destination_field'],
            source_field='CATEGORY',
            sdk_connection_string=task_settings['sdk_connection_string'],
            credentials=task_settings['credentials'],
            destination_sync_methods=task_settings['import_categories']['destination_sync_methods'],
            is_auto_sync_enabled=task_settings['import_categories']['is_auto_sync_enabled'],
            is_3d_mapping=task_settings['import_categories']['is_3d_mapping'],
            charts_of_accounts=task_settings['import_categories']['charts_of_accounts'],
            is_custom=False,
            use_mapping_table=task_settings['import_categories']['use_mapping_table'] if 'use_mapping_table' in task_settings['import_categories'] else True,
            prepend_code_to_name=task_settings['import_categories']['prepend_code_to_name'] if 'prepend_code_to_name' in task_settings['import_categories'] else False,
            import_without_destination_id=task_settings['import_categories']['import_without_destination_id'] if 'import_without_destination_id' in task_settings['import_categories'] else False,
            q_options={
                'cluster': cluster
            }
        )

    if task_settings['import_tax']:
        chain.append(
            'fyle_integrations_imports.tasks.trigger_import_via_schedule',
            workspace_id=workspace_id,
            destination_field=task_settings['import_tax']['destination_field'],
            source_field='TAX_GROUP',
            sdk_connection_string=task_settings['sdk_connection_string'],
            credentials=task_settings['credentials'],
            destination_sync_methods=task_settings['import_tax']['destination_sync_methods'],
            is_auto_sync_enabled=task_settings['import_tax']['is_auto_sync_enabled'],
            is_3d_mapping=task_settings['import_tax']['is_3d_mapping'],
            charts_of_accounts=None,
            is_custom=False,
            q_options={
                'cluster': cluster
            }
        )

    if task_settings['import_vendors_as_merchants']:
        chain.append(
            'fyle_integrations_imports.tasks.trigger_import_via_schedule',
            workspace_id=workspace_id,
            destination_field=task_settings['import_vendors_as_merchants']['destination_field'],
            source_field='MERCHANT',
            sdk_connection_string=task_settings['sdk_connection_string'],
            credentials=task_settings['credentials'],
            destination_sync_methods=task_settings['import_vendors_as_merchants']['destination_sync_methods'],
            is_auto_sync_enabled=task_settings['import_vendors_as_merchants']['is_auto_sync_enabled'],
            is_3d_mapping=task_settings['import_vendors_as_merchants']['is_3d_mapping'],
            prepend_code_to_name=task_settings['import_vendors_as_merchants']['prepend_code_to_name'] if 'prepend_code_to_name' in task_settings['import_vendors_as_merchants'] else False,
            q_options={
                'cluster': cluster
            }
        )

    if task_settings['import_items'] is not None and task_settings['import_items']:
        chain.append(
            'fyle_integrations_imports.tasks.disable_items',
            workspace_id=workspace_id,
            is_import_enabled=task_settings['import_items'],
            q_options={
                'cluster': cluster
            }
        )

    if task_settings['mapping_settings']:
        for mapping_setting in task_settings['mapping_settings']:
            if mapping_setting['source_field'] in ['PROJECT', 'COST_CENTER'] or mapping_setting['is_custom']:
                chain.append(
                    'fyle_integrations_imports.tasks.trigger_import_via_schedule',
                    workspace_id=workspace_id,
                    destination_field=mapping_setting['destination_field'],
                    source_field=mapping_setting['source_field'],
                    sdk_connection_string=task_settings['sdk_connection_string'],
                    credentials=task_settings['credentials'],
                    destination_sync_methods=mapping_setting['destination_sync_methods'],
                    is_auto_sync_enabled=mapping_setting['is_auto_sync_enabled'],
                    is_3d_mapping=False,
                    charts_of_accounts=None,
                    is_custom=mapping_setting['is_custom'],
                    import_without_destination_id=mapping_setting['import_without_destination_id'] if 'import_without_destination_id' in mapping_setting else False,
                    prepend_code_to_name=mapping_setting['prepend_code_to_name'] if 'prepend_code_to_name' in mapping_setting else False,
                    q_options={
                        'cluster': cluster
                    }
                )

    if task_settings.get('import_dependent_fields'):
        chain.append(
            task_settings['import_dependent_fields']['func'],
            **task_settings['import_dependent_fields']['args'],
            q_options={
                'cluster': cluster
            }
        )

    if chain.length() > 0:
        chain.run()
