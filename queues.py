from django_q.tasks import Chain
from fyle_integrations_imports.dataclasses import TaskSetting


def chain_import_fields_to_fyle(workspace_id, task_settings: TaskSetting):
    """
    Chain import fields to Fyle
    :param workspace_id: Workspace Id
    """
    chain = Chain()

    if task_settings['custom_properties'] is not None:
        chain.append(
            task_settings['custom_properties']['func'],
            **task_settings['custom_properties']['args']
        )

    if task_settings['import_categories']:
        chain.append(
            'fyle_integrations_imports.tasks.trigger_import_via_schedule',
            workspace_id,
            task_settings['import_categories']['destination_field'],
            'CATEGORY',
            task_settings['sdk_connection_string'],
            task_settings['credentials'],
            task_settings['import_categories']['destination_sync_methods'],
            task_settings['import_categories']['is_auto_sync_enabled'],
            task_settings['import_categories']['is_3d_mapping'],
            task_settings['import_categories']['charts_of_accounts'],
            False
        )

    if task_settings['import_tax']:
        chain.append(
            'fyle_integrations_imports.tasks.trigger_import_via_schedule',
            workspace_id,
            task_settings['import_tax']['destination_field'],
            'TAX_GROUP',
            task_settings['sdk_connection_string'],
            task_settings['credentials'],
            task_settings['import_tax']['destination_sync_methods'],
            task_settings['import_tax']['is_auto_sync_enabled'],
            task_settings['import_tax']['is_3d_mapping'],
            None,
            False
        )

    if task_settings['import_vendors_as_merchants']:
        chain.append(
            'fyle_integrations_imports.tasks.trigger_import_via_schedule',
            workspace_id,
            task_settings['import_vendors_as_merchants']['destination_field'],
            'MERCHANT',
            task_settings['sdk_connection_string'],
            task_settings['credentials'],
            task_settings['import_vendors_as_merchants']['destination_sync_methods'],
            task_settings['import_vendors_as_merchants']['is_auto_sync_enabled'],
            task_settings['import_vendors_as_merchants']['is_3d_mapping'],
            None,
            False
        )

    if task_settings['import_items'] is not None and not task_settings['import_items']:
        chain.append(
            'fyle_integrations_imports.tasks.disable_category_for_items_mapping',
            workspace_id,
            task_settings['sdk_connection_string'],
            task_settings['credentials'],
        )

    if task_settings['mapping_settings']:
        for mapping_setting in task_settings['mapping_settings']:
            if mapping_setting['source_field'] in ['PROJECT', 'COST_CENTER'] or mapping_setting['is_custom']:
                chain.append(
                    'fyle_integrations_imports.tasks.trigger_import_via_schedule',
                    workspace_id,
                    mapping_setting['destination_field'],
                    mapping_setting['source_field'],
                    task_settings['sdk_connection_string'],
                    task_settings['credentials'],
                    mapping_setting['destination_sync_methods'],
                    mapping_setting['is_auto_sync_enabled'],
                    False,
                    None,
                    mapping_setting['is_custom']
                )

    if chain.length() > 0:
        chain.run()
