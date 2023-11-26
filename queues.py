from django_q.tasks import Chain
from fyle_integrations_imports.dataclasses import TaskSetting


def chain_import_fields_to_fyle(workspace_id, task_settings: TaskSetting):
    """
    Chain import fields to Fyle
    :param workspace_id: Workspace Id
    """
    chain = Chain()

    if task_settings['import_categories']:
        chain.append(
            'fyle_integrations_imports.tasks.trigger_import_via_schedule',
            workspace_id,
            task_settings['import_categories']['destination_field'],
            'CATEGORY',
            task_settings['sdk_connection_string'],
            task_settings['credentials'],
            task_settings['import_categories']['destination_sync_method'],
            task_settings['import_categories']['is_auto_sync_enabled'],
            task_settings['import_categories']['is_3d_mapping'],
            False
        )

    if task_settings['mapping_settings']:
        for mapping_setting in task_settings['mapping_settings']:
            if mapping_setting['source_field'] in ['PROJECT']:
                chain.append(
                    'fyle_integrations_imports.tasks.trigger_import_via_schedule',
                    workspace_id,
                    mapping_setting['destination_field'],
                    mapping_setting['source_field'],
                    task_settings['sdk_connection_string'],
                    task_settings['credentials'],
                    mapping_setting['destination_sync_method'],
                    mapping_setting['is_auto_sync_enabled'],
                    False,
                    mapping_setting['is_custom']
                )

    if chain.length() > 0:
        chain.run()
