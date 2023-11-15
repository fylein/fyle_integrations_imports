from django_q.tasks import Chain

def chain_import_fields_to_fyle(workspace_id, tasks_settings: dict):
    """
    Chain import fields to Fyle
    :param workspace_id: Workspace Id
    """
    chain = Chain()

    if tasks_settings['mapping_settings']:
        for mapping_setting in tasks_settings['mapping_settings']:
            if mapping_setting['source_field'] in ['PROJECT']:
                chain.append(
                    'fyle_integrations_imports.tasks.trigger_import_via_schedule',
                    workspace_id,
                    mapping_setting['destination_field'],
                    mapping_setting['source_field'],
                    tasks_settings['sdk_connection_string'],
                    tasks_settings['credentials'],
                    mapping_setting['destination_sync_method'],
                    mapping_setting['is_auto_sync_enabled'],
                    mapping_setting['is_custom']
                )

    if chain.length() > 0:
        chain.run()
