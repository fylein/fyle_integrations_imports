from django_q.tasks import Chain
from fyle_accounting_mappings.models import MappingSetting
from apps.workspaces.models import Configuration

def chain_import_fields_to_fyle(workspace_id):
    """
    Chain import fields to Fyle
    :param workspace_id: Workspace Id
    """
    mapping_settings = MappingSetting.objects.filter(workspace_id=workspace_id, import_to_fyle=True)
    custom_field_mapping_settings = MappingSetting.objects.filter(workspace_id=workspace_id, is_custom=True, import_to_fyle=True)
    configuration = Configuration.objects.get(workspace_id=workspace_id)
    chain = Chain()

    if configuration.import_tax_codes:
        chain.append(
            'apps.mappings.imports.tasks.trigger_import_via_schedule',
            workspace_id,
            'TAX_DETAIL',
            'TAX_GROUP'
        )

    if configuration.import_vendors_as_merchants:
        chain.append(
            'apps.mappings.imports.tasks.trigger_import_via_schedule',
            workspace_id,
            'VENDOR',
            'MERCHANT'
        )

    if configuration.import_categories:
        if configuration.reimbursable_expenses_object == 'EXPENSE_REPORT' or \
            configuration.corporate_credit_card_expenses_object == 'EXPENSE_REPORT':
            destination_field = 'EXPENSE_TYPE'
        else:
            destination_field = 'ACCOUNT'

        chain.append(
            'apps.mappings.imports.tasks.trigger_import_via_schedule',
            workspace_id,
            destination_field,
            'CATEGORY'
        )

    for mapping_setting in mapping_settings:
        if mapping_setting.source_field in ['PROJECT', 'COST_CENTER']:
            chain.append(
               'apps.mappings.imports.tasks.trigger_import_via_schedule',
                workspace_id,
                mapping_setting.destination_field,
                mapping_setting.source_field
            )

    for custom_fields_mapping_setting in custom_field_mapping_settings:
        chain.append(
            'apps.mappings.imports.tasks.trigger_import_via_schedule',
            workspace_id,
            custom_fields_mapping_setting.destination_field,
            custom_fields_mapping_setting.source_field,
            True
        )

    if chain.length() > 0:
        chain.run()
