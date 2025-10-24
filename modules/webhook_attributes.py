import logging
from datetime import datetime
from typing import Dict, Any

from django.core.cache import cache
from fyle_accounting_mappings.models import ExpenseAttribute
from fyle_integrations_imports.models import ImportLog
from fyle_accounting_library.fyle_platform.enums import WebhookAttributeActionEnum, ImportLogStatusEnum, FyleAttributeTypeEnum, CacheKeyEnum


logger = logging.getLogger(__name__)
logger.level = logging.INFO

ATTRIBUTE_FIELD_MAPPING = {
    FyleAttributeTypeEnum.CATEGORY: {
        'active_field': 'is_enabled',
        'display_name': 'Category'
    },
    FyleAttributeTypeEnum.PROJECT: {
        'active_field': 'is_enabled',
        'display_name': 'Project'
    },
    FyleAttributeTypeEnum.COST_CENTER: {
        'active_field': 'is_enabled',
        'display_name': 'Cost Center'
    },
    FyleAttributeTypeEnum.EMPLOYEE: {
        'active_field': 'is_enabled',
        'detail_fields': {
            'user_id': 'user_id',
            'employee_code': 'code',
            'full_name': 'user.full_name',
            'location': 'location',
            'department': 'department.name',
            'department_id': 'department_id',
            'department_code': 'department.code'
        },
        'display_name': 'Employee'
    },
    FyleAttributeTypeEnum.CORPORATE_CARD: {
        'active_default': True,
        'detail_fields': {
            'cardholder_name': 'cardholder_name'
        },
        'display_name': 'Corporate Card'
    },
    FyleAttributeTypeEnum.TAX_GROUP: {
        'active_field': 'is_enabled',
        'detail_fields': {
            'tax_rate': 'percentage'
        },
        'display_name': 'Tax Group'
    },
    FyleAttributeTypeEnum.EXPENSE_FIELD: {
        'active_field': 'is_enabled',
        'detail_fields': {
            'custom_field_id': 'id',
            'placeholder': 'placeholder',
            'is_mandatory': 'is_mandatory',
            'is_dependent': False
        },
        'display_name_field': 'field_name',
        'display_name': 'Expense Field'
    },
    FyleAttributeTypeEnum.DEPENDENT_FIELD: {
        'active_field': 'is_enabled',
        'detail_fields': {
            'custom_field_id': 'id',
            'placeholder': 'placeholder',
            'is_mandatory': 'is_mandatory',
            'is_dependent': True
        },
        'display_name_field': 'field_name',
        'display_name': 'Dependent Field'
    }
}


class WebhookAttributeProcessor:
    """
    Class to process webhook attribute changes
    """
    
    def __init__(self, workspace_id: int):
        self.workspace_id = workspace_id

    def _get_nested_value(self, data: Dict[str, Any], field_path: str) -> Any:
        """Get value from nested dict using dot notation"""
        try:
            value = data
            for field in field_path.split('.'):
                value = value.get(field, {}) if isinstance(value, dict) else None
                if value is None:
                    break
            return value
        except Exception as e:
            logger.error(f"Error getting nested value for {field_path}: {e}")
            return None

    def _get_attribute_data(self, data: Dict[str, Any], attribute_type: FyleAttributeTypeEnum) -> Dict[str, Any]:
        """
        Get value, source_id, detail, active, display_name for the attribute type
        """
        config = ATTRIBUTE_FIELD_MAPPING.get(attribute_type, {})
        source_id = str(data.get('id', ''))
        value = None

        if attribute_type == FyleAttributeTypeEnum.CATEGORY:
            name = data.get('name', '')
            sub_category = data.get('sub_category', '')
            value = f"{name} / {sub_category}" if (sub_category and name != sub_category) else name
            
        elif attribute_type == FyleAttributeTypeEnum.PROJECT:
            name = data.get('name', '')
            sub_project = data.get('sub_project', '')
            value = f"{name} / {sub_project}" if sub_project else name
            
        elif attribute_type == FyleAttributeTypeEnum.EMPLOYEE:
            value = self._get_nested_value(data, 'user.email')
            
        elif attribute_type == FyleAttributeTypeEnum.CORPORATE_CARD:
            bank_name = data.get('bank_name', '')
            last_6 = data.get('card_number', '')[-6:].replace('-', '')
            value = f"{bank_name} - {last_6}"
            
        else:  # COST_CENTER, TAX_GROUP
            value = data.get('name', None)

        active_field = config.get('active_field')
        active = data.get(active_field, True) if active_field else config.get('active_default', True)

        detail = None
        if config.get('detail_fields'):
            detail = {}
            for detail_key, source_field in config['detail_fields'].items():
                if isinstance(source_field, str):
                    detail[detail_key] = self._get_nested_value(data, source_field)
                else:
                    detail[detail_key] = source_field

        display_name_field = config.get('display_name_field')
        display_name = data.get(display_name_field, config['display_name']) if display_name_field else config.get('display_name', '')

        return {
            'value': value,
            'source_id': source_id,
            'detail': detail,
            'active': active,
            'display_name': display_name,
            'attribute_type': attribute_type.value
        }

    def _process_expense_field(self, data: Dict[str, Any]) -> None:
        """
        Process EXPENSE_FIELD type with special handling for options
        :param data: Webhook data
        :return: None
        """
        field_name = data.get('field_name', '')
        field_type = data.get('type', '')

        if field_type != 'SELECT':
            logger.debug(f"Skipping non-SELECT expense field {field_name} of type {field_type} for workspace {self.workspace_id}")
            return
            
        attribute_type = field_name.upper().replace(' ', '_')
        options = data.get('options', [])

        existing_attributes = ExpenseAttribute.objects.filter(
            workspace_id=self.workspace_id,
            attribute_type=attribute_type
        )
        existing_values = set(attr.value for attr in existing_attributes)
        new_options_set = set(options)

        new_options = new_options_set - existing_values
        if new_options:
            attributes_to_create = []
            for option in new_options:
                attribute_data = self._get_attribute_data(data, FyleAttributeTypeEnum.EXPENSE_FIELD)
                attribute_data['value'] = option
                attribute_data['attribute_type'] = attribute_type
                attribute_data['workspace_id'] = self.workspace_id
                attributes_to_create.append(ExpenseAttribute(**attribute_data))
            
            ExpenseAttribute.objects.bulk_create(attributes_to_create, batch_size=50)

        attributes_to_disable = existing_values - new_options_set
        if attributes_to_disable:
            ExpenseAttribute.objects.filter(
                workspace_id=self.workspace_id,
                attribute_type=attribute_type,
                active=True,
                value__in=attributes_to_disable
            ).update(active=False, updated_at=datetime.now())

    def _process_expense_attribute(self, data: Dict[str, Any], attribute_type: FyleAttributeTypeEnum, action: WebhookAttributeActionEnum) -> None:
        """
        Process expense attribute based on webhook action (CREATED, UPDATED, DELETED)
        :param data: Webhook data
        :param attribute_type: Type of attribute
        :param action: Webhook action
        :return: None
        """
        source_id = str(data.get('id'))
        if attribute_type == FyleAttributeTypeEnum.EMPLOYEE and data.get('has_accepted_invite') == False:
            logger.info(f"Employee {data.get('user', {}).get('email')} has not accepted invite, skipping webhook processing")
            return

        if attribute_type == FyleAttributeTypeEnum.EXPENSE_FIELD:
            self._process_expense_field(data)
            logger.debug(f"Successfully processed {action.value} webhook for {attribute_type.value} in workspace {self.workspace_id}")
            return
        
        if action == WebhookAttributeActionEnum.DELETED:
            ExpenseAttribute.objects.filter(
                workspace_id=self.workspace_id,
                attribute_type=attribute_type.value,
                active=True,
                source_id=source_id
            ).update(active=False, updated_at=datetime.now())
            logger.debug(f"Disabled expense attribute {attribute_type.value} with source_id {source_id} for workspace {self.workspace_id}")
        else:
            attribute_data = self._get_attribute_data(data, attribute_type)
            ExpenseAttribute.create_or_update_expense_attribute(
                attribute=attribute_data,
                workspace_id=self.workspace_id
            )
            logger.debug(f"Processed {action.value} for expense attribute {attribute_type.value} with source_id {source_id} for workspace {self.workspace_id}")
    
    def _is_import_in_progress(self, attribute_type: FyleAttributeTypeEnum) -> bool:
        """
        Check if import is already in progress for the given attribute type
        Cached for 15 minutes to optimize webhook bursts
        :param attribute_type: Attribute type to check
        :return: True if import is in progress, False otherwise
        """
        cache_key = CacheKeyEnum.IMPORT_LOG_IN_PROGRESS.value.format(workspace_id=self.workspace_id, attribute_type=attribute_type.value)
        cached_result = cache.get(cache_key)
        if cached_result is not None:
            return cached_result

        import_in_progress = ImportLog.objects.filter(
            workspace_id=self.workspace_id,
            attribute_type=attribute_type.value,
            status=ImportLogStatusEnum.IN_PROGRESS.value
        ).exists()

        cache.set(cache_key, import_in_progress, 900)
        return import_in_progress

    def process_webhook(self, webhook_body: Dict[str, Any]) -> None:
        """
        Process webhook for attribute changes
        :param webhook_body: Webhook payload
        :return: None
        """
        action = WebhookAttributeActionEnum(webhook_body.get('action'))
        attribute_type = FyleAttributeTypeEnum(webhook_body.get('resource'))
        data = webhook_body.get('data')

        if attribute_type not in ATTRIBUTE_FIELD_MAPPING:
            logger.error(f"Unsupported resource type {attribute_type.value} for workspace {self.workspace_id}")
            return

        if self._is_import_in_progress(attribute_type) and action != WebhookAttributeActionEnum.DELETED:
            logger.info(f"Import already in progress for {attribute_type.value} in workspace {self.workspace_id}, skipping webhook processing")
            return

        self._process_expense_attribute(data, attribute_type, action)
        logger.info(f"Successfully processed {action.value} webhook for {attribute_type.value} in workspace {self.workspace_id}")
