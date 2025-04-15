from dataclasses import dataclass
from typing import List, Optional, Dict
from django.db import models


@dataclass
class MappingSetting:
    source_field: str
    destination_field: str
    destination_sync_methods: List[str]
    is_custom: bool
    is_auto_sync_enabled: bool


@dataclass
class ImportConfig:
    destination_field: str
    destination_sync_methods: List[str]
    is_auto_sync_enabled: bool


@dataclass
class CustomProperties:
    func: str
    args: Optional[Dict]


@dataclass
class TaskSetting:
    import_tax_codes: Optional[ImportConfig]
    import_vendors_as_merchants: Optional[ImportConfig]
    import_suppliers_as_merchants: Optional[ImportConfig]
    import_categories: Optional[ImportConfig]
    import_items: bool
    mapping_settings: List[MappingSetting]
    sdk_connection_string: str
    credentials: models.Model
    custom_properties: Optional[CustomProperties]
    import_dependent_fields: Optional[CustomProperties]
