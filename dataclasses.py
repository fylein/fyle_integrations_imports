from dataclasses import dataclass
from typing import List, Optional
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
class TaskSetting:
    import_tax_codes: Optional[ImportConfig]
    import_vendors_as_merchants: Optional[ImportConfig]
    import_categories: Optional[ImportConfig]
    import_items: bool
    mapping_settings: List[MappingSetting]
    sdk_connection_string: str
    credentials: models.Model
