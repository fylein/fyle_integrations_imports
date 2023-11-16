from dataclasses import dataclass
from typing import List, Optional, Dict, Any


@dataclass
class MappingSetting:
    source_field: str
    destination_field: str
    destination_sync_method: str
    is_custom: bool
    is_auto_sync_enabled: bool


@dataclass
class TaskSettings:
    import_tax_codes: Optional[Dict[str, str]]
    import_vendors_as_merchants: Optional[Dict[str, str]]
    import_categories: Optional[Dict[str, str]]
    mapping_settings: List[MappingSetting]
    sdk_connection_string: str
    credentials: Optional[Any] = None
