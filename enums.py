from enum import Enum


class WebhookAction(str, Enum):
    """Enum for webhook actions"""
    CREATED = 'CREATED'
    UPDATED = 'UPDATED'
    DELETED = 'DELETED'

