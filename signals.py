from django.db.models.signals import post_save
from django.dispatch import receiver
from django.core.cache import cache

from fyle_accounting_library.enums import ImportLogStatusEnum, CacheKeyEnum
from .models import ImportLog


@receiver(post_save, sender=ImportLog)
def invalidate_import_progress_cache(sender, instance, created, **kwargs):
    """
    Invalidate import progress cache when ImportLog status changes from IN_PROGRESS
    to any other state (COMPLETE, FAILED, FATAL)
    
    :param sender: ImportLog model
    :param instance: ImportLog instance
    :param created: Boolean indicating if this is a new record
    :param kwargs: Additional arguments
    """
    # If the record was just created and status is IN_PROGRESS, don't invalidate
    # If the status is not IN_PROGRESS, invalidate the cache
    if instance.status != ImportLogStatusEnum.IN_PROGRESS.value:
        cache_key = CacheKeyEnum.IMPORT_PROGRESS.value.format(workspace_id=instance.workspace_id, attribute_type=instance.attribute_type)
        cache.delete(cache_key)

