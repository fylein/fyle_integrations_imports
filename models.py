from django.db import models
from django.db.models import JSONField

from apps.workspaces.models import Workspace


IMPORT_STATUS_CHOICES = (
    ('FATAL', 'FATAL'),
    ('COMPLETE', 'COMPLETE'),
    ('IN_PROGRESS', 'IN_PROGRESS'),
    ('FAILED', 'FAILED')
)


class ImportLog(models.Model):
    """
    Table to store import logs
    """
    # TODO: Add is_custom Flag
    id = models.AutoField(primary_key=True)
    workspace = models.ForeignKey(Workspace, on_delete=models.PROTECT, help_text='Reference to Workspace model')
    attribute_type = models.CharField(max_length=150, help_text='Attribute type')
    status = models.CharField(max_length=255, help_text='Status', choices=IMPORT_STATUS_CHOICES, null=True)
    error_log = JSONField(help_text='Error Log', default=list)
    total_batches_count = models.IntegerField(help_text='Queued batches', default=0)
    processed_batches_count = models.IntegerField(help_text='Processed batches', default=0)
    last_successful_run_at = models.DateTimeField(help_text='Last successful run', null=True)
    created_at = models.DateTimeField(auto_now_add=True, help_text='Created at datetime')
    updated_at = models.DateTimeField(auto_now=True, help_text='Updated at datetime')

    class Meta:
        app_label = 'fyle_integrations_imports'
        db_table = 'import_logs'
        unique_together = ('workspace', 'attribute_type')

    @classmethod
    def update_or_create_in_progress_import_log(self, attribute_type, workspace_id):
        """
        Create import logs set to IN_PROGRESS
        """
        import_log, _ = self.objects.update_or_create(
            workspace_id=workspace_id,
            attribute_type=attribute_type,
            defaults={
                'status': 'IN_PROGRESS'
            }
        )
        return import_log
