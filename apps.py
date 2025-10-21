from django.apps import AppConfig


class FyleIntegrationsImportsConfig(AppConfig):
    name = 'fyle_integrations_imports'

    def ready(self):
        import fyle_integrations_imports.signals  # noqa: F401
