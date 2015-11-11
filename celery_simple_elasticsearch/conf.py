from django.conf import settings  # noqa
from appconf import AppConf


class CelerySimpleElasticSearch(AppConf):
    #: The default alias to
    DEFAULT_ALIAS = None
    #: The delay (in seconds) before task will be executed (Celery countdown)
    COUNTDOWN = 0
    #: The delay (in seconds) after which a failed index is retried
    RETRY_DELAY = 5 * 60
    #: The number of retries that are done
    MAX_RETRIES = 1
    #: The default Celery task class
    DEFAULT_TASK = '.'.join([
        'celery_simple_elasticsearch.tasks',
        'CelerySimpleElasticSearchSignalHandler'
    ])
    #: The name of the celery queue to use, or None for default
    QUEUE = None
    #: Whether the task should be handled transaction safe
    TRANSACTION_SAFE = True
    #: The batch size used by CelerySimpleElasticSearchUpdateIndex
    COMMAND_BATCH_SIZE = None
    #: The max age of items used by CelerySimpleElasticSearchUpdateIndex
    COMMAND_AGE = None
    #: Wehther to remove items from the index that aren't in the DB anymore
    COMMAND_REMOVE = False
    #: The number of multiprocessing workers used
    COMMAND_WORKERS = 0
    #: The names of apps to run update_index for
    COMMAND_APPS = []
    #: The verbosity level of the update_index call
    COMMAND_VERBOSITY = 1

    def configure_default_alias(self, value):
        return value or getattr(self, 'DEFAULT_ALIAS', None)

    def configure(self):
        data = {}
        for name, value in self.configured_data.items():
            if name in ('RETRY_DELAY', 'MAX_RETRIES',
                        'COMMAND_WORKERS', 'COMMAND_VERBOSITY'):
                value = int(value)
            data[name] = value
        return data


signal_processor = getattr(
    settings,
    'SIMPLE_ELASTICSEARCH_SIGNAL_PROCESSOR',
    None
)
