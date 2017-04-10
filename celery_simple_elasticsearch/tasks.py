from celery.utils.log import get_task_logger
from django.apps import apps
from django.db import transaction
from django.core.exceptions import ImproperlyConfigured
from django.core.management import call_command
from importlib import import_module

from .conf import settings

from celery import current_app, Task

logger = get_task_logger(__name__)


def _get_celery_settings(setting, default=False):
    """ Returns CELERY setting
    :param setting:
    :param default:
    :return:
    """
    return any(getattr(obj, setting, default)
               for obj in (current_app.conf, settings))


class CelerySimpleElasticSearchSignalHandler(Task):
    using = settings.CELERY_SIMPLE_ELASTICSEARCH_DEFAULT_ALIAS
    max_retries = settings.CELERY_SIMPLE_ELASTICSEARCH_MAX_RETRIES
    default_retry_delay = settings.CELERY_SIMPLE_ELASTICSEARCH_RETRY_DELAY

    def original_apply_async(self, *args, **kwargs):
        """Shortcut method to reach real implementation
        of celery.Task.apply_sync
        """
        return super(
            CelerySimpleElasticSearchSignalHandler, self
        ).apply_async(*args, **kwargs)

    def apply_async(self, *args, **kwargs):
        # Delay the task unless the client requested otherwise or transactions
        # aren't being managed (i.e. the signal handlers won't send the task).

        celery_eager = _get_celery_settings('CELERY_ALWAYS_EAGER')

        # New setting to run eager task post transaction
        # defaults to `not CELERY_ALWAYS_EAGER`
        eager_transaction = _get_celery_settings('CELERY_EAGER_TRANSACTION',
                                                 not celery_eager)

        connection = transaction.get_connection()
        if connection.in_atomic_block and eager_transaction:
            transaction.on_commit(
                lambda: self.original_apply_async(*args, **kwargs)
            )
        else:
            return self.original_apply_async(*args, **kwargs)

    def split_identifier(self, identifier, **kwargs):
        """
        Break down the identifier representing the instance.

        Converts 'notes.note.23' into ('notes.note', 23).
        """
        bits = identifier.split('.')

        if len(bits) < 2:
            logger.error("Unable to parse object "
                         "identifer '%s'. Moving on..." % identifier)
            return (None, None)

        pk = bits[-1]
        # In case Django ever handles full paths...
        object_path = '.'.join(bits[:-1])
        return (object_path, pk)

    def get_method(self, method_identifier):
        """
        Given a method identifier, return the method
        """
        class_string, method_string = method_identifier.rsplit('.', 1)

        try:
            class_obj = self.get_model_class(class_string)
        except ImproperlyConfigured:
            # We assume that just means this isn't a Django model - try
            # loading it as a module:
            class_obj = import_module(class_string)

        method = getattr(class_obj, method_string, None)

        if not method:
            msg = 'Could not get method from "{}"'.format(method_identifier)
            logger.error(msg)
            raise ValueError(msg)

        return method

    def get_model_class(self, object_path, **kwargs):
        """
        Fetch the model's class in a standarized way.
        """
        bits = object_path.split('.')
        app_name = '.'.join(bits[:-1])
        classname = bits[-1]
        model_class = apps.get_model(app_name, classname)

        if model_class is None:
            raise ImproperlyConfigured("Could not load model '%s'." %
                                       object_path)
        return model_class

    def get_instance(self, model_class, pk, **kwargs):
        """
        Fetch the instance in a standarized way.
        """
        instance = None

        try:
            instance = model_class._default_manager.get(pk=pk)
        except model_class.DoesNotExist:
            logger.error("Couldn't load %s.%s.%s. Somehow it went missing?" %
                         (model_class._meta.app_label.lower(),
                          model_class._meta.object_name.lower(), pk))
        except model_class.MultipleObjectsReturned:
            logger.error("More than one object with pk %s. Oops?" % pk)

        return instance

    def run(self, action, identifier, instantiator=None, **kwargs):
        """
        Trigger the actual index handler depending on the
        given action.
        """
        # First get the object path and pk (e.g. ('notes.note', 23))
        object_path, pk = self.split_identifier(identifier, **kwargs)
        if object_path is None or pk is None:
            msg = "Couldn't handle object with identifier %s" % identifier
            logger.error(msg)
            raise ValueError(msg)

        # Then get the model class for the object path
        model_class = self.get_model_class(object_path, **kwargs)

        if instantiator:
            instantiator_method = self.get_method(instantiator)
        else:
            instantiator_method = self.get_instance

        instance = instantiator_method(model_class, pk)

        if hasattr(model_class, 'get_index_name'):
            current_index_name = model_class.get_index_name()
        else:
            current_index_name = settings.ELASTICSEARCH_INDEX

        action_method = self.get_method(action)

        try:
            action_method(instance)
        except Exception as exc:
            logger.exception(exc)
            self.retry(exc=exc)
        else:
            logger.debug('Successfully executed {} on {} for index {}'.format(
                action,
                identifier,
                current_index_name,
            ))


class CelerySimpleElasticSearchUpdateIndex(Task):
    """
    A celery task class to be used to call the update_index management
    command from Celery.
    """
    def run(self, apps=None, **kwargs):
        defaults = {
          'batchsize': settings.CELERY_SIMPLE_ELASTICSEARCH_COMMAND_BATCH_SIZE,
          'age': settings.CELERY_SIMPLE_ELASTICSEARCH_COMMAND_AGE,
          'remove': settings.CELERY_SIMPLE_ELASTICSEARCH_COMMAND_REMOVE,
          'using': [settings.CELERY_SIMPLE_ELASTICSEARCH_DEFAULT_ALIAS],
          'workers': settings.CELERY_SIMPLE_ELASTICSEARCH_COMMAND_WORKERS,
          'verbosity': settings.CELERY_SIMPLE_ELASTICSEARCH_COMMAND_VERBOSITY,
          'no_input': True,
          'rebuild': True,
        }
        defaults.update(kwargs)
        if apps is None:
            apps = settings.CELERY_SIMPLE_ELASTICSEARCH_COMMAND_APPS
        # Run the update_index management command
        logger.info("Starting update index")
        call_command('es_manage', *apps, **defaults)
        logger.info("Finishing update index")
