from django.core.exceptions import ImproperlyConfigured
from django.core.management import call_command
from django.db.models.loading import get_model
from celery.utils.log import get_task_logger

from .conf import settings

if settings.CELERY_SIMPLE_ELASTICSEARCH_TRANSACTION_SAFE \
   and not getattr(settings, 'CELERY_ALWAYS_EAGER', False):
    from djcelery_transactions import PostTransactionTask as Task
else:
    from celery.task import Task  # noqa

logger = get_task_logger(__name__)


class CelerySimpleElasticSearchSignalHandler(Task):
    using = settings.CELERY_SIMPLE_ELASTICSEARCH_DEFAULT_ALIAS
    max_retries = settings.CELERY_SIMPLE_ELASTICSEARCH_MAX_RETRIES
    default_retry_delay = settings.CELERY_SIMPLE_ELASTICSEARCH_RETRY_DELAY

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

    def get_model_class(self, object_path, **kwargs):
        """
        Fetch the model's class in a standarized way.
        """
        bits = object_path.split('.')
        app_name = '.'.join(bits[:-1])
        classname = bits[-1]
        model_class = get_model(app_name, classname)

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

    def run(self, action, identifier, **kwargs):
        """
        Trigger the actual index handler depending on the
        given action ('update' or 'delete').
        """
        # First get the object path and pk (e.g. ('notes.note', 23))
        object_path, pk = self.split_identifier(identifier, **kwargs)
        if object_path is None or pk is None:
            msg = "Couldn't handle object with identifier %s" % identifier
            logger.error(msg)
            raise ValueError(msg)

        # Then get the model class for the object path
        model_class = self.get_model_class(object_path, **kwargs)
        current_index_name = model_class.get_index_name()
        instance = self.get_instance(model_class, pk)

        if action == 'delete':
            # If the object is gone, we'll use just the identifier
            # against the index.
            try:
                model_class.index_delete(instance)
            except Exception as exc:
                logger.exception(exc)
                self.retry(exc=exc)
            else:
                msg = ("Deleted '%s' (with %s)" %
                       (identifier, current_index_name))
                logger.debug(msg)
        elif action == 'update':
            try:
                model_class.index_add(instance)
            except Exception as exc:
                logger.exception(exc)
                self.retry(exc=exc)
            else:
                msg = ("Updated '%s' (with %s)" %
                       (identifier, current_index_name))
                logger.debug(msg)
        else:
            logger.error("Unrecognized action '%s'. Moving on..." % action)
            raise ValueError("Unrecognized action %s" % action)


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
        }
        defaults.update(kwargs)
        if apps is None:
            apps = settings.CELERY_SIMPLE_ELASTICSEARCH_COMMAND_APPS
        # Run the update_index management command
        logger.info("Starting update index")
        call_command('--rebuild', *apps, **defaults)
        logger.info("Finishing update index")
