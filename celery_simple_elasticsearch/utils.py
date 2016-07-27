from django.core.exceptions import ImproperlyConfigured
from django.db import connection, transaction
from importlib import import_module

from .conf import settings


def get_update_task(task_path=None):
    default_task = settings.CELERY_SIMPLE_ELASTICSEARCH_DEFAULT_TASK
    import_path = task_path or default_task

    module, attr = import_path.rsplit('.', 1)
    try:
        mod = import_module(module)
    except ImportError as e:
        raise ImproperlyConfigured('Error importing module %s: "%s"' %
                                   (module, e))
    try:
        Task = getattr(mod, attr)
    except AttributeError:
        raise ImproperlyConfigured('Module "%s" does not define a "%s" '
                                   'class.' % (module, attr))
    return Task()


def enqueue_task(action, instance, instantiator=None):
    """
    Common utility for enqueing a task for the given action and
    model instance. Optionally provide an instantiator that handles
    instance instantiation.
    """
    def submit_task():
        if transaction.get_connection().in_atomic_block:
            with transaction.atomic():
                task.delay(action, identifier, instantiator)
        else:
            task.delay(action, identifier, instantiator)

    action = get_method_identifier(action)
    identifier = get_object_identifier(instance)
    if instantiator:
        instantiator = get_method_identifier(instantiator)

    kwargs = {}
    if settings.CELERY_SIMPLE_ELASTICSEARCH_QUEUE:
        kwargs['queue'] = settings.CELERY_SIMPLE_ELASTICSEARCH_QUEUE
    if settings.CELERY_SIMPLE_ELASTICSEARCH_COUNTDOWN:
        kwargs['countdown'] = settings.CELERY_SIMPLE_ELASTICSEARCH_COUNTDOWN
    task = get_update_task()
    if hasattr(connection, 'on_commit'):
        connection.on_commit(
                lambda: submit_task()
        )
    else:
        submit_task()


def get_object_identifier(obj):
    """
    This function will provide a dot notated reference to the
    item to identify.
    """
    return u'{}.{}.{}'.format(
        obj._meta.app_label,
        obj.__class__.__name__,
        obj.id
    )


def get_method_identifier(identify):
    """
    This function provides a dot notated reference to a bound
    function
    """
    return u'{}.{}.{}'.format(
        identify.im_self._meta.app_label,
        identify.im_self.__name__,
        identify.im_func.__name__
    )
