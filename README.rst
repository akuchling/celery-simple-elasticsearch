===========================
celery-simple-elasticsearch
===========================

This Django app allows you to utilize Celery for automatically updating and
deleting objects in a `Django Simple Elasticsearch`_ search index.

Requirements
------------

* Django 1.9+
* `Django Simple Elasticsearch`_ `0.9.16`_
* `Celery`_ 3.X

Installation
------------

Use your favorite Python package manager to install the app from PyPI, e.g.::

    pip install celery-simple-elasticsearch

By default a few dependencies will automatically be installed:

- `django-appconf`_ -- An app to gracefully handle application settings.

Usage
-----

#. Add ``'celery_simple_elasticsearch'`` to the ``INSTALLED_APPS`` setting

   .. code:: python

     INSTALLED_APPS = [
         # ..
         'celery_simple_elasticsearch',
     ]

#. Alter all of your ``ElasticsearchIndexMixin`` subclasses to also inherit
   from ``celery_simple_elasticsearch.indexes.CelerySearchIndex``

   .. code:: python

      from django.db import models
      from celery_simple_elasticsearch.indexes import CelerySearchIndex
      from simple_elasticsearch.mixins import ElasticsearchIndexMixin

      class Person(CelerySearchIndex, ElasticsearchIndexMixin, models.Model):
        first_name = models.CharField(max_length=30)
        last_name = models.CharField(max_length=30)

#. Ensure your Celery instance is running.

Thanks
------

This app is a blatant rip-off of `Celery Haystack`_. A great basis,
but I didn't want Haystack users to have all the celery goodness
to themselves.

Issues
------

Please use the `Github issue tracker`_ for any bug reports or feature
requests.

.. _`Django Simple Elasticsearch`: http://django-simple-elasticsearch.readthedocs.org/en/latest/
.. _`Celery Haystack`: https://celery-haystack.readthedocs.org/en/latest/
.. _`0.9.16`: https://pypi.python.org/pypi/django-simple-elasticsearch/0.9.16
.. _`Celery`: http://celeryproject.org/
.. _`Github issue tracker`: https://github.com/jimjkelly/celery-simple-elasticsearch/issues
.. _`django-appconf`: http://pypi.python.org/pypi/django-appconf
.. _`Celery's user guide`: http://celery.readthedocs.org/en/latest/userguide/tasks.html#database-transactions
