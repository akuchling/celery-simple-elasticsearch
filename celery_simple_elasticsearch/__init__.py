__version__ = '0.1.0.2'


def version_hook(config):
    config['metadata']['version'] = __version__
