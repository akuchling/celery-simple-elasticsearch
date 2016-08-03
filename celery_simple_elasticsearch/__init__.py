__version__ = '0.1.0.3'


def version_hook(config):
    config['metadata']['version'] = __version__
