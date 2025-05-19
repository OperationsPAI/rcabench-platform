import importlib.metadata

__version__ = importlib.metadata.version(__package__ or __name__)

__all__ = []


def main():
    from .v1.cli.main import with_subcommands

    app = with_subcommands()
    app()
