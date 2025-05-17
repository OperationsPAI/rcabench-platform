#!/usr/bin/env -S uv run -s
from rcabench_platform.v1.cli.main import with_subcommands

app = with_subcommands()

if __name__ == "__main__":
    app()
