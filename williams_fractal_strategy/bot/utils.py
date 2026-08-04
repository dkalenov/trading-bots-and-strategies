"""Small shared helpers used across the bot — logging setup and the
minimal .env loader. Kept separate from config.py so config.py stays
focused on "what the settings are" rather than "how they get loaded"."""
from __future__ import annotations

import logging
import os
from pathlib import Path


def load_dotenv(path: Path) -> None:
    """Minimal .env loader (KEY=VALUE per line, '#' comments) — no
    extra dependency needed for something this small."""
    if not path.exists():
        return
    for line in path.read_text().splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, _, value = line.partition("=")
        key = key.strip()
        value = value.strip().strip('"').strip("'")
        os.environ.setdefault(key, value)


def setup_logging(verbose: bool = False) -> None:
    logging.basicConfig(
        level=logging.DEBUG if verbose else logging.INFO,
        format="%(asctime)s %(levelname)-7s %(name)s: %(message)s",
    )
