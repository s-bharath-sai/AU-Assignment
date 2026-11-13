"""Entry point runnable from the repo root: python solution/main.py <command> ..."""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

from pipeline.cli import main  # noqa: E402

sys.exit(main())
