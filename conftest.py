"""Root conftest — makes 'backend' importable in all test subdirectories."""
import sys
from pathlib import Path

# Ensure the project root is on sys.path so tests can do:
#   from backend.council import ...
#   from backend.storage import ...
sys.path.insert(0, str(Path(__file__).parent))
