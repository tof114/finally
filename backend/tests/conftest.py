import sys
import os

# Ensure the backend app package is importable from tests.
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
