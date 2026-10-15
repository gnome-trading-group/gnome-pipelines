import sys
from pathlib import Path

# The image copies app.py on its own, so tests import it the same way, and must leave no compiled files beside it.
sys.dont_write_bytecode = True
sys.path.insert(0, str(Path(__file__).parents[2] / "cdk" / "data-api"))
