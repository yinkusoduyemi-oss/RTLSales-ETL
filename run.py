

# ================================================================
# ENTRY POINT
# ================================================================
 
import sys, pathlib

_root = pathlib.Path(__file__).resolve().parent
while not (_root / "config.py").exists() and _root != _root.parent:
    _root = _root.parent
if str(_root) not in sys.path:
    sys.path.insert(0, str(_root))

#from src.DataValidator import DataValidator
#from src.DataTransformer import DataTransformer
from src.etl_pipeline import ETLPipeline


if __name__ == "__main__":
    print("=" * 60)
    print("  MODULE 05 — ETL PIPELINE")
    print("=" * 60)

    pipeline = ETLPipeline()
    pipeline.extract().validate().transform().load()

    print(f"\nPipeline object: {pipeline}")