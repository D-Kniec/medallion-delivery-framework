cd "$(dirname "$0")/../.."

echo "=========================================="
echo "  MEDALLION DELIVERY FRAMEWORK  "
echo "=========================================="
echo "Root: $(pwd)"

export PYTHONPATH=$(pwd)

python scripts/orchestrator.py