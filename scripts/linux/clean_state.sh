cd "$(dirname "$0")/../.."

echo "Cleaning Data Directories..."

rm -rf data/silver/*
rm -rf data/checkpoints/*

mkdir -p data/silver
mkdir -p data/checkpoints
touch data/bronze/.gitkeep
touch data/silver/.gitkeep
touch data/checkpoints/.gitkeep

echo "Done."