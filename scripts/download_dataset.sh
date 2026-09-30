#!/usr/bin/env bash
# Kaggle "US Accidents (2016-2023)" by Sobhan Moosavi, CC BY-NC-SA 4.0 (~3 GB CSV).
set -euo pipefail
cd "$(dirname "$0")/.."

if ! command -v kaggle >/dev/null; then
  echo "Install the Kaggle CLI first:  pip install kaggle"
  echo "then put your API token at ~/.kaggle/kaggle.json (Kaggle > Settings > Create New Token)."
  exit 1
fi

mkdir -p data/raw
kaggle datasets download -d sobhanmoosavi/us-accidents -p data/raw --unzip
ls -lh data/raw
