#!/usr/bin/env bash
set -euo pipefail

if [[ $# -lt 1 ]]; then
  echo "Usage: ./scripts/inspect_gdb.sh /data/raw_gdb/KONYA.gdb"
  exit 1
fi

GDB_PATH="$1"

if [[ ! -d "$GDB_PATH" ]]; then
  echo "GDB path not found: $GDB_PATH"
  exit 1
fi

echo "[1/2] Layer list"
ogrinfo -ro "$GDB_PATH"

echo
echo "[2/2] Layer details"
LAYERS="$(ogrinfo -ro "$GDB_PATH" | awk '
/^[0-9]+: / {
  name=$0
  sub(/^[0-9]+: /, "", name)
  sub(/ \(.*/, "", name)
  print name
  next
}
/^Layer: / {
  name=$0
  sub(/^Layer: /, "", name)
  sub(/ \(.*/, "", name)
  print name
  next
}
')"

while IFS= read -r LAYER; do
  [[ -z "$LAYER" ]] && continue
  echo "----------------------------------------"
  echo "Layer: $LAYER"
  ogrinfo -ro -so "$GDB_PATH" "$LAYER"
  echo
done <<< "$LAYERS"
