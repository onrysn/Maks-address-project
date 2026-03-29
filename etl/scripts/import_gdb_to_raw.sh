#!/usr/bin/env bash
set -euo pipefail

if [[ $# -lt 1 ]]; then
  echo "Usage: ./scripts/import_gdb_to_raw.sh /data/raw_gdb/KONYA.gdb"
  exit 1
fi

GDB_PATH="$1"
RAW_SCHEMA="raw_maks"
# Import all layers by default.
IMPORT_MODE="${IMPORT_MODE:-all}"
# Import behavior: overwrite (default) or append.
IMPORT_BEHAVIOR="${IMPORT_BEHAVIOR:-overwrite}"

: "${POSTGRES_HOST:?POSTGRES_HOST is required}"
: "${POSTGRES_PORT:?POSTGRES_PORT is required}"
: "${POSTGRES_DB:?POSTGRES_DB is required}"
: "${POSTGRES_USER:?POSTGRES_USER is required}"
: "${POSTGRES_PASSWORD:?POSTGRES_PASSWORD is required}"

if [[ ! -d "$GDB_PATH" ]]; then
  echo "GDB path not found: $GDB_PATH"
  exit 1
fi

if [[ "$IMPORT_BEHAVIOR" != "overwrite" && "$IMPORT_BEHAVIOR" != "append" ]]; then
  echo "Invalid IMPORT_BEHAVIOR: $IMPORT_BEHAVIOR (allowed: overwrite|append)"
  exit 1
fi

export PGPASSWORD="$POSTGRES_PASSWORD"

psql \
  -h "$POSTGRES_HOST" \
  -p "$POSTGRES_PORT" \
  -U "$POSTGRES_USER" \
  -d "$POSTGRES_DB" \
  -c "CREATE SCHEMA IF NOT EXISTS ${RAW_SCHEMA};"

mapfile -t ALL_LAYERS < <(ogrinfo -ro "$GDB_PATH" | awk '
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
')

if [[ ${#ALL_LAYERS[@]} -eq 0 ]]; then
  echo "No layers found in $GDB_PATH"
  exit 1
fi

if [[ "$IMPORT_MODE" == "all" ]]; then
  LAYERS=("${ALL_LAYERS[@]}")
else
  # Minimal required layers for Konya reverse geocoding preset.
  REQUIRED=(il ilce mahalle yolortahat yolortahatyon yol yapi numarataj)
  LAYERS=()
  for r in "${REQUIRED[@]}"; do
    for a in "${ALL_LAYERS[@]}"; do
      if [[ "$a" == "$r" ]]; then
        LAYERS+=("$a")
      fi
    done
  done
fi

if [[ ${#LAYERS[@]} -eq 0 ]]; then
  echo "No target layers selected. Check IMPORT_MODE and GDB layer names."
  exit 1
fi

echo "Import mode: $IMPORT_MODE"
echo "Import behavior: $IMPORT_BEHAVIOR"
echo "Layers: ${LAYERS[*]}"

for LAYER in "${LAYERS[@]}"; do
  TABLE_NAME=$(echo "$LAYER" | tr '[:upper:]' '[:lower:]' | sed 's/[^a-z0-9_]/_/g' | sed 's/__\+/_/g' | sed 's/^_\|_$//g')

  if [[ -z "$TABLE_NAME" ]]; then
    echo "Skipping invalid layer name: $LAYER"
    continue
  fi

  echo "Importing layer '$LAYER' -> ${RAW_SCHEMA}.${TABLE_NAME}"

  OGR_FLAGS=(
    -f PostgreSQL
    "PG:host=${POSTGRES_HOST} port=${POSTGRES_PORT} dbname=${POSTGRES_DB} user=${POSTGRES_USER} password=${POSTGRES_PASSWORD} active_schema=${RAW_SCHEMA}"
    "$GDB_PATH"
    "$LAYER"
    -nln "$TABLE_NAME"
    -lco GEOMETRY_NAME=geom
    -nlt PROMOTE_TO_MULTI
    -t_srs EPSG:4326
    -makevalid
  )

  if [[ "$IMPORT_BEHAVIOR" == "append" ]]; then
    OGR_FLAGS+=(-append)
  else
    OGR_FLAGS+=(-overwrite)
  fi

  ogr2ogr \
    "${OGR_FLAGS[@]}"
 done

echo "Raw import complete."
