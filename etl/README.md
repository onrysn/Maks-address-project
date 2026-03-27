# ETL Akisi (GDB -> raw_maks)

Bu klasor, MAKS `.gdb` verisini PostGIS `raw_maks` semasina almak icin kullanilir.

## 0) Onkosul

```bash
docker compose up -d --build db etl api
```

## 1) Katmanlari ve alanlari incele

```bash
docker compose run --rm etl -lc "bash /etl/scripts/inspect_gdb.sh /data/raw_gdb/KONYA.gdb"
```

## 2) GDB katmanlarini raw semaya yukle

```bash
docker compose run --rm etl -lc "bash /etl/scripts/import_gdb_to_raw.sh /data/raw_gdb/KONYA.gdb"
```

Yukleme hedefi: `raw_maks.*` tablolari

Not: Varsayilan mod `IMPORT_MODE=core` oldugu icin yalnizca reverse geocoding icin gereken katmanlar alinir.
Tum katmanlari almak icin:

```bash
docker compose run --rm etl -lc "IMPORT_MODE=all bash /etl/scripts/import_gdb_to_raw.sh /data/raw_gdb/KONYA.gdb"
```

## 3) raw profilini JSON olarak cikar

```bash
docker compose run --rm etl -lc "python /etl/scripts/profile_raw.py"
```

Cikti dosyasi: `data/processed/raw_profile.json`

## 4) API testi

```bash
curl "http://localhost:8000/reverse-geocode?lat=37.8715&lon=32.4846&door_radius_m=15&building_radius_m=40&road_radius_m=90&metric=geodesic"
```

## Tek Komut Pipeline (PowerShell)

Tum adimlari kontrollu sekilde calistirmak icin:

```powershell
powershell -ExecutionPolicy Bypass -File .\etl\scripts\run_pipeline_safe.ps1 -GdbName KONYA.gdb
```

Ikinci calistirmada hizli gitmek icin:

```powershell
powershell -ExecutionPolicy Bypass -File .\etl\scripts\run_pipeline_safe.ps1 -GdbName KONYA.gdb -SkipInspect -SkipImport
```
