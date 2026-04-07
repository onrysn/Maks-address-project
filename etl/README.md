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
Not: Import sonunda varsayilan olarak `raw_maks` icin spatial + join indexleri olusturulur ve `ANALYZE` calisir (`CREATE_INDEXES=1`).

Not: Varsayilan mod `IMPORT_MODE=all` oldugu icin tum katmanlar alinir.
Sadece temel katmanlari almak istersen:

```bash
docker compose run --rm etl -lc "IMPORT_MODE=core bash /etl/scripts/import_gdb_to_raw.sh /data/raw_gdb/KONYA.gdb"
```

Mevcut tablolari silmeden ekleme (append) yapmak icin:

```bash
docker compose run --rm etl -lc "IMPORT_BEHAVIOR=append bash /etl/scripts/import_gdb_to_raw.sh /data/raw_gdb/YENI_IL.gdb"
```

Mevcut import edilmis veri icin indexleri sonradan olusturmak istersen:

```bash
docker compose run --rm etl -lc "export PGPASSWORD=\$POSTGRES_PASSWORD; psql -h \$POSTGRES_HOST -p \$POSTGRES_PORT -U \$POSTGRES_USER -d \$POSTGRES_DB -v ON_ERROR_STOP=1 -f /etl/scripts/create_raw_indexes.sql"
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

`data/raw_gdb` altindaki tum `.gdb` klasorlerini tek seferde import etmek icin:

```powershell
powershell -ExecutionPolicy Bypass -File .\etl\scripts\run_pipeline_safe.ps1 -ImportAllGdbs
```

Yeni gelen bir `.gdb` dosyasini mevcut veriye eklemek icin:

```powershell
powershell -ExecutionPolicy Bypass -File .\etl\scripts\run_pipeline_safe.ps1 -GdbName YENI_IL.gdb -AppendImport -SkipInspect
```

Import modunu pipeline seviyesinde secmek icin:

```powershell
powershell -ExecutionPolicy Bypass -File .\etl\scripts\run_pipeline_safe.ps1 -ImportAllGdbs -ImportMode all
```

Ikinci calistirmada hizli gitmek icin:

```powershell
powershell -ExecutionPolicy Bypass -File .\etl\scripts\run_pipeline_safe.ps1 -GdbName KONYA.gdb -SkipInspect -SkipImport
```
