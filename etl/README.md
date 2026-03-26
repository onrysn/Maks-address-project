# ETL Akışı (GDB -> raw_maks -> core)

Bu klasör, MAKS `.gdb` verisini PostGIS'e almak ve API'nin kullandığı core tablolara map etmek için hazırlanmıştır.

## 0) Önkoşul

```bash
docker compose up -d --build db etl api
```

## 1) Katmanları ve alanları incele

```bash
docker compose run --rm etl -lc "bash /etl/scripts/inspect_gdb.sh /data/raw_gdb/KONYA.gdb"
```

## 2) GDB katmanlarını raw şemaya yükle

```bash
docker compose run --rm etl -lc "bash /etl/scripts/import_gdb_to_raw.sh /data/raw_gdb/KONYA.gdb"
```

Yükleme hedefi: `raw_maks.*` tabloları

## 3) raw profilini JSON olarak çıkar

```bash
docker compose run --rm etl -lc "python /etl/scripts/profile_raw.py"
```

Çıktı dosyası: `data/processed/raw_profile.json`

## 4) Mapping dosyasını oluştur

```bash
copy etl\config\mapping.konya.example.json etl\config\mapping.json
```

`mapping.json` içinde tablo/kolon adlarını kendi GDB katmanlarına göre güncelle.

## 5) raw -> core map et

```bash
docker compose run --rm etl -lc "MAPPING_FILE=/etl/config/mapping.json python /etl/scripts/map_raw_to_core.py"
```

Hedef core tablolar:

- `admin_city`
- `admin_district`
- `admin_neighborhood`
- `roads`
- `buildings`
- `doors`

## 6) API testi

```bash
curl "http://localhost:8000/reverse-geocode?lat=37.8715&lon=32.4846&door_radius_m=15&building_radius_m=40&road_radius_m=90&metric=geodesic"
```

## Tek Komut Pipeline (PowerShell)

Tum adimlari kontrollu sekilde calistirmak icin:

```powershell
powershell -ExecutionPolicy Bypass -File .\etl\scripts\run_pipeline.ps1 -GdbName KONYA.gdb
```

Notlar:

- `mapping.json` yoksa script otomatik kopyalar ve bilincli olarak durur.
- `etl/config/mapping.json` icindeki tablo/kolon adlarini duzenledikten sonra tekrar calistir.
- Ikinci calistirmada hizli gitmek icin:

```powershell
powershell -ExecutionPolicy Bypass -File .\etl\scripts\run_pipeline.ps1 -GdbName KONYA.gdb -SkipInspect -SkipImport
```

