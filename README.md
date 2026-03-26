# MAKS Reverse Geocoding Starter

Bu proje Docker ile calisan bir MAKS reverse geocoding iskeletidir:

- `PostGIS` veritabani
- `FastAPI` reverse geocode servisi
- `ETL` container (GDB analiz + import + mapping)

## 1) Servisleri baslat

```bash
docker compose up -d --build
```

API:

- Health: `http://localhost:8000/health`
- Reverse geocode: `http://localhost:8000/reverse-geocode?lat=37.87&lon=32.49`

## 2) MAKS verisini yerlestir

`.gdb` klasorunu `data/raw_gdb/` altina koy.

Ornek: `data/raw_gdb/KONYA.gdb`

## 3) ETL calistir

Tek komut pipeline (PowerShell):

```powershell
powershell -ExecutionPolicy Bypass -File .\etl\scripts\run_pipeline.ps1 -GdbName KONYA.gdb
```

Detayli adimlar icin: [etl/README.md](./etl/README.md)

## 4) Query bazli yakinlik parametreleri

Her cagrida degistirilebilir:

- `door_radius_m` (varsayilan 20)
- `building_radius_m` (varsayilan 60)
- `road_radius_m` (varsayilan 120)
- `metric`:
  - `geodesic` (WGS84 geography)
  - `planar` (EPSG:3857)

Ornek:

```bash
curl "http://localhost:8000/reverse-geocode?lat=37.8715&lon=32.4846&door_radius_m=12&building_radius_m=40&road_radius_m=90&metric=geodesic"
```


## 5) Basit UI

Tarayicida su adrese git:

- http://localhost:8000/ui

UI uzerinden koordinat girip reverse geocode sorgusu yapabilir, haritaya tiklayarak nokta secip sonucu gorebilirsin.

