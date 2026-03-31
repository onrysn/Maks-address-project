# MAKS Reverse Geocoding Starter

Bu proje Docker ile calisan bir MAKS reverse geocoding iskeletidir:

- `PostGIS` veritabani
- `FastAPI` reverse geocode servisi
- `ETL` container (GDB analiz + raw import)

## 1) Servisleri baslat

```bash
docker compose up -d --build
```

API:

- Health: `http://localhost:8000/health`
- Reverse geocode: `http://localhost:8000/reverse-geocode?lat=37.87&lon=32.49`
- Batch reverse geocode: `POST http://localhost:8000/reverse-geocode/batch`
- Excel import + sonuc indir: `POST http://localhost:8000/reverse-geocode/excel`
  - `parallel_workers` parametresi ile paralel satir isleme desteklenir (1-8).
  - Durum takibi: `GET /reverse-geocode/excel/status/{job_id}`
  - Sonuc indir: `GET /reverse-geocode/excel/download/{job_id}`

## 2) MAKS verisini yerlestir

`.gdb` klasorunu `data/raw_gdb/` altina koy.

Ornek: `data/raw_gdb/KONYA.gdb`

## 3) ETL calistir

Tek komut pipeline (PowerShell):

```powershell
powershell -ExecutionPolicy Bypass -File .\etl\scripts\run_pipeline_safe.ps1 -GdbName KONYA.gdb
```

`data/raw_gdb` altindaki tum `.gdb` klasorlerini tek seferde almak icin:

```powershell
powershell -ExecutionPolicy Bypass -File .\etl\scripts\run_pipeline_safe.ps1 -ImportAllGdbs
```

Yeni eklenen bir `.gdb` dosyasini mevcut raw verinin ustune eklemek (append) icin:

```powershell
powershell -ExecutionPolicy Bypass -File .\etl\scripts\run_pipeline_safe.ps1 -GdbName YENI_IL.gdb -AppendImport -SkipInspect
```

## 3.1) Tek Sefer Import, Sonra Sadece Servis Ac

Veritabani `docker-compose.yml` icindeki `pgdata` volume ile kalicidir. Bu nedenle importu bir kere yaptiktan sonra her acilista tekrar import etmen gerekmez.

Ilk kurulum (import dahil):

```powershell
powershell -ExecutionPolicy Bypass -File .\etl\scripts\run_pipeline_safe.ps1 -ImportAllGdbs -SkipInspect
```

Sonraki acilislar (import yok):

```bash
docker compose up -d
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

Toplu sorgu ornegi:

```bash
curl -X POST "http://localhost:8000/reverse-geocode/batch" \
  -H "Content-Type: application/json" \
  -d "{\"points\":[{\"id\":\"A1\",\"lat\":37.8715,\"lon\":32.4846},{\"id\":\"A2\",\"lat\":37.8700,\"lon\":32.4900}],\"door_radius_m\":50,\"building_radius_m\":50,\"road_radius_m\":90,\"metric\":\"geodesic\",\"parallel_workers\":4}"
```

## 5) Excel Import Akisi

Excel dosyasi `.xlsx` olmalidir:

- A sutunu: `tesisat_no`
- B sutunu: `CBS_X` (lon)
- C sutunu: `CBS_Y` (lat)

Koordinat format kurali:

- Ondalik ayirici olarak `.` veya `,` kabul edilir.
- B (X) araligi: `-180..180`
- C (Y) araligi: `-90..90`
- Hucre metni otomatik trimlenir ve en fazla 64 karakter dikkate alinir.

API dosyayi isler, D sutunundan itibaren su alanlari yazar:

- `IL`
- `ILCE`
- `KOY`
- `KOY_BULMA_YONTEMI` (`poligon_icinde` veya `en_yakin`)
- `MAHALLE`
- `MAHALLE_BULMA_YONTEMI` (`poligon_icinde` veya `en_yakin`)
- `EN_YAKIN_CADDE_SOKAK`
- `BINADAN_GELEN_CADDE_SOKAK`
- `BINA_NO`
- `KAPI_NO`

UI tarafinda job progress (`islenen/toplam/yuzde`) gorunur ve is bitince dosya indirilebilir.

## 6) UI

Tarayicida su adrese git:

- http://localhost:8000/ui

UI uzerinden:

- Tekli koordinat sorgu
- Toplu koordinat sorgu
- Excel import ve sonucu indir

akislari kullanabilirsin.
