from pathlib import Path
from typing import Literal

from fastapi import FastAPI, HTTPException, Query
from fastapi.responses import FileResponse

from app.db import get_conn

app = FastAPI(title="MAKS Reverse Geocoding", version="0.1.0")
UI_PATH = Path(__file__).resolve().parent / "ui" / "index.html"


@app.get("/health")
def health() -> dict:
    return {"status": "ok"}


@app.get("/")
def ui_root() -> FileResponse:
    return FileResponse(UI_PATH)


@app.get("/ui")
def ui_page() -> FileResponse:
    return FileResponse(UI_PATH)


@app.get("/reverse-geocode")
def reverse_geocode(
    lat: float = Query(..., ge=-90, le=90),
    lon: float = Query(..., ge=-180, le=180),
    door_radius_m: float = Query(20.0, gt=0, le=500),
    building_radius_m: float = Query(60.0, gt=0, le=1000),
    road_radius_m: float = Query(120.0, gt=0, le=2000),
    metric: Literal["geodesic", "planar"] = Query("geodesic"),
) -> dict:
    pt_expr = "ST_SetSRID(ST_MakePoint(%s, %s), 4326)"

    if metric == "geodesic":
        dist = "ST_Distance(p.geom::geography, {geom}::geography)"
        within = "ST_DWithin(p.geom::geography, {geom}::geography, %s)"
    else:
        dist = "ST_Distance(ST_Transform(p.geom, 3857), ST_Transform({geom}, 3857))"
        within = "ST_DWithin(ST_Transform(p.geom, 3857), ST_Transform({geom}, 3857), %s)"

    with get_conn() as conn:
        with conn.cursor() as cur:
            query = """
                WITH p AS (
                  SELECT {pt_expr} AS geom
                ),
                admin AS (
                  SELECT
                    c.city_name AS il,
                    d.district_name AS ilce,
                    n.neighborhood_name AS mahalle
                  FROM p
                  LEFT JOIN admin_neighborhood n ON ST_Contains(n.geom, p.geom)
                  LEFT JOIN admin_district d ON d.id = n.district_id
                  LEFT JOIN admin_city c ON c.id = d.city_id
                  LIMIT 1
                ),
                nearest_door AS (
                  SELECT
                    dr.id,
                    dr.external_no,
                    dr.building_id,
                    {door_dist} AS dist_m
                  FROM doors dr, p
                  WHERE {door_within}
                  ORDER BY dr.geom <-> p.geom
                  LIMIT 1
                ),
                nearest_building AS (
                  SELECT
                    b.id,
                    b.building_no,
                    b.road_id,
                    {building_dist} AS dist_m
                  FROM buildings b, p
                  WHERE {building_within}
                  ORDER BY b.geom <-> p.geom
                  LIMIT 1
                ),
                nearest_road AS (
                  SELECT
                    r.id,
                    r.road_name,
                    {road_dist} AS dist_m
                  FROM roads r, p
                  WHERE {road_within}
                  ORDER BY r.geom <-> p.geom
                  LIMIT 1
                )
                SELECT
                  a.il,
                  a.ilce,
                  a.mahalle,
                  nd.external_no AS kapi_no,
                  nb.building_no AS bina_no,
                  COALESCE(r1.road_name, r2.road_name, nr.road_name) AS cadde,
                  nd.dist_m AS door_dist_m,
                  nb.dist_m AS building_dist_m,
                  nr.dist_m AS road_dist_m,
                  CASE
                    WHEN nd.id IS NOT NULL THEN 'door'
                    WHEN nb.id IS NOT NULL THEN 'building'
                    WHEN nr.id IS NOT NULL THEN 'road'
                    ELSE 'none'
                  END AS source_level
                FROM admin a
                LEFT JOIN nearest_door nd ON TRUE
                LEFT JOIN buildings b1 ON b1.id = nd.building_id
                LEFT JOIN roads r1 ON r1.id = b1.road_id
                LEFT JOIN nearest_building nb ON TRUE
                LEFT JOIN roads r2 ON r2.id = nb.road_id
                LEFT JOIN nearest_road nr ON TRUE
                LIMIT 1
                """.format(
                pt_expr=pt_expr,
                door_dist=dist.format(geom="dr.geom"),
                door_within=within.format(geom="dr.geom"),
                building_dist=dist.format(geom="b.geom"),
                building_within=within.format(geom="b.geom"),
                road_dist=dist.format(geom="r.geom"),
                road_within=within.format(geom="r.geom"),
            )
            cur.execute(
                query,
                (
                    lon,
                    lat,
                    door_radius_m,
                    building_radius_m,
                    road_radius_m,
                ),
            )

            row = cur.fetchone()
            if not row:
                raise HTTPException(status_code=404, detail="Adres bulunamadi")

    il, ilce, mahalle, kapi_no, bina_no, cadde, d_d, b_d, r_d, source_level = row

    parts = [p for p in [mahalle, cadde, bina_no, ilce, il] if p]
    adres = ", ".join(parts) if parts else None

    confidence = 0.2
    if source_level == "door":
        confidence = 0.95
    elif source_level == "building":
        confidence = 0.8
    elif source_level == "road":
        confidence = 0.6

    return {
        "il": il,
        "ilce": ilce,
        "mahalle": mahalle,
        "cadde": cadde,
        "bina_no": bina_no,
        "kapi_no": kapi_no,
        "adres": adres,
        "source_level": source_level,
        "confidence": confidence,
        "distance_m": {
            "door": d_d,
            "building": b_d,
            "road": r_d,
        },
        "query_params": {
            "door_radius_m": door_radius_m,
            "building_radius_m": building_radius_m,
            "road_radius_m": road_radius_m,
            "metric": metric,
        },
    }
