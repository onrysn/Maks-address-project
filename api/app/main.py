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
    norm_tpl = "UPPER(REGEXP_REPLACE(BTRIM(CAST({expr} AS text)), '\\.0+$', ''))"

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
                    i.ad AS il,
                    d.ad AS ilce,
                    m.ad AS mahalle
                  FROM p
                  LEFT JOIN raw_maks.mahalle m ON ST_Contains(m.geom, p.geom)
                  LEFT JOIN raw_maks.ilce d
                    ON {norm_m_district} = {norm_d_id}
                  LEFT JOIN raw_maks.il i
                    ON {norm_d_city} = {norm_i_id}
                  LIMIT 1
                ),
                nearest_door AS (
                  SELECT
                    nm.id AS door_id,
                    nm.kapino AS kapi_no,
                    nm.yapiid AS yapi_id,
                    {door_dist} AS dist_m
                  FROM raw_maks.numarataj nm, p
                  WHERE {door_within}
                  ORDER BY nm.geom <-> p.geom
                  LIMIT 1
                ),
                nearest_yapi AS (
                  SELECT
                    y.id AS yapi_id,
                    y.ad AS bina_no,
                    {building_dist} AS dist_m
                  FROM raw_maks.yapi y, p
                  WHERE {building_within}
                  ORDER BY y.geom <-> p.geom
                  LIMIT 1
                ),
                selected_yapi AS (
                  SELECT
                    COALESCE(nd.yapi_id, ny.yapi_id) AS yapi_id,
                    ny.bina_no,
                    ny.dist_m AS yapi_dist_m
                  FROM nearest_door nd
                  FULL OUTER JOIN nearest_yapi ny ON TRUE
                  LIMIT 1
                ),
                yapi_road AS (
                  SELECT
                    yol.ad AS road_name,
                    COUNT(*) AS cnt
                  FROM selected_yapi sy
                  JOIN raw_maks.numarataj nm
                    ON {norm_nm_yapi} = {norm_sy_yapi}
                  JOIN raw_maks.yolortahatyon yhy
                    ON {norm_yhy_id} = {norm_nm_yhy}
                  JOIN raw_maks.yolortahat yoh
                    ON {norm_yoh_id} = {norm_yhy_yoh}
                  JOIN raw_maks.yol yol
                    ON {norm_yol_id} = {norm_yoh_yol}
                  GROUP BY yol.ad
                  ORDER BY COUNT(*) DESC, yol.ad
                  LIMIT 1
                ),
                nearest_road AS (
                  SELECT
                    yol.ad AS road_name,
                    {road_dist} AS dist_m
                  FROM raw_maks.yolortahat yoh
                  CROSS JOIN p
                  LEFT JOIN raw_maks.yol yol
                    ON {norm_yol_id} = {norm_yoh_yol}
                  WHERE {road_within}
                  ORDER BY yoh.geom <-> p.geom
                  LIMIT 1
                )
                SELECT
                  a.il,
                  a.ilce,
                  a.mahalle,
                  nd.kapi_no,
                  sy.bina_no,
                  nr.road_name AS en_yakin_cadde_sokak,
                  yr.road_name AS yapinin_bagli_oldugu_cadde_sokak,
                  nd.dist_m AS door_dist_m,
                  sy.yapi_dist_m AS building_dist_m,
                  nr.dist_m AS road_dist_m,
                  CASE
                    WHEN nd.door_id IS NOT NULL THEN 'door'
                    WHEN sy.yapi_id IS NOT NULL THEN 'building'
                    WHEN nr.road_name IS NOT NULL THEN 'road'
                    ELSE 'none'
                  END AS source_level
                FROM admin a
                LEFT JOIN nearest_door nd ON TRUE
                LEFT JOIN selected_yapi sy ON TRUE
                LEFT JOIN yapi_road yr ON TRUE
                LEFT JOIN nearest_road nr ON TRUE
                LIMIT 1
                """.format(
                pt_expr=pt_expr,
                norm_m_district=norm_tpl.format(expr="m.ilceid"),
                norm_d_id=norm_tpl.format(expr="d.id"),
                norm_d_city=norm_tpl.format(expr="d.ilid"),
                norm_i_id=norm_tpl.format(expr="i.id"),
                norm_nm_yapi=norm_tpl.format(expr="nm.yapiid"),
                norm_sy_yapi=norm_tpl.format(expr="sy.yapi_id"),
                norm_nm_yhy=norm_tpl.format(expr="nm.yolortahatyonid"),
                norm_yhy_id=norm_tpl.format(expr="yhy.id"),
                norm_yhy_yoh=norm_tpl.format(expr="yhy.yolortahatid"),
                norm_yoh_id=norm_tpl.format(expr="yoh.id"),
                norm_yoh_yol=norm_tpl.format(expr="yoh.yolid"),
                norm_yol_id=norm_tpl.format(expr="yol.id"),
                door_dist=dist.format(geom="nm.geom"),
                door_within=within.format(geom="nm.geom"),
                building_dist=dist.format(geom="y.geom"),
                building_within=within.format(geom="y.geom"),
                road_dist=dist.format(geom="yoh.geom"),
                road_within=within.format(geom="yoh.geom"),
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

    il, ilce, mahalle, kapi_no, bina_no, en_yakin_cadde_sokak, yapinin_bagli_oldugu_cadde_sokak, d_d, b_d, r_d, source_level = row

    parts = [p for p in [mahalle, en_yakin_cadde_sokak, bina_no, ilce, il] if p]
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
        "En yakın Cadde/Sokak": en_yakin_cadde_sokak,
        "Yapının bağlı olduğu Cadde/Sokak": yapinin_bagli_oldugu_cadde_sokak,
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
