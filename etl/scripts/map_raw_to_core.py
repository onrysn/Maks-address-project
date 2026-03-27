import json
import os
import time
from pathlib import Path
from typing import Any

import psycopg
from psycopg import sql


def q_ident(name: str) -> sql.Identifier:
    return sql.Identifier(name)



def log(msg: str) -> None:
    now = time.strftime("%H:%M:%S")
    print(f"[{now}] {msg}", flush=True)


def log_counts(cur: psycopg.Cursor) -> None:
    cur.execute(
        """
        SELECT 'admin_city', count(*) FROM admin_city
        UNION ALL SELECT 'admin_district', count(*) FROM admin_district
        UNION ALL SELECT 'admin_neighborhood', count(*) FROM admin_neighborhood
        UNION ALL SELECT 'roads', count(*) FROM roads
        UNION ALL SELECT 'buildings', count(*) FROM buildings
        UNION ALL SELECT 'doors', count(*) FROM doors
        """
    )
    rows = cur.fetchall()
    summary = ", ".join([f"{name}={cnt}" for name, cnt in rows])
    log(f"Core counts: {summary}")
def load_config(path: str) -> dict[str, Any]:
    return json.loads(Path(path).read_text(encoding="utf-8-sig"))


def run_stmt(cur: psycopg.Cursor, stmt: sql.Composed, params: tuple[Any, ...] | None = None) -> None:
    if params is None:
        cur.execute(stmt)
    else:
        cur.execute(stmt, params)


def truncate_core(cur: psycopg.Cursor) -> None:
    cur.execute(
        """
        TRUNCATE TABLE
          doors,
          buildings,
          roads,
          admin_neighborhood,
          admin_district,
          admin_city
        RESTART IDENTITY CASCADE;
        """
    )


def ensure_norm_func(cur: psycopg.Cursor) -> None:
    cur.execute(
        """
        CREATE OR REPLACE FUNCTION public.norm_maks_id(val text)
        RETURNS text
        LANGUAGE sql
        IMMUTABLE
        AS $$
          SELECT NULLIF(
            UPPER(
              REGEXP_REPLACE(
                BTRIM(val),
                '\\.0+$',
                ''
              )
            ),
            ''
          )
        $$;
        """
    )


def table_columns(cur: psycopg.Cursor, schema: str, table: str) -> set[str]:
    cur.execute(
        """
        SELECT column_name
        FROM information_schema.columns
        WHERE table_schema = %s AND table_name = %s
        """,
        (schema, table),
    )
    return {r[0].lower() for r in cur.fetchall()}


def ensure_table_exists(cur: psycopg.Cursor, schema: str, table: str) -> None:
    cur.execute("SELECT to_regclass(%s)", (f"{schema}.{table}",))
    if cur.fetchone()[0] is None:
        raise RuntimeError(f"Raw table not found: {schema}.{table}")


def pick_col(cols: set[str], candidates: list[str], label: str, table: str) -> str:
    for c in candidates:
        if c.lower() in cols:
            return c.lower()
    raise RuntimeError(f"Column not found for {table}.{label}. Tried: {', '.join(candidates)}")


def insert_city(cur: psycopg.Cursor, cfg: dict[str, Any], raw_schema: str) -> None:
    run_stmt(
        cur,
        sql.SQL(
            """
            INSERT INTO admin_city (city_code, city_name, geom)
            SELECT
              CAST({code_col} AS text),
              CAST({name_col} AS text),
              ST_Multi(ST_CollectionExtract(ST_MakeValid({geom_col}), 3))::geometry(MultiPolygon, 4326)
            FROM {raw_schema}.{table}
            WHERE {geom_col} IS NOT NULL;
            """
        ).format(
            raw_schema=q_ident(raw_schema),
            table=q_ident(cfg["table"]),
            code_col=q_ident(cfg["code_col"]),
            name_col=q_ident(cfg["name_col"]),
            geom_col=q_ident(cfg["geom_col"]),
        ),
    )


def insert_district(cur: psycopg.Cursor, cfg: dict[str, Any], raw_schema: str) -> None:
    run_stmt(
        cur,
        sql.SQL(
            """
            INSERT INTO admin_district (district_code, district_name, city_id, geom)
            SELECT
              CAST(d.{code_col} AS text),
              CAST(d.{name_col} AS text),
              c.id,
              ST_Multi(ST_CollectionExtract(ST_MakeValid(d.{geom_col}), 3))::geometry(MultiPolygon, 4326)
            FROM {raw_schema}.{table} d
            LEFT JOIN admin_city c ON c.city_code = CAST(d.{city_code_col} AS text)
            WHERE d.{geom_col} IS NOT NULL;
            """
        ).format(
            raw_schema=q_ident(raw_schema),
            table=q_ident(cfg["table"]),
            code_col=q_ident(cfg["code_col"]),
            name_col=q_ident(cfg["name_col"]),
            city_code_col=q_ident(cfg["city_code_col"]),
            geom_col=q_ident(cfg["geom_col"]),
        ),
    )


def insert_neighborhood(cur: psycopg.Cursor, cfg: dict[str, Any], raw_schema: str) -> None:
    run_stmt(
        cur,
        sql.SQL(
            """
            INSERT INTO admin_neighborhood (neighborhood_code, neighborhood_name, district_id, geom)
            SELECT
              CAST(n.{code_col} AS text),
              CAST(n.{name_col} AS text),
              d.id,
              ST_Multi(ST_CollectionExtract(ST_MakeValid(n.{geom_col}), 3))::geometry(MultiPolygon, 4326)
            FROM {raw_schema}.{table} n
            LEFT JOIN admin_district d ON d.district_code = CAST(n.{district_code_col} AS text)
            WHERE n.{geom_col} IS NOT NULL;
            """
        ).format(
            raw_schema=q_ident(raw_schema),
            table=q_ident(cfg["table"]),
            code_col=q_ident(cfg["code_col"]),
            name_col=q_ident(cfg["name_col"]),
            district_code_col=q_ident(cfg["district_code_col"]),
            geom_col=q_ident(cfg["geom_col"]),
        ),
    )


def insert_road(cur: psycopg.Cursor, cfg: dict[str, Any], raw_schema: str) -> None:
    run_stmt(
        cur,
        sql.SQL(
            """
            INSERT INTO roads (road_code, road_name, road_type, neighborhood_id, geom)
            SELECT
              CAST(r.{code_col} AS text),
              CAST(r.{name_col} AS text),
              CAST(r.{type_col} AS text),
              n.id,
              ST_Multi(ST_CollectionExtract(ST_MakeValid(r.{geom_col}), 2))::geometry(MultiLineString, 4326)
            FROM {raw_schema}.{table} r
            LEFT JOIN admin_neighborhood n ON n.neighborhood_code = CAST(r.{neighborhood_code_col} AS text)
            WHERE r.{geom_col} IS NOT NULL;
            """
        ).format(
            raw_schema=q_ident(raw_schema),
            table=q_ident(cfg["table"]),
            code_col=q_ident(cfg["code_col"]),
            name_col=q_ident(cfg["name_col"]),
            type_col=q_ident(cfg["type_col"]),
            neighborhood_code_col=q_ident(cfg["neighborhood_code_col"]),
            geom_col=q_ident(cfg["geom_col"]),
        ),
    )


def insert_building(cur: psycopg.Cursor, cfg: dict[str, Any], raw_schema: str) -> None:
    run_stmt(
        cur,
        sql.SQL(
            """
            INSERT INTO buildings (building_code, building_no, road_id, neighborhood_id, geom)
            SELECT
              CAST(b.{code_col} AS text),
              CAST(b.{number_col} AS text),
              r.id,
              n.id,
              ST_Multi(ST_CollectionExtract(ST_MakeValid(b.{geom_col}), 3))::geometry(MultiPolygon, 4326)
            FROM {raw_schema}.{table} b
            LEFT JOIN roads r ON r.road_code = CAST(b.{road_code_col} AS text)
            LEFT JOIN admin_neighborhood n ON n.neighborhood_code = CAST(b.{neighborhood_code_col} AS text)
            WHERE b.{geom_col} IS NOT NULL;
            """
        ).format(
            raw_schema=q_ident(raw_schema),
            table=q_ident(cfg["table"]),
            code_col=q_ident(cfg["code_col"]),
            number_col=q_ident(cfg["number_col"]),
            road_code_col=q_ident(cfg["road_code_col"]),
            neighborhood_code_col=q_ident(cfg["neighborhood_code_col"]),
            geom_col=q_ident(cfg["geom_col"]),
        ),
    )


def insert_door(cur: psycopg.Cursor, cfg: dict[str, Any], raw_schema: str) -> None:
    run_stmt(
        cur,
        sql.SQL(
            """
            INSERT INTO doors (door_code, external_no, building_id, geom)
            SELECT
              CAST(d.{code_col} AS text),
              CAST(d.{external_no_col} AS text),
              b.id,
              ST_SetSRID(
                ST_GeometryN(ST_CollectionExtract(ST_MakeValid(d.{geom_col}), 1), 1),
                4326
              )::geometry(Point, 4326)
            FROM {raw_schema}.{table} d
            LEFT JOIN buildings b ON b.building_code = CAST(d.{building_code_col} AS text)
            WHERE d.{geom_col} IS NOT NULL;
            """
        ).format(
            raw_schema=q_ident(raw_schema),
            table=q_ident(cfg["table"]),
            code_col=q_ident(cfg["code_col"]),
            external_no_col=q_ident(cfg["external_no_col"]),
            building_code_col=q_ident(cfg["building_code_col"]),
            geom_col=q_ident(cfg["geom_col"]),
        ),
    )


def map_konya_v1(cur: psycopg.Cursor, raw_schema: str, opts: dict[str, Any]) -> None:
    log("Konya preset mapping started")
    ensure_norm_func(cur)

    needed_tables = ["il", "ilce", "mahalle", "yolortahat", "yolortahatyon", "yol", "yapi", "numarataj"]
    for t in needed_tables:
        ensure_table_exists(cur, raw_schema, t)
    log("Raw tables validated")

    il_cols = table_columns(cur, raw_schema, "il")
    ilce_cols = table_columns(cur, raw_schema, "ilce")
    mah_cols = table_columns(cur, raw_schema, "mahalle")
    yol_cols = table_columns(cur, raw_schema, "yol")
    yoh_cols = table_columns(cur, raw_schema, "yolortahat")
    yohyon_cols = table_columns(cur, raw_schema, "yolortahatyon")
    yapi_cols = table_columns(cur, raw_schema, "yapi")
    num_cols = table_columns(cur, raw_schema, "numarataj")

    il_id_col = pick_col(il_cols, ["id", "id_1", "globalid"], "id", "il")
    il_name_col = pick_col(il_cols, ["ad", "name"], "name", "il")
    ilce_id_col = pick_col(ilce_cols, ["id", "id_1", "globalid"], "id", "ilce")
    ilce_name_col = pick_col(ilce_cols, ["ad", "name"], "name", "ilce")
    ilce_il_fk_col = pick_col(ilce_cols, ["ilid", "il_id"], "il fk", "ilce")
    mah_id_col = pick_col(mah_cols, ["id", "id_1", "globalid"], "id", "mahalle")
    mah_name_col = pick_col(mah_cols, ["ad", "name"], "name", "mahalle")
    mah_ilce_fk_col = pick_col(mah_cols, ["ilceid", "ilce_id"], "ilce fk", "mahalle")

    yoh_id_col = pick_col(yoh_cols, ["id", "id_1", "globalid"], "id", "yolortahat")
    yoh_yol_fk_col = pick_col(yoh_cols, ["yolid", "yol_id"], "yol fk", "yolortahat")
    yoh_name_col = pick_col(yoh_cols, ["ad", "name"], "name", "yolortahat")
    yoh_batch_col = pick_col(yoh_cols, ["objectid_", "objectid"], "batch key", "yolortahat")

    yol_id_col = pick_col(yol_cols, ["id", "id_1", "globalid"], "id", "yol")
    yol_name_col = pick_col(yol_cols, ["ad", "name"], "name", "yol")
    yol_type_col = pick_col(yol_cols, ["tip", "type"], "type", "yol")

    yohyon_id_col = pick_col(yohyon_cols, ["id", "id_1", "globalid"], "id", "yolortahatyon")
    yohyon_yoh_fk_col = pick_col(yohyon_cols, ["yolortahatid", "yolortahat_id"], "yolortahat fk", "yolortahatyon")
    yohyon_mah_fk_col = pick_col(yohyon_cols, ["mahalleid", "mahalle_id"], "mahalle fk", "yolortahatyon")

    yapi_id_col = pick_col(yapi_cols, ["id", "id_1", "globalid"], "id", "yapi")
    yapi_no_col = opts.get("building_no_col") or pick_col(yapi_cols, ["ad", "binano", "bina_no"], "building no", "yapi")
    yapi_batch_col = pick_col(yapi_cols, ["objectid_", "objectid"], "batch key", "yapi")

    num_id_col = pick_col(num_cols, ["id", "id_1", "globalid"], "id", "numarataj")
    num_yapi_fk_col = pick_col(num_cols, ["yapiid", "yapi_id"], "yapi fk", "numarataj")
    num_yohyon_fk_col = pick_col(num_cols, ["yolortahatyonid", "yolortahatyon_id"], "yolortahatyon fk", "numarataj")
    num_kapi_col = pick_col(num_cols, ["kapino", "tasarimkapino"], "kapi no", "numarataj")
    num_batch_col = pick_col(num_cols, ["objectid_", "objectid"], "batch key", "numarataj")

    road_batch_size = int(opts.get("road_batch_size", 5000))
    building_batch_size = int(opts.get("building_batch_size", 5000))
    door_batch_size = int(opts.get("door_batch_size", 5000))

    log("Creating helper indexes on raw tables...")
    run_stmt(cur, sql.SQL("CREATE INDEX IF NOT EXISTS idx_raw_ilce_il_fk ON {s}.ilce ({c})").format(s=q_ident(raw_schema), c=q_ident(ilce_il_fk_col)))
    run_stmt(cur, sql.SQL("CREATE INDEX IF NOT EXISTS idx_raw_mahalle_ilce_fk ON {s}.mahalle ({c})").format(s=q_ident(raw_schema), c=q_ident(mah_ilce_fk_col)))
    run_stmt(cur, sql.SQL("CREATE INDEX IF NOT EXISTS idx_raw_yolortahat_id ON {s}.yolortahat ({c})").format(s=q_ident(raw_schema), c=q_ident(yoh_id_col)))
    run_stmt(cur, sql.SQL("CREATE INDEX IF NOT EXISTS idx_raw_yolortahat_yol_fk ON {s}.yolortahat ({c})").format(s=q_ident(raw_schema), c=q_ident(yoh_yol_fk_col)))
    run_stmt(cur, sql.SQL("CREATE INDEX IF NOT EXISTS idx_raw_yol_id ON {s}.yol ({c})").format(s=q_ident(raw_schema), c=q_ident(yol_id_col)))
    run_stmt(cur, sql.SQL("CREATE INDEX IF NOT EXISTS idx_raw_yohyon_id ON {s}.yolortahatyon ({c})").format(s=q_ident(raw_schema), c=q_ident(yohyon_id_col)))
    run_stmt(cur, sql.SQL("CREATE INDEX IF NOT EXISTS idx_raw_yohyon_yoh_fk ON {s}.yolortahatyon ({c})").format(s=q_ident(raw_schema), c=q_ident(yohyon_yoh_fk_col)))
    run_stmt(cur, sql.SQL("CREATE INDEX IF NOT EXISTS idx_raw_yohyon_mah_fk ON {s}.yolortahatyon ({c})").format(s=q_ident(raw_schema), c=q_ident(yohyon_mah_fk_col)))
    run_stmt(cur, sql.SQL("CREATE INDEX IF NOT EXISTS idx_raw_yapi_id ON {s}.yapi ({c})").format(s=q_ident(raw_schema), c=q_ident(yapi_id_col)))
    run_stmt(cur, sql.SQL("CREATE INDEX IF NOT EXISTS idx_raw_numarataj_yapi_fk ON {s}.numarataj ({c})").format(s=q_ident(raw_schema), c=q_ident(num_yapi_fk_col)))
    run_stmt(cur, sql.SQL("CREATE INDEX IF NOT EXISTS idx_raw_numarataj_yohyon_fk ON {s}.numarataj ({c})").format(s=q_ident(raw_schema), c=q_ident(num_yohyon_fk_col)))
    run_stmt(cur, sql.SQL("CREATE INDEX IF NOT EXISTS idx_raw_yapi_geom ON {s}.yapi USING GIST (geom)").format(s=q_ident(raw_schema)))
    run_stmt(cur, sql.SQL("CREATE INDEX IF NOT EXISTS idx_raw_mahalle_geom ON {s}.mahalle USING GIST (geom)").format(s=q_ident(raw_schema)))

    log("Step 1/6: admin_city")
    run_stmt(cur, sql.SQL("""
        INSERT INTO admin_city (city_code, city_name, geom)
        SELECT norm_maks_id(CAST(i.{il_id} AS text)), CAST(i.{il_name} AS text),
               ST_Multi(ST_CollectionExtract(ST_MakeValid(i.geom), 3))::geometry(MultiPolygon, 4326)
        FROM {raw_schema}.il i
        WHERE i.geom IS NOT NULL
    """).format(raw_schema=q_ident(raw_schema), il_id=q_ident(il_id_col), il_name=q_ident(il_name_col)))

    log("Step 2/6: admin_district")
    run_stmt(cur, sql.SQL("""
        INSERT INTO admin_district (district_code, district_name, city_id, geom)
        SELECT norm_maks_id(CAST(d.{ilce_id} AS text)), CAST(d.{ilce_name} AS text), c.id,
               ST_Multi(ST_CollectionExtract(ST_MakeValid(d.geom), 3))::geometry(MultiPolygon, 4326)
        FROM {raw_schema}.ilce d
        LEFT JOIN admin_city c ON norm_maks_id(c.city_code) = norm_maks_id(CAST(d.{ilce_il_fk} AS text))
        WHERE d.geom IS NOT NULL
    """).format(raw_schema=q_ident(raw_schema), ilce_id=q_ident(ilce_id_col), ilce_name=q_ident(ilce_name_col), ilce_il_fk=q_ident(ilce_il_fk_col)))

    log("Step 3/6: admin_neighborhood")
    run_stmt(cur, sql.SQL("""
        INSERT INTO admin_neighborhood (neighborhood_code, neighborhood_name, district_id, geom)
        SELECT norm_maks_id(CAST(m.{mah_id} AS text)), CAST(m.{mah_name} AS text), d.id,
               ST_Multi(ST_CollectionExtract(ST_MakeValid(m.geom), 3))::geometry(MultiPolygon, 4326)
        FROM {raw_schema}.mahalle m
        LEFT JOIN admin_district d ON norm_maks_id(d.district_code) = norm_maks_id(CAST(m.{mah_ilce_fk} AS text))
        WHERE m.geom IS NOT NULL
    """).format(raw_schema=q_ident(raw_schema), mah_id=q_ident(mah_id_col), mah_name=q_ident(mah_name_col), mah_ilce_fk=q_ident(mah_ilce_fk_col)))

    log(f"Step 4/6: roads batching started (batch_size={road_batch_size})")
    cur.execute(sql.SQL("SELECT MIN(CAST({b} AS bigint)), MAX(CAST({b} AS bigint)) FROM {s}.yolortahat WHERE geom IS NOT NULL").format(s=q_ident(raw_schema), b=q_ident(yoh_batch_col)))
    roads_min, roads_max = cur.fetchone()
    if roads_min is not None and roads_max is not None:
        current = int(roads_min)
        end_max = int(roads_max)
        while current <= end_max:
            nxt = current + road_batch_size
            run_stmt(cur, sql.SQL("""
                INSERT INTO roads (road_code, road_name, road_type, neighborhood_id, geom)
                SELECT norm_maks_id(CAST(yh.{yoh_id} AS text)),
                       COALESCE(CAST(y.{yol_name} AS text), CAST(yh.{yoh_name} AS text)),
                       CAST(y.{yol_type} AS text),
                       n.id,
                       ST_Multi(ST_CollectionExtract(ST_MakeValid(yh.geom), 2))::geometry(MultiLineString, 4326)
                FROM {raw_schema}.yolortahat yh
                LEFT JOIN {raw_schema}.yol y
                  ON norm_maks_id(CAST(y.{yol_id} AS text)) = norm_maks_id(CAST(yh.{yoh_yol_fk} AS text))
                LEFT JOIN LATERAL (
                  SELECT yy.{yohyon_mah_fk} AS mahalle_id
                  FROM {raw_schema}.yolortahatyon yy
                  WHERE norm_maks_id(CAST(yy.{yohyon_yoh_fk} AS text)) = norm_maks_id(CAST(yh.{yoh_id} AS text))
                    AND yy.{yohyon_mah_fk} IS NOT NULL
                  LIMIT 1
                ) ym ON TRUE
                LEFT JOIN admin_neighborhood n
                  ON norm_maks_id(n.neighborhood_code) = norm_maks_id(CAST(ym.mahalle_id AS text))
                WHERE yh.geom IS NOT NULL
                  AND CAST(yh.{yoh_batch} AS bigint) >= %s
                  AND CAST(yh.{yoh_batch} AS bigint) < %s
            """).format(
                raw_schema=q_ident(raw_schema),
                yoh_id=q_ident(yoh_id_col),
                yoh_name=q_ident(yoh_name_col),
                yoh_yol_fk=q_ident(yoh_yol_fk_col),
                yol_id=q_ident(yol_id_col),
                yol_name=q_ident(yol_name_col),
                yol_type=q_ident(yol_type_col),
                yohyon_yoh_fk=q_ident(yohyon_yoh_fk_col),
                yohyon_mah_fk=q_ident(yohyon_mah_fk_col),
                yoh_batch=q_ident(yoh_batch_col),
            ), (current, nxt))
            inserted = 0 if cur.rowcount is None or cur.rowcount < 0 else cur.rowcount
            log(f"Roads batch [{current}, {nxt}) inserted={inserted}")
            current = nxt

    log("Building relation staging from MAKS chain...")
    run_stmt(cur, sql.SQL("""
        CREATE TEMP TABLE tmp_yapi_rel ON COMMIT DROP AS
        SELECT
          norm_maks_id(CAST(nm.{num_yapi_fk} AS text)) AS yapi_code,
          norm_maks_id(CAST(yh.{yoh_id} AS text)) AS yoh_code,
          norm_maks_id(CAST(yy.{yohyon_mah_fk} AS text)) AS mahalle_code,
          COUNT(*) AS cnt,
          ROW_NUMBER() OVER (
            PARTITION BY norm_maks_id(CAST(nm.{num_yapi_fk} AS text))
            ORDER BY COUNT(*) DESC, norm_maks_id(CAST(yh.{yoh_id} AS text))
          ) AS rn
        FROM {raw_schema}.numarataj nm
        JOIN {raw_schema}.yolortahatyon yy
          ON norm_maks_id(CAST(yy.{yohyon_id} AS text)) = norm_maks_id(CAST(nm.{num_yohyon_fk} AS text))
        JOIN {raw_schema}.yolortahat yh
          ON norm_maks_id(CAST(yh.{yoh_id} AS text)) = norm_maks_id(CAST(yy.{yohyon_yoh_fk} AS text))
        WHERE nm.{num_yapi_fk} IS NOT NULL AND yh.{yoh_id} IS NOT NULL
        GROUP BY norm_maks_id(CAST(nm.{num_yapi_fk} AS text)), norm_maks_id(CAST(yh.{yoh_id} AS text)), norm_maks_id(CAST(yy.{yohyon_mah_fk} AS text))
    """).format(
        raw_schema=q_ident(raw_schema),
        num_yapi_fk=q_ident(num_yapi_fk_col),
        num_yohyon_fk=q_ident(num_yohyon_fk_col),
        yohyon_id=q_ident(yohyon_id_col),
        yohyon_yoh_fk=q_ident(yohyon_yoh_fk_col),
        yohyon_mah_fk=q_ident(yohyon_mah_fk_col),
        yoh_id=q_ident(yoh_id_col),
    ))
    run_stmt(cur, sql.SQL("CREATE INDEX idx_tmp_yapi_rel_yapi_rn ON tmp_yapi_rel(yapi_code, rn)"))

    log(f"Step 5/6: buildings batching started (batch_size={building_batch_size})")
    cur.execute(sql.SQL("SELECT MIN(CAST({b} AS bigint)), MAX(CAST({b} AS bigint)) FROM {s}.yapi WHERE geom IS NOT NULL").format(s=q_ident(raw_schema), b=q_ident(yapi_batch_col)))
    bmin, bmax = cur.fetchone()
    if bmin is not None and bmax is not None:
        current = int(bmin)
        end_max = int(bmax)
        while current <= end_max:
            nxt = current + building_batch_size
            cur.execute(sql.SQL("""
                WITH src AS (
                  SELECT
                    norm_maks_id(CAST(b.{yapi_id} AS text)) AS building_code,
                    CAST(b.{yapi_no} AS text) AS building_no,
                    r.id AS road_id,
                    COALESCE(n_rel.id, nn.id) AS neighborhood_id,
                    ST_Multi(ST_CollectionExtract(ST_MakeValid(b.geom), 3))::geometry(MultiPolygon, 4326) AS geom
                  FROM {raw_schema}.yapi b
                  LEFT JOIN tmp_yapi_rel yr
                    ON yr.yapi_code = norm_maks_id(CAST(b.{yapi_id} AS text))
                   AND yr.rn = 1
                  LEFT JOIN roads r ON norm_maks_id(r.road_code) = yr.yoh_code
                  LEFT JOIN admin_neighborhood n_rel ON norm_maks_id(n_rel.neighborhood_code) = yr.mahalle_code
                  LEFT JOIN LATERAL (
                    SELECT n.id
                    FROM admin_neighborhood n
                    WHERE ST_Intersects(n.geom, ST_PointOnSurface(b.geom))
                    ORDER BY n.geom <-> ST_PointOnSurface(b.geom)
                    LIMIT 1
                  ) nn ON TRUE
                  WHERE b.geom IS NOT NULL
                    AND CAST(b.{yapi_batch} AS bigint) >= %s
                    AND CAST(b.{yapi_batch} AS bigint) < %s
                ),
                ins AS (
                  INSERT INTO buildings (building_code, building_no, road_id, neighborhood_id, geom)
                  SELECT building_code, building_no, road_id, neighborhood_id, geom FROM src
                  RETURNING road_id
                )
                SELECT
                  (SELECT COUNT(*) FROM ins) AS inserted,
                  (SELECT COUNT(*) FROM ins WHERE road_id IS NOT NULL) AS with_road
            """).format(
                raw_schema=q_ident(raw_schema),
                yapi_id=q_ident(yapi_id_col),
                yapi_no=q_ident(yapi_no_col),
                yapi_batch=q_ident(yapi_batch_col),
            ), (current, nxt))
            inserted, with_road = cur.fetchone()
            log(f"Buildings batch [{current}, {nxt}) inserted={inserted}, with_road_from_rel={with_road}")
            current = nxt

    log(f"Step 6/6: doors batching started (batch_size={door_batch_size})")
    cur.execute(sql.SQL("SELECT MIN(CAST({b} AS bigint)), MAX(CAST({b} AS bigint)) FROM {s}.numarataj WHERE geom IS NOT NULL").format(s=q_ident(raw_schema), b=q_ident(num_batch_col)))
    min_id, max_id = cur.fetchone()
    if min_id is None or max_id is None:
        log("Door batching skipped: no numarataj geometry rows.")
    else:
        current = int(min_id)
        end_max = int(max_id)
        while current <= end_max:
            nxt = current + door_batch_size
            run_stmt(cur, sql.SQL("""
                INSERT INTO doors (door_code, external_no, building_id, geom)
                SELECT
                  norm_maks_id(CAST(nm.{num_id} AS text)),
                  CAST(nm.{num_kapi} AS text),
                  b.id,
                  ST_SetSRID(ST_GeometryN(ST_CollectionExtract(ST_MakeValid(nm.geom), 1), 1), 4326)::geometry(Point, 4326)
                FROM {raw_schema}.numarataj nm
                LEFT JOIN buildings b ON norm_maks_id(b.building_code) = norm_maks_id(CAST(nm.{num_yapi_fk} AS text))
                WHERE nm.geom IS NOT NULL
                  AND CAST(nm.{num_batch} AS bigint) >= %s
                  AND CAST(nm.{num_batch} AS bigint) < %s
            """).format(
                raw_schema=q_ident(raw_schema),
                num_id=q_ident(num_id_col),
                num_kapi=q_ident(num_kapi_col),
                num_yapi_fk=q_ident(num_yapi_fk_col),
                num_batch=q_ident(num_batch_col),
            ), (current, nxt))
            inserted = 0 if cur.rowcount is None or cur.rowcount < 0 else cur.rowcount
            log(f"Doors batch [{current}, {nxt}) inserted={inserted}")
            current = nxt

def map_generic(cur: psycopg.Cursor, cfg: dict[str, Any], raw_schema: str) -> None:
    insert_city(cur, cfg["city"], raw_schema)
    insert_district(cur, cfg["district"], raw_schema)
    insert_neighborhood(cur, cfg["neighborhood"], raw_schema)
    insert_road(cur, cfg["road"], raw_schema)
    insert_building(cur, cfg["building"], raw_schema)
    insert_door(cur, cfg["door"], raw_schema)


def main() -> None:
    cfg_path = os.getenv("MAPPING_FILE", "/etl/config/mapping.json")
    cfg = load_config(cfg_path)
    raw_schema = cfg.get("raw_schema", "raw_maks")
    log(f"Mapping started (raw_schema={raw_schema})")

    conn = psycopg.connect(
        host=os.getenv("POSTGRES_HOST", "db"),
        port=int(os.getenv("POSTGRES_PORT", "5432")),
        dbname=os.getenv("POSTGRES_DB", "maks"),
        user=os.getenv("POSTGRES_USER", "maks"),
        password=os.getenv("POSTGRES_PASSWORD", "maks"),
    )

    try:
        with conn:
            with conn.cursor() as cur:
                if cfg.get("clear_core_tables", True):
                    log("Clearing core tables...")
                    truncate_core(cur)

                preset = (cfg.get("preset") or "").lower()
                if preset == "konya_maks_v1":
                    log("Using preset: konya_maks_v1")
                    map_konya_v1(cur, raw_schema, cfg.get("konya", {}))
                else:
                    log("Using generic mapping config")
                    map_generic(cur, cfg, raw_schema)
                log_counts(cur)

        log("Core mapping completed successfully.")
    finally:
        conn.close()


if __name__ == "__main__":
    main()




