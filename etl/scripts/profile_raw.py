import json
import os
from pathlib import Path

import psycopg


DEFAULT_OUT = "/data/processed/raw_profile.json"


def fetch_profile(conn: psycopg.Connection, schema: str = "raw_maks") -> dict:
    profile = {"schema": schema, "tables": []}

    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT table_name
            FROM information_schema.tables
            WHERE table_schema = %s
            ORDER BY table_name;
            """,
            (schema,),
        )
        tables = [r[0] for r in cur.fetchall()]

        for table in tables:
            cur.execute(
                """
                SELECT column_name, data_type
                FROM information_schema.columns
                WHERE table_schema = %s AND table_name = %s
                ORDER BY ordinal_position;
                """,
                (schema, table),
            )
            columns = [{"name": c, "type": t} for c, t in cur.fetchall()]

            cur.execute(
                """
                SELECT type
                FROM geometry_columns
                WHERE f_table_schema = %s AND f_table_name = %s;
                """,
                (schema, table),
            )
            geom = cur.fetchone()
            geom_type = geom[0] if geom else None

            profile["tables"].append(
                {
                    "table": table,
                    "geometry_type": geom_type,
                    "columns": columns,
                }
            )

    return profile


def main() -> None:
    out_path = Path(os.getenv("RAW_PROFILE_OUT", DEFAULT_OUT))
    out_path.parent.mkdir(parents=True, exist_ok=True)

    conn = psycopg.connect(
        host=os.getenv("POSTGRES_HOST", "db"),
        port=int(os.getenv("POSTGRES_PORT", "5432")),
        dbname=os.getenv("POSTGRES_DB", "maks"),
        user=os.getenv("POSTGRES_USER", "maks"),
        password=os.getenv("POSTGRES_PASSWORD", "maks"),
    )
    try:
        profile = fetch_profile(conn, schema=os.getenv("RAW_SCHEMA", "raw_maks"))
    finally:
        conn.close()

    out_path.write_text(json.dumps(profile, ensure_ascii=False, indent=2), encoding="utf-8")
    print(f"Raw profile written to: {out_path}")


if __name__ == "__main__":
    main()
