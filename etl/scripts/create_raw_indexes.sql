-- Spatial indexes used by reverse geocoding query paths.
CREATE INDEX IF NOT EXISTS idx_raw_il_geom ON raw_maks.il USING GIST (geom);
CREATE INDEX IF NOT EXISTS idx_raw_ilce_geom ON raw_maks.ilce USING GIST (geom);
CREATE INDEX IF NOT EXISTS idx_raw_koy_geom ON raw_maks.koy USING GIST (geom);
CREATE INDEX IF NOT EXISTS idx_raw_mahalle_geom ON raw_maks.mahalle USING GIST (geom);
CREATE INDEX IF NOT EXISTS idx_raw_numarataj_geom ON raw_maks.numarataj USING GIST (geom);
CREATE INDEX IF NOT EXISTS idx_raw_yapi_geom ON raw_maks.yapi USING GIST (geom);
CREATE INDEX IF NOT EXISTS idx_raw_yolortahat_geom ON raw_maks.yolortahat USING GIST (geom);

-- Geography expression indexes to accelerate geodesic ST_DWithin/ST_Distance paths.
CREATE INDEX IF NOT EXISTS idx_raw_numarataj_geog ON raw_maks.numarataj USING GIST ((geom::geography));
CREATE INDEX IF NOT EXISTS idx_raw_yapi_geog ON raw_maks.yapi USING GIST ((geom::geography));
CREATE INDEX IF NOT EXISTS idx_raw_yolortahat_geog ON raw_maks.yolortahat USING GIST ((geom::geography));

-- EPSG:3857 transform indexes for planar metric queries.
CREATE INDEX IF NOT EXISTS idx_raw_numarataj_3857 ON raw_maks.numarataj USING GIST ((ST_Transform(geom, 3857)));
CREATE INDEX IF NOT EXISTS idx_raw_yapi_3857 ON raw_maks.yapi USING GIST ((ST_Transform(geom, 3857)));
CREATE INDEX IF NOT EXISTS idx_raw_yolortahat_3857 ON raw_maks.yolortahat USING GIST ((ST_Transform(geom, 3857)));

-- Expression indexes for normalized join keys used in yapi_road / nearest_road joins.
CREATE INDEX IF NOT EXISTS idx_raw_numarataj_yapi_norm ON raw_maks.numarataj (
  UPPER(REGEXP_REPLACE(BTRIM(CAST(yapiid AS text)), '\.0+$', ''))
);
CREATE INDEX IF NOT EXISTS idx_raw_numarataj_yhy_norm ON raw_maks.numarataj (
  UPPER(REGEXP_REPLACE(BTRIM(CAST(yolortahatyonid AS text)), '\.0+$', ''))
);
CREATE INDEX IF NOT EXISTS idx_raw_yhy_id_norm ON raw_maks.yolortahatyon (
  UPPER(REGEXP_REPLACE(BTRIM(CAST(id AS text)), '\.0+$', ''))
);
CREATE INDEX IF NOT EXISTS idx_raw_yhy_yoh_norm ON raw_maks.yolortahatyon (
  UPPER(REGEXP_REPLACE(BTRIM(CAST(yolortahatid AS text)), '\.0+$', ''))
);
CREATE INDEX IF NOT EXISTS idx_raw_yoh_id_norm ON raw_maks.yolortahat (
  UPPER(REGEXP_REPLACE(BTRIM(CAST(id AS text)), '\.0+$', ''))
);
CREATE INDEX IF NOT EXISTS idx_raw_yoh_yol_norm ON raw_maks.yolortahat (
  UPPER(REGEXP_REPLACE(BTRIM(CAST(yolid AS text)), '\.0+$', ''))
);
CREATE INDEX IF NOT EXISTS idx_raw_yol_id_norm ON raw_maks.yol (
  UPPER(REGEXP_REPLACE(BTRIM(CAST(id AS text)), '\.0+$', ''))
);
