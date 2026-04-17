-- PostgreSQL schema for shareholder snapshots by Norwegian organisation number (orgnr).
-- Designed for files with columns like:
-- Orgnr, Selskap, Aksjeklasse, Navn aksjonær, Fødselsår/orgnr,
-- Postnr/sted, Landkode, Antall aksjer, Antall aksjer selskap

CREATE TABLE IF NOT EXISTS shareholder_snapshot (
    snapshot_date date NOT NULL,
    orgnr text NOT NULL,
    company_name text,
    share_class text,
    shareholder_name text NOT NULL,
    shareholder_identifier text,
    shareholder_identifier_norm text GENERATED ALWAYS AS (COALESCE(NULLIF(shareholder_identifier, ''), '-')) STORED,
    postal_place text,
    country_code text,
    shares_owned numeric(20, 0),
    company_total_shares numeric(20, 0),
    source_file text,
    imported_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (snapshot_date, orgnr, shareholder_name, shareholder_identifier_norm)
);

CREATE INDEX IF NOT EXISTS idx_shareholder_snapshot_orgnr_date
    ON shareholder_snapshot (orgnr, snapshot_date DESC);

CREATE INDEX IF NOT EXISTS idx_shareholder_snapshot_owner_date
    ON shareholder_snapshot (shareholder_name, snapshot_date DESC);

CREATE INDEX IF NOT EXISTS idx_shareholder_snapshot_identifier_date
    ON shareholder_snapshot (shareholder_identifier, snapshot_date DESC)
    WHERE shareholder_identifier IS NOT NULL AND shareholder_identifier <> '';

CREATE INDEX IF NOT EXISTS idx_shareholder_snapshot_country
    ON shareholder_snapshot (country_code);

-- Optional helper view for latest snapshot per orgnr.
CREATE OR REPLACE VIEW shareholder_latest_per_company AS
SELECT s.*
FROM shareholder_snapshot s
JOIN (
    SELECT orgnr, MAX(snapshot_date) AS max_snapshot_date
    FROM shareholder_snapshot
    GROUP BY orgnr
) latest
  ON latest.orgnr = s.orgnr
 AND latest.max_snapshot_date = s.snapshot_date;
