-- Indeks for raske fylkes-uttrekk.
--
-- Fylkesaggregatet filtrerer på LEFT(kommunenummer, 2) IN (...). Uten en indeks
-- som matcher dette uttrykket må Postgres full-skanne hele entity-tabellen for
-- hvert kall — det er hovedgrunnen til at fylkes-uttrekk kan være tregt (og av
-- og til time ut i gatewayen med en HTML-feilside).
--
-- En funksjonell indeks på nøyaktig samme uttrykk gjør filteret indeks-vennlig,
-- så bare radene i fylket leses.
--
-- Kjør én gang mot databasen (der du har nett-tilgang til RDS):
--     psql "$DATABASE_URL" -f scripts/sql/idx_entity_kommune_prefix.sql
--
-- CONCURRENTLY gjør at bygging ikke låser tabellen for skriving. Kan ikke kjøres
-- inne i en transaksjon; kjør derfor akkurat som vist over (ikke i en BEGIN-blokk).

CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_entity_kommune_prefix
    ON entity (LEFT(COALESCE(kommunenummer::text, ''), 2));

-- Valgfritt: indeks på næringskode-prefiks hjelper sektorfiltrering i
-- topp-selskaper-uttrekket.
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_entity_naeringskode_prefix
    ON entity (LEFT(regexp_replace(COALESCE(naeringskode::text, ''), '\D', '', 'g'), 2));

-- Sørg for at LATERAL-oppslaget mot regnskap_siste er raskt (bør allerede finnes
-- via primærnøkkel, men skader ikke å sikre):
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_regnskap_siste_orgnr
    ON regnskap_siste (orgnr);
