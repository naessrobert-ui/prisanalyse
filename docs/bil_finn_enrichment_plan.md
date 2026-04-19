# Bil-søk med utvidede attributter + FINN-berikelse (raskt oppsett)

## Hva som nå kan brukes i søk

I tillegg til pris, km, år, drivstoff og hjuldrift, kan søk også filtreres på:

- `farge` (multivalg)
- `storrelseklasse` (multivalg)
- `personlig_skilt` (ja/nei)
- `motor_hk_min` / `motor_hk_max` (hk, med fallback fra kW til hk)
- `bruktimport` (ja/nei)
- `import_land` (multivalg)

`bruktimport` tolkes strengt:
- kun verdi `1` regnes som bruktimport
- blank/NULL/andre verdier regnes som **ikke** bruktimport

I tillegg finnes en dedikert side for bruktimport:
- `/bil/solgt/bruktimport` (forhåndsfilter `bruktimport=ja`)

## Foreslått arkitektur for «ikke tregt»

1. **Hold FINN som sanntidskilde for trefflisten**
   - Hent aktuelle annonser (id/finnkode + pris + url + sist oppdatert).
2. **Berik trefflisten med intern DB lokalt i minne**
   - Join på `finnkode` (evt. registreringsnummer hvis tilgjengelig internt).
3. **Asynkron backfill for manglende biler**
   - Kjør i bakgrunnskø (ikke i request/response) for biler som mangler intern metadata.
4. **Kort TTL-cache for FINN-søk**
   - F.eks. 30–120 sekunder per søkekombinasjon.
5. **Materialiserte søkeindekser**
   - Pre-kalkuler felter som motor hk, størrelseklasse og booleans så filtrering går raskt.

## Praktiske nøkkelpunkt

- Ikke last ned «alt data» synkront når bruker trykker søk; det gir treghet.
- Bruk «stale-while-revalidate»: vis resultat raskt, oppdater metadata i bakgrunnen.
- Behold «source of truth» per felt:
  - pris/status: FINN
  - tekniske attributter/historikk: intern DB/SVV

## Om FINN-kobling

- Direkte integrasjon er teknisk mulig (API hvis du har avtale, ellers forsiktig scraping innenfor vilkår).
- For stabil drift anbefales avtalt API/partnerløsning hvis tilgjengelig.
