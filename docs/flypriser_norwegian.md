# Norwegian prisindeks & booking-proxy (investor)

Investor-vinklet analyse av flyprisdata som et **ledende signal** for Norwegian
(Oslo Børs: NAS), på `/flypriser/norwegian`.

## Idé

Vi henter billigste priser på tvers av Norwegians viktigste ruter (begge
retninger) og bruker flyselskapskoden (`DY` = Norwegian) til å skille
Norwegians egne priser fra konkurrentene. Ut av det bygger vi:

| Komponent | Hva det viser |
|-----------|---------------|
| **Norwegian prisindeks** | Prisnivå over tid (basis 100), Norwegian vs. markedet. To linjer: **laveste** (inngangspris, renest signal) og **median** (typisk billettpris, yield-proxy). |
| **Prisspredning** | Gap mellom median og laveste pris. Krympende gap = de billigste billettene selges ut → prispress/høy etterspørsel. |
| **Prisavvik** | Ruter der prisen plutselig avviker unormalt fra egen basislinje (median tidligere) – flagg opp/ned. |
| **Konkurrent-gap** | Norwegians pris vs. billigste konkurrent (SAS/Wizz/Ryanair …) per rute. |
| **Booking-kurve** | Hvordan billigste pris stiger mot avreise – proxy for etterspørsel/fyllingsgrad. |
| **Trafikktall** | Norwegians offisielle månedlige børstall (kabinfaktor/RPK/passasjerer). |

> ⚠️ **Forbehold:** prisdataene er cachede *laveste* priser, ikke offisielle
> booking-/kabinfaktortall. En **indikasjon** på prispress, ikke fasit.
> Signalene blir meningsfulle etter noen dagers innsamlet historikk.

## Datainnsamling

`scripts/flypriser_bergen.py` henter nå hele rutenettet (grupper: `bergen`,
`innenriks`, `norwegian_intl`), begge retninger, og lagrer **alle** tilbud
(ikke bare billigste) slik at flyselskap kan skilles ut i analysen:

- `data/flypriser_historikk.csv` – full historikk (append, én rad per tilbud)
- `data/flypriser_beste.csv` – billigste pr. rute+måned ved siste kjøring

Analysen ligger i `scripts/flypriser_analyse.py` (rene funksjoner, ingen IO
utover CSV) og eksponeres via `/flypriser/norwegian/api`.

## Offisielle trafikktall (fase 2)

`scripts/norwegian_trafikk.py` leser `data/norwegian_trafikk.csv` og beregner
endring måned-over-måned og år-over-år. Modulen **finner ikke opp tall** – du
fyller inn verifiserte tall fra den offisielle rapporten.

### Hvor finner du rapporten
Norwegian publiserer «Traffic figures / Trafikktall» rundt den 5. hver måned:

- Oslo Børs NewsWeb: <https://newsweb.oslobors.no> (søk «Norwegian Air Shuttle»)
- Norwegian IR: <https://www.norwegian.com/no/about/company/investor-relations/>

### Slik legger du inn
Én rad per måned i `data/norwegian_trafikk.csv`:

```csv
maaned,passasjerer,ask_mill,rpk_mill,kabinfaktor_pst,punktlighet_pst,kilde
2026-07,2200000,4300,3900,90.7,83,https://newsweb.oslobors.no/...
```

Tomme felt er greit – panelet viser det som finnes. Automatisk henting fra
NewsWeb/IR er en mulig senere utvidelse (kilden er PDF/børsmelding, så det
krever egen parser).

## Justere rutenettet

Rutene ligger i `_RUTER_RAW` øverst i `scripts/flypriser_bergen.py` som
`(A, B, gruppe)` og hentes automatisk i begge retninger. Legg til/fjern par
etter behov. Flyselskapsnavn styres av `SELSKAP`-tabellen samme sted.
