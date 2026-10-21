# NewsWeb AI-monitor på prisanalyse.no – prototype

Denne funksjonen har to separate deler:

1. **Nettside**: /newsweb/ på prisanalyse.no (Flask Blueprint i newsweb_routes.py). Foreløpig vises en eksplisitt merket demonstrasjon med oppdiktede meldinger.
2. **Analysejobb**: .github/workflows/newsweb-monitor.yml kjører omtrent én gang i timen, og kan startes manuelt fra **Actions → NewsWeb AI monitor → Run workflow**. Rapportene ligger i Actions Summary og Artifacts.

> **Ingen ekte NewsWeb-data hentes automatisk i denne versjonen.** Direkte scraping fra NewsWeb er ikke implementert. Koble bare til en maskinlesbar datakilde som er godkjent for slik bruk.

## Slik tester du

Gå til Actions og start jobben med mode=demo og dry_run=true. Det brukes fem oppdiktede meldinger. Uten OPENAI_API_KEY kjøres en regelbasert vurdering, tydelig merket IKKE AI. Med nøkkel får du AI-vurderinger i GitHub Actions-rapporten. Demo sender aldri Pushover.

## Overgang til live

Opprett **repository secrets** (aldri i koden): NEWS_FEED_URL, OPENAI_API_KEY, PUSHOVER_APP_TOKEN, PUSHOVER_USER_KEY.

Valgfrie **repository variables**:
- NEWSWEB_SOURCE_MODE = live (ellers forblir timeplanen i demo)
- FEED_FORMAT = json (eller rss)
- VALID_MARKETS = XOSL
- EQUITY_TICKERS = kommaseparert godkjent liste, dersom feeden mangler instrumentmetadata
- OPENAI_MODEL = gpt-4.1-mini
- NOTIFICATIONS_ENABLED = true (standard false)
- NOTIFY_SCORE = 75
- NEWSWEB_DRY_RUN = false (standard true)

Datafeed må gi en JSON-liste eller {"items":[...]} med feltene id, published_at (ISO med tidssone), market=XOSL, instrument_type=equity, ticker, issuer, title, body og valgfri url. Mange RSS-feeds mangler metadata og må tilpasses. Andre markeder/obligasjoner og rutinemeldinger filtreres bort.

**Sikkerhet:** Dette repositoryet er offentlig. Ikke lagre ikke-offentlige børsmeldinger, interne porteføljeopplysninger eller lisensiert innhold i Git. Action-rapportene er ment for testdata eller innhold dere har rett til å behandle. Dersom dere vil vise live analyser på prisanalyse.no, kreves egen autorisert lagring og tilgangsstyring.

Nettsiden viser inntil videre \`newsweb_monitor/sample/demo_report.json\`. For å vise rapport fra en godkjent kilde kan Render senere settes opp med en serverlokal rapportfil via NEWSWEB_DASHBOARD_REPORT_PATH. GitHub Actions skriver foreløpig rapportene **kun som Actions-artifacts**, ikke direkte til nettsiden. Dermed eksponeres ikke en framtidig privat datafeed utilsiktet.

## Utvikling lokalt

\`\`\`sh
python -m unittest discover -s newsweb_monitor/tests -v
SOURCE_MODE=demo DRY_RUN=true python -m newsweb_monitor.monitor
\`\`\`
