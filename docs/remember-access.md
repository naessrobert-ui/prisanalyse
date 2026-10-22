# Husk innlogging

Innloggingssiden har «Husk meg i 90 dager på denne enheten», avkrysset som
standard. Uten avkrysning varer innloggingen bare i nettleserens sesjon.

En egen signert, tidsbegrenset informasjonskapsel gjenoppretter tilgangen hvis
Flask-Session mister sine lokale filer ved deploy eller omstart. Eksisterende
serversesjoner beholdes for blant annet opplastede porteføljer; selve tilgangen
er ikke lenger avhengig av at filene finnes. Informasjonskapselen inneholder
verken tilgangskoden eller porteføljedata.

Den gjelder hele domenet, er HttpOnly og SameSite=Lax, og sendes bare over HTTPS
når SITE_BASE_URL bruker https (standard). «Logg ut» sletter den. Utløp av tokenet
avslutter husket tilgang også om Flask-sesjonen fortsatt finnes. Fristen er 90
dager fra innloggingen og fornyes ikke ved vanlig sidebruk.

## Drift

- Behold `SITE_ACCESS_CODE` og `FLASK_SECRET_KEY` stabile mellom deployer og
  instanser. Endring av én av dem gjør tidligere husket tilgang ugyldig.
- Sett en egen sterk, tilfeldig `FLASK_SECRET_KEY` i webtjenestens miljøvariabler.
  Nøkkelen skal ikke legges i GitHub. Signeringsnøkkelen avledes fra både denne
  verdien og tilgangskoden, og trenger ingen database eller ny ekstern tjeneste.
- Etter utrulling må eksisterende brukere logge inn én gang for å få den nye
  informasjonskapselen. Nettlesere/enheter huskes hver for seg.
- Privat nettlesing eller sletting av informasjonskapsler krever ny innlogging.

Verifisering: `python -m pytest tests/test_site_remember_access.py -q`.
