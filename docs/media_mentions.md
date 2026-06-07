# Medieovervåking

Siden `/media/robert-naess/` henter åpne nyhetstreff automatisk og kan i tillegg hente X-omtaler for siste 7 døgn.

## X-integrasjon

Automatisk X-henting bruker X API v2 recent search. Dette krever en Bearer Token fra X Developer Portal.

Sett miljøvariabelen i Render:

```text
X_BEARER_TOKEN=<din bearer token>
```

Når variabelen finnes:

- perioder på 1-7 døgn henter X-poster automatisk
- grafen får daglige X-counts
- X-poster vises i nyhetsflyten sammen med nyhetsartikler

Uten `X_BEARER_TOKEN` fortsetter siden å virke med Google News og manuelle X-søkelenker.
