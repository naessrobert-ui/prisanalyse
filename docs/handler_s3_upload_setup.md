# Handler DB: direkte opplasting til S3 (store filer, f.eks. 235 MB)

Denne appen bruker nå **presigned PUT URL** for opplasting av `topchanges.db` direkte fra nettleser til S3.

Hvis opplastingen feiler med CORS-feil i browser (typisk HTTP 403/blocked by CORS), sett CORS på S3-bucketen.

## Anbefalt S3 CORS-konfigurasjon

> Bytt ut domenet under med ditt faktiske produksjonsdomene.

```json
[
  {
    "AllowedHeaders": [
      "*"
    ],
    "AllowedMethods": [
      "PUT",
      "GET",
      "HEAD"
    ],
    "AllowedOrigins": [
      "https://prisanalyse.no",
      "http://localhost:5000"
    ],
    "ExposeHeaders": [
      "ETag",
      "x-amz-request-id",
      "x-amz-id-2"
    ],
    "MaxAgeSeconds": 3000
  }
]
```

## AWS CLI

```bash
aws s3api put-bucket-cors \
  --bucket <DIN_BUCKET> \
  --cors-configuration file://cors.json
```

## IAM-tillatelser som trengs

Rollen/brukeren som appen kjører med må minst kunne:

- `s3:PutObject` (for upload via presigned URL)
- `s3:GetObject` (for refresh av lokal DB fra S3)

Eksempel på policy for ett key-prefix:

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": [
        "s3:PutObject",
        "s3:GetObject"
      ],
      "Resource": "arn:aws:s3:::<DIN_BUCKET>/<DIN_PREFIX>/*"
    }
  ]
}
```


## Opplastingsgrense i app

- Appen bruker `HANDLER_DB_UPLOAD_MAX_MB` (standard **300 MB**).
- For filer rundt 235 MB skal dette normalt være innenfor.
- Hvis proxy (NGINX/Render) har lavere grense, bruk direkte S3-flyt i UI.

## Feilsøking

- **Upload stopper på 502 via app-endepunkt**: bruk direkte S3-opplasting i UI (standardflyt).
- **PUT mot presigned URL feiler med CORS**: verifiser `AllowedOrigins`, `AllowedMethods` og `AllowedHeaders`.
- **Upload ok, men data ikke oppdatert i appen**: kall `/handler-oslo-bors/api/reload-db-from-s3` (gjøres automatisk i UI etter vellykket upload).


## Lokal nød-opplasting (anbefalt når web-opplasting feiler)

Kjør fra maskinen som har DB-filen:

```bash
python scripts/upload_handler_db_to_s3.py \
  --source /path/to/topchanges_recent_60d.db \
  --bucket <DIN_BUCKET> \
  --key <DIN_PREFIX>/topchanges.db
```

Etter opplasting: verifiser at `HANDLER_DB_S3_URI` peker til samme `s3://bucket/key` i drift.
