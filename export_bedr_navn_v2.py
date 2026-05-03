"""
Eksporter underenheter med adressedata til CSV for import til PostgreSQL.

Kjør lokalt:
    python export_bedr_navn_v2.py underenheter_alle.xlsx

Output: bedr_navn_v2.csv
"""
import sys
import pandas as pd

input_file = sys.argv[1] if len(sys.argv) > 1 else "underenheter_alle.xlsx"

print(f"Leser {input_file}...")
df = pd.read_excel(
    input_file,
    dtype=str,
    usecols=[
        "Organisasjonsnummer",
        "Navn",
        "Overordnet Enhet",
        "Nedleggelsesdato",
        "Næringskode 1",
        "Beliggenhetsadresse.adresse",
        "Beliggenhetsadresse.poststed",
        "Beliggenhetsadresse.postnummer",
        "Beliggenhetsadresse.kommunenummer",
    ],
)

print(f"  Leste {len(df):,} rader totalt")

# Filtrer bort nedlagte og de uten morselskap
df = df[df["Nedleggelsesdato"].isna()]
df = df[df["Overordnet Enhet"].notna()]
df = df[df["Overordnet Enhet"].str.strip() != ""]

print(f"  {len(df):,} aktive underenheter med morselskap")

df = df.rename(columns={
    "Organisasjonsnummer":               "bedr_orgnr",
    "Navn":                              "bedr_navn",
    "Overordnet Enhet":                  "parent_orgnr",
    "Næringskode 1":                     "naeringskode",
    "Beliggenhetsadresse.adresse":       "adresse",
    "Beliggenhetsadresse.poststed":      "poststed",
    "Beliggenhetsadresse.postnummer":    "postnummer",
    "Beliggenhetsadresse.kommunenummer": "kommunenummer",
})[["bedr_orgnr", "bedr_navn", "parent_orgnr", "naeringskode",
    "adresse", "poststed", "postnummer", "kommunenummer"]]

# Rens
for col in df.columns:
    df[col] = df[col].str.strip() if df[col].dtype == object else df[col]

df["bedr_orgnr"]   = df["bedr_orgnr"].str.strip()
df["parent_orgnr"] = df["parent_orgnr"].str.strip()
df["bedr_navn"]    = df["bedr_navn"].str.strip()

# Fjern duplikater
df = df.drop_duplicates(subset=["bedr_orgnr"])

output_file = "bedr_navn_v2.csv"
df.to_csv(output_file, index=False, encoding="utf-8")
print(f"  Eksportert {len(df):,} rader til {output_file}")
print(f"  Har adresse: {df['adresse'].notna().sum():,}")
print(f"  Har kommunenummer: {df['kommunenummer'].notna().sum():,}")
print(f"  Har næringskode: {df['naeringskode'].notna().sum():,}")
