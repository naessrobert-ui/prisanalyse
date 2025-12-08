import pandas as pd
from dotenv import load_dotenv  # hvis ikke allerede lastet et annet sted

load_dotenv()  # gjør ingenting skadelig hvis det kalles flere ganger

import pandas as pd

pd.set_option("display.max_columns", None)
pd.set_option("display.width", None)  # gjør at linjen ikke brytes
pd.set_option("display.max_colwidth", None)  # viser hele tekstfelt


S3_PATH = "s3://prisanalyse-data/calc/bil/database_biler.parquet"

def main():
    # Les Parquet direkte fra S3 (pandas + s3fs bruker AWS_* fra .env automatisk)
    df = pd.read_parquet(S3_PATH)

    print("Kolonner og typer:")
    print(df.dtypes)
    print("\nAntall rader:", len(df))

    print("\nDe første 10 radene:")
    print(df.head(10))

    # Eksempler på litt analyse:
    print("\nAntall biler per årstall:")
    if "årstall" in df.columns:
        print(df["årstall"].value_counts().sort_index())

    print("\nPris-statistikk:")
    if "pris_num" in df.columns:
        print(df["pris_num"].describe())



if __name__ == "__main__":
    main()

