# check_api.py
import os
import google.generativeai as genai
from dotenv import load_dotenv

print ("--- Starter diagnostisk test ---")

# Last inn .env-filen for å hente nøkkelen
load_dotenv ()
api_key = os.environ.get ("GEMINI_API_KEY")

if not api_key:
    print ("FEIL: Fant ikke GEMINI_API_KEY i .env-filen.")
else:
    print ("API-nøkkel funnet. Konfigurerer klient...")
    try:
        genai.configure (api_key=api_key)

        print ("\nForsøker å liste modeller...")

        # Dette er testen: Hvilke modeller kan du se?
        for m in genai.list_models ():
            print (f"- {m.name}")

        print ("\nTesten var vellykket!")

    except Exception as e:
        print ("\n--- EN KRITISK FEIL OPPSTOD ---")
        print ("Full feilmelding:")
        print (e)
        print ("---------------------------------")

print ("\n--- Testen er ferdig ---")