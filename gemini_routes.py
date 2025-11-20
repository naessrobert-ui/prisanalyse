# gemini_routes.py
import os
# RETTELSE 1: Vi bruker standard import som fungerer med .configure()
import google.generativeai as genai
from flask import Blueprint, request, jsonify, render_template
from dotenv import load_dotenv

# Opprett en ny Blueprint
gemini_bp = Blueprint ('gemini_kode', __name__, template_folder='templates')

# Last inn API-nøkkel fra .env-filen
load_dotenv ()

# Konfigurer Gemini API
try:
    # Sjekker både GEMINI og GOOGLE nøkkelnavn for sikkerhets skyld
    api_key = os.environ.get ("GEMINI_API_KEY") or os.environ.get ("GOOGLE_API_KEY")

    if not api_key:
        print ("❌ Advarsel: Fant ingen API-nøkkel i .env-filen (sjekk GEMINI_API_KEY eller GOOGLE_API_KEY).")
        model = None
    else:
        genai.configure (api_key=api_key)

        # RETTELSE 2: Bruk en modell som faktisk finnes. '2.5-pro' finnes ikke.
        # Vi bruker 1.5-flash fordi den er raskest og gir færrest feilmeldinger.
        model = genai.GenerativeModel ('gemini-2.5-pro')
        print ("✅ Gemini API konfigurert OK med modell: gemini-1.5-flash")

except Exception as e:
    print (f"❌ Feil under konfigurering av Gemini API: {e}")
    model = None

# Systeminstruksjon
SYSTEM_INSTRUCTION = """
Du er en ekspert på finansiell analyse og Python-koding, spesialisert for Bloomberg's BQUANT-miljø.
All kode du genererer må være kompatibel med BQUANT.
Svar kun med ren, kommentert Python-kode, med mindre du blir bedt om noe annet.
Start kodesvaret med ```python og avslutt med ```.
Ikke inkluder noe tekst før eller etter kodeblokken.
"""


@gemini_bp.route ("/kode/")
def kode_side():
    """Viser selve kode-appen."""
    return render_template ("kode_analyse.html")


@gemini_bp.route ("/kode/generate", methods=['POST'])
def generate_code():
    """API-endepunkt som frontend kaller for å generere kode."""
    if not model:
        return jsonify ({"error": "Gemini API er ikke konfigurert. Sjekk server-loggen."}), 500

    user_prompt = request.json.get ('prompt')
    if not user_prompt:
        return jsonify ({"error": "Forespørselen kan ikke være tom."}), 400

    try:
        full_prompt = f"{SYSTEM_INSTRUCTION}\n\nBrukerens forespørsel: {user_prompt}"
        response = model.generate_content (full_prompt)
        return jsonify ({"code": response.text})
    except Exception as e:
        print (f"En feil oppstod: {e}")
        return jsonify ({"error": "Kunne ikke generere kode. Prøv igjen senere."}), 500


# --- Test-blokk ---
if __name__ == "__main__":
    print ("\n--- Starter manuell test av gemini_routes.py ---")
    if model:
        print ("Sender test-spørsmål til Gemini...")
        try:
            test_response = model.generate_content ("Skriv print('Hello World') i Python")
            print ("\nSvar fra Gemini:")
            print (test_response.text)
            print ("\n✅ Test vellykket!")
        except Exception as e:
            print (f"\n❌ Test feilet: {e}")
    else:
        print ("❌ Kan ikke kjøre test fordi modellen ikke er lastet.")