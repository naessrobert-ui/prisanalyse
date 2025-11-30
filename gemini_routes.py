import os
import google.generativeai as genai
from flask import Blueprint, request, jsonify, render_template
from dotenv import load_dotenv

# Opprett en ny Blueprint
gemini_bp = Blueprint("gemini_kode", __name__, template_folder="templates")

# Last inn API-nøkkel fra .env-filen
load_dotenv()

# --- Konfigurer Gemini API ---
try:
    # Sjekker både GEMINI og GOOGLE nøkkelnavn for sikkerhets skyld
    api_key = os.environ.get("GEMINI_API_KEY") or os.environ.get("GOOGLE_API_KEY")

    if not api_key:
        print("❌ Advarsel: Fant ingen API-nøkkel i .env-filen (sjekk GEMINI_API_KEY eller GOOGLE_API_KEY).")
        model = None
    else:
        genai.configure(api_key=api_key)

        # Inntil videre bruker vi 2.5-modellen fast
        GEMINI_MODEL_NAME = "gemini-2.5-pro"

        model = genai.GenerativeModel(GEMINI_MODEL_NAME)
        print(f"✅ Gemini API konfigurert OK med modell: {GEMINI_MODEL_NAME}")

except Exception as e:
    print(f"❌ Feil under konfigurering av Gemini API: {e}")
    model = None

# --- Systeminstruksjoner ---

# 1) BQUANT-modus – ren kode, som før
SYSTEM_INSTRUCTION_BQUANT = """
Du er en ekspert på finansiell analyse og Python-koding, spesialisert for Bloombergs BQUANT-miljø.
All kode du genererer må være kompatibel med BQUANT.
Svar kun med ren, kommentert Python-kode, med mindre du blir bedt om noe annet.
Start kodesvaret med ```python og avslutt med ```.
Ikke inkluder noe tekst før eller etter kodeblokken.
"""

# 2) Finans / analyse – kan svare med både tekst og kode
SYSTEM_INSTRUCTION_FINANS = """
Du er en erfaren finansiell analytiker og Python-koder.
Du kan kombinere forklarende tekst og kode.
Når du foreslår kode, bruk ```python-blokker, men det er lov å forklare både før og etter koden.
Ta hensyn til at brukeren ofte jobber i Bloomberg/BQUANT, men du trenger ikke begrense deg til det
hvis andre verktøy eller forklaringer er mer hensiktsmessige.
"""

# 3) Helt fri assistent – ingen kodebegrensning
SYSTEM_INSTRUCTION_FREE = """
Du er en hjelpsom, generell AI-assistent.
Du kan svare med tekst, eksempler, kode eller hva som er mest nyttig,
på norsk eller engelsk avhengig av hva brukeren skriver.
Ingen spesielle begrensninger på format, utover å være klar og nyttig.
"""


@gemini_bp.route("/kode/")
def kode_side():
    """Viser selve kode-/chat-appen."""
    return render_template("kode_analyse.html")


@gemini_bp.route("/kode/generate", methods=["POST"])
def generate_code():
    """
    API-endepunkt som frontend kaller for å generere svar.
    Støtter tre modes:
      - mode='bquant' (default): strengt BQUANT/Bloomberg-fokus, bare kode i output.
      - mode='finans'          : finans/analyse, både tekst og kode.
      - mode='free'            : helt fri assistent.
    """
    if not model:
        return jsonify({"error": "Gemini API er ikke konfigurert. Sjekk server-loggen."}), 500

    data = request.get_json(silent=True) or {}
    user_prompt = data.get("prompt")
    mode = data.get("mode", "bquant")  # "bquant", "finans" eller "free"

    if not user_prompt:
        return jsonify({"error": "Forespørselen kan ikke være tom."}), 400

    try:
        # Velg systeminstruksjon basert på mode
        if mode == "bquant":
            system_instruction = SYSTEM_INSTRUCTION_BQUANT
        elif mode == "finans":
            system_instruction = SYSTEM_INSTRUCTION_FINANS
        elif mode == "free":
            system_instruction = SYSTEM_INSTRUCTION_FREE
        else:
            return jsonify({"error": f"Ukjent mode: {mode}"}), 400

        full_prompt = f"{system_instruction}\n\nBrukerens forespørsel: {user_prompt}"

        response = model.generate_content(full_prompt)
        text = response.text or ""

        # Forskjellig payload ut basert på mode
        if mode == "bquant":
            # Kompatibel med gammel frontend: 'code'
            return jsonify({"mode": mode, "code": text})
        else:
            # Finans + fri-modus: bruk 'text'
            return jsonify({"mode": mode, "text": text})

    except Exception as e:
        print(f"En feil oppstod: {e}")
        return jsonify({"error": "Kunne ikke generere svar. Prøv igjen senere."}), 500


# --- Test-blokk ---
if __name__ == "__main__":
    print("\n--- Starter manuell test av gemini_routes.py ---")
    if model:
        print("Sender test-spørsmål til Gemini...")
        try:
            test_response = model.generate_content("Skriv print('Hello World') i Python")
            print("\nSvar fra Gemini:")
            print(test_response.text)
            print("\n✅ Test vellykket!")
        except Exception as e:
            print(f"\n❌ Test feilet: {e}")
    else:
        print("❌ Kan ikke kjøre test fordi modellen ikke er lastet.")
