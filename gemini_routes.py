import os
import anthropic
from flask import Blueprint, request, jsonify, render_template
from dotenv import load_dotenv

# Opprett en ny Blueprint
gemini_bp = Blueprint("gemini_kode", __name__, template_folder="templates")

# Last inn API-nøkkel fra .env-filen
load_dotenv()

# --- Konfigurer Anthropic API ---
try:
    api_key = os.environ.get("ANTHROPIC_API_KEY")

    if not api_key:
        print("❌ Advarsel: Fant ingen ANTHROPIC_API_KEY i .env-filen.")
        client = None
    else:
        client = anthropic.Anthropic(api_key=api_key)
        CLAUDE_MODEL = "claude-sonnet-4-6"
        print(f"✅ Anthropic API konfigurert OK med modell: {CLAUDE_MODEL}")

except Exception as e:
    print(f"❌ Feil under konfigurering av Anthropic API: {e}")
    client = None

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
    if not client:
        return jsonify({"error": "Anthropic API er ikke konfigurert. Sjekk server-loggen."}), 500

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

        response = client.messages.create(
            model=CLAUDE_MODEL,
            max_tokens=4096,
            system=system_instruction,
            messages=[{"role": "user", "content": user_prompt}]
        )
        text = response.content[0].text or ""

        # Forskjellig payload ut basert på mode
        if mode == "bquant":
            return jsonify({"mode": mode, "code": text})
        else:
            return jsonify({"mode": mode, "text": text})

    except Exception as e:
        print(f"En feil oppstod: {e}")
        return jsonify({"error": "Kunne ikke generere svar. Prøv igjen senere."}), 500


# --- Test-blokk ---
if __name__ == "__main__":
    print("\n--- Starter manuell test av gemini_routes.py ---")
    if client:
        print("Sender test-spørsmål til Claude...")
        try:
            test_response = client.messages.create(
                model=CLAUDE_MODEL,
                max_tokens=100,
                messages=[{"role": "user", "content": "Skriv print('Hello World') i Python"}]
            )
            print("\nSvar fra Claude:")
            print(test_response.content[0].text)
            print("\n✅ Test vellykket!")
        except Exception as e:
            print(f"\n❌ Test feilet: {e}")
    else:
        print("❌ Kan ikke kjøre test fordi klienten ikke er lastet.")
