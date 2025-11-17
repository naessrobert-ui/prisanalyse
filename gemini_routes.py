# gemini_routes.py
import os
import google.generativeai as genai
from flask import Blueprint, request, jsonify, render_template
from dotenv import load_dotenv

# Opprett en ny Blueprint
gemini_bp = Blueprint('gemini_kode', __name__, template_folder='templates')

# Last inn API-nøkkel fra .env-filen (viktig for sikkerhet)
load_dotenv()

# Konfigurer Gemini API med din nøkkel
try:
    api_key = os.environ.get("GEMINI_API_KEY")
    if not api_key:
        print("Advarsel: GEMINI_API_KEY er ikke satt i .env-filen.")
        model = None
    else:
        genai.configure(api_key=api_key)
        model = genai.GenerativeModel('gemini-2.5-pro')
except Exception as e:
    print(f"Feil under konfigurering av Gemini API: {e}")
    model = None

# Systeminstruksjon som forteller Gemini hvordan den skal oppføre seg
SYSTEM_INSTRUCTION = """
Du er en ekspert på finansiell analyse og Python-koding, spesialisert for Bloomberg's BQUANT-miljø.
All kode du genererer må være kompatibel med BQUANT.
Bruk Bloomberg Query Language (BQL) der det er relevant.
Svar kun med ren, kommentert Python-kode, med mindre du blir bedt om noe annet.
Start kodesvaret med ```python og avslutt med ```.
Ikke inkluder noe tekst før eller etter kodeblokken.
"""

@gemini_bp.route("/kode/")
def kode_side():
    """Viser selve kode-appen."""
    # Vi kaller HTML-filen kode_analyse.html for å være konsekvent med dine andre filer
    return render_template("kode_analyse.html")

@gemini_bp.route("/kode/generate", methods=['POST'])
def generate_code():
    """API-endepunkt som frontend kaller for å generere kode."""
    if not model:
        return jsonify({"error": "Gemini API er ikke konfigurert. Mangler API-nøkkel?"}), 500

    user_prompt = request.json.get('prompt')
    if not user_prompt:
        return jsonify({"error": "Forespørselen kan ikke være tom."}), 400

    try:
        full_prompt = f"{SYSTEM_INSTRUCTION}\n\nBrukerens forespørsel: {user_prompt}"
        response = model.generate_content(full_prompt)
        return jsonify({"code": response.text})
    except Exception as e:
        print(f"En feil oppstod: {e}")
        return jsonify({"error": "Kunne ikke generere kode. Prøv igjen."}), 500