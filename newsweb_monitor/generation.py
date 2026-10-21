"""Version of the currently published NewsWeb AI analysis history.

Bump ONLY for a deliberate full reset. Historical Git commits/actions artifacts
remain in GitHub history; this controls what current dashboard and cache show.
"""
NEWSWEB_GENERATION = "ai-restart-2026-10-21-v1"

def prepare_state(state: dict, *, mode: str) -> tuple[dict, bool]:
    if mode != "newsweb":
        return state, False
    if state.get("newsweb_generation") == NEWSWEB_GENERATION:
        return state, False
    return {
        "seen": {},
        "reports_by_ticker": {},
        "newsweb_generation": NEWSWEB_GENERATION,
    }, True
