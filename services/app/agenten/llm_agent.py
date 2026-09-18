"""llm-agent: der generische "Agent"-Knoten des Prozessdesigners.

Bekommt eine Frage, in die der Interpreter schon die Werte aus dem Kontext
eingesetzt hat ({{ mde.mde_prozent }} → 30), und stellt sie Claude. Zurück
kommt eine kurze Antwort, eine Einschätzung (ja/nein/unklar) und eine
Konfidenz – so kann eine nachfolgende Bedingung auf "agent.einschaetzung"
verzweigen.

Ohne API-Schlüssel antwortet der Agent ehrlich, dass er nicht antworten kann
(einschaetzung "unklar", Konfidenz 0). Nichts wird simuliert.
"""
import asyncio
import logging
from typing import Literal

from pydantic import BaseModel, Field
from temporalio import activity
from temporalio.worker import Worker

from common import config, db
from common.temporal_util import verbinde

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("llm-agent")

SYSTEM_PROMPT = (
    "Du bist ein Sachbearbeitungs-Assistent in der gesetzlichen "
    "Unfallversicherung. Beantworte die Frage knapp (höchstens drei Sätze) "
    "und nur auf Grundlage der mitgelieferten Angaben. Gib zusätzlich eine "
    "Einschätzung 'ja', 'nein' oder 'unklar' und eine Konfidenz zwischen 0 "
    "und 1."
)


class AgentAntwort(BaseModel):
    antwort: str = Field(description="Kurze Antwort in ganzen Sätzen")
    einschaetzung: Literal["ja", "nein", "unklar"]
    konfidenz: float = Field(ge=0, le=1)


@activity.defn(name="agent_fragen")
async def agent_fragen(fall_id: str, titel: str, frage: str) -> dict:
    await db.verlauf_schreiben(
        fall_id, titel, "llm-agent", "IN_ARBEIT",
        "Agent bearbeitet die Frage …",
        {"frage": frage, "verfahren": "Claude" if config.ANTHROPIC_API_KEY
         else "kein Schlüssel"},
    )
    if config.ANTHROPIC_API_KEY:
        try:
            ergebnis = await _mit_claude(frage)
        except Exception as e:  # noqa: BLE001
            log.exception("Claude-Anfrage fehlgeschlagen")
            ergebnis = {"antwort": f"Anfrage fehlgeschlagen: {e}",
                        "einschaetzung": "unklar", "konfidenz": 0.0,
                        "verfahren": "fehler"}
    else:
        await asyncio.sleep(1.0)
        ergebnis = {
            "antwort": "Kein ANTHROPIC_API_KEY hinterlegt – der Agent kann die "
                       "Frage nicht beantworten. Schlüssel in geheim.env "
                       "eintragen, dann antwortet Claude.",
            "einschaetzung": "unklar", "konfidenz": 0.0,
            "verfahren": "kein Schlüssel",
        }
    ergebnis["frage"] = frage
    await db.verlauf_schreiben(
        fall_id, titel, "llm-agent", "ABGESCHLOSSEN",
        f"Agent: {ergebnis['antwort']} (Einschätzung: {ergebnis['einschaetzung']})",
        ergebnis,
    )
    log.info("Fall %s: Agent '%s' → %s", fall_id, titel, ergebnis["einschaetzung"])
    return ergebnis


async def _mit_claude(frage: str) -> dict:
    from anthropic import AsyncAnthropic

    client = AsyncAnthropic(api_key=config.ANTHROPIC_API_KEY)
    antwort = await client.messages.parse(
        model=config.CLAUDE_MODELL,
        max_tokens=2000,
        system=SYSTEM_PROMPT,
        messages=[{"role": "user", "content": frage}],
        output_format=AgentAntwort,
    )
    if getattr(antwort, "stop_reason", None) == "refusal":
        raise RuntimeError("Anfrage wurde abgelehnt")
    a: AgentAntwort = antwort.parsed_output
    return {"antwort": a.antwort, "einschaetzung": a.einschaetzung,
            "konfidenz": round(a.konfidenz, 2),
            "verfahren": f"Claude ({config.CLAUDE_MODELL})"}


async def main() -> None:
    await db.pool()
    client = await verbinde()
    worker = Worker(client, task_queue=config.QUEUE_AGENT, activities=[agent_fragen])
    log.info("llm-agent laeuft auf Queue '%s' (%s)", config.QUEUE_AGENT,
             "Claude" if config.ANTHROPIC_API_KEY else "ohne Schluessel")
    await worker.run()


if __name__ == "__main__":
    asyncio.run(main())
