"""mde-agent: ermittelt die Minderung der Erwerbsfähigkeit (MdE) in Prozent.

Der Agent liest den Wert aus dem hochgeladenen **Gutachten** – so, wie es eine
Sachbearbeiterin auch täte. Ist ein Claude-API-Schlüssel hinterlegt, liest
Claude das Dokument; ohne Schlüssel greift eine Textsuche. Beides liefert
Konfidenzwert, Begründung und die Fundstelle im Gutachten.

Kommt gar kein Gutachten (sollte nicht vorkommen, der Workflow wartet ja
darauf), schätzt der Agent ersatzweise aus den Falldaten.
"""
import asyncio
import logging
import random

from temporalio import activity
from temporalio.worker import Worker

from common import config, db, gutachten
from common.temporal_util import verbinde

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("mde-agent")


@activity.defn(name="ermittle_mde")
async def ermittle_mde(fall: dict, dokument: dict | None = None) -> dict:
    fall_id = fall["id"]
    dok_id = (dokument or {}).get("dokument_id")

    await db.verlauf_schreiben(
        fall_id, "MdE-Ermittlung", "mde-agent", "IN_ARBEIT",
        "Agent liest das eingegangene Gutachten …",
        {"dokument": (dokument or {}).get("dateiname"),
         "verfahren": "Claude" if config.ANTHROPIC_API_KEY else "Textsuche"},
    )

    # Kurze Denkzeit, damit man den Schritt im Dashboard sieht.
    await asyncio.sleep(random.uniform(config.AGENT_MIN_DAUER,
                                       config.AGENT_MAX_DAUER))

    if dok_id:
        ergebnis = await _aus_gutachten(dok_id)
    else:
        ergebnis = _ohne_gutachten(fall)

    await db.fall_aktualisieren(
        fall_id,
        mde_prozent=ergebnis["mde_prozent"],
        mde_konfidenz=ergebnis["konfidenz"],
        mde_begruendung=ergebnis["begruendung"],
        mde_quelle=ergebnis["quelle"],
    )
    await db.verlauf_schreiben(
        fall_id, "MdE-Ermittlung", "mde-agent", "ABGESCHLOSSEN",
        f"MdE festgestellt: {ergebnis['mde_prozent']} % "
        f"(Konfidenz {int(ergebnis['konfidenz'] * 100)} %)",
        ergebnis,
    )
    log.info("Fall %s: MdE = %s %% (%s)", fall_id, ergebnis["mde_prozent"],
             ergebnis["quelle"])
    return ergebnis


async def _aus_gutachten(dok_id: str) -> dict:
    dok = await db.dokument_lesen(dok_id)
    if not dok:
        raise RuntimeError(f"Dokument {dok_id} nicht gefunden")

    text = gutachten.text_aus_dokument(
        dok["inhalt"], dok.get("medientyp"), dok.get("dateiname", "")
    )
    if not text.strip():
        raise RuntimeError("Aus dem Dokument ließ sich kein Text lesen "
                           "(gescanntes Bild ohne Texterkennung?)")

    ergebnis = await gutachten.mde_aus_gutachten(text)
    ergebnis["dokument"] = dok.get("dateiname")
    ergebnis["zeichen_im_dokument"] = len(text)
    return ergebnis


def _ohne_gutachten(fall: dict) -> dict:
    """Notnagel: aus den Falldaten schätzen, wenn kein Gutachten vorliegt."""
    mde = gutachten.plausibler_mde_wert(
        fall.get("koerperteil") or "Sonstiges", fall.get("schwere") or "mittel"
    )
    return {
        "mde_prozent": mde,
        "konfidenz": 0.45,
        "begruendung": f"Ohne Gutachten aus Körperteil "
                       f"'{fall.get('koerperteil')}' und Schweregrad "
                       f"'{fall.get('schwere')}' geschätzt: {mde} %.",
        "fundstelle": "",
        "quelle": "Schätzung aus den Falldaten (kein Gutachten vorhanden)",
        "verfahren": "schaetzung",
    }


async def main() -> None:
    await db.pool()
    client = await verbinde()
    worker = Worker(client, task_queue=config.QUEUE_MDE, activities=[ermittle_mde])
    log.info("mde-agent läuft auf Queue '%s' (Extraktion: %s)",
             config.QUEUE_MDE,
             "Claude" if config.ANTHROPIC_API_KEY else "Textsuche ohne API-Schlüssel")
    await worker.run()


if __name__ == "__main__":
    asyncio.run(main())
