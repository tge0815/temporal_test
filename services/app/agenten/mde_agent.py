"""mde-agent: ermittelt die Minderung der Erwerbsfähigkeit (MdE) in Prozent.

DEMO-HINWEIS: Der Agent "denkt" hier nicht wirklich. Er leitet einen
plausiblen Wert aus den Eingabedaten ab und wuerfelt eine kleine Streuung
dazu - inklusive Konfidenzwert und kurzer Begruendung. In der echten Welt
saesse hier ein KI-Agent oder ein Regelwerk mit aerztlichem Gutachten.
"""
import asyncio
import logging
import random

from temporalio import activity
from temporalio.worker import Worker

from common import config, db
from common.temporal_util import verbinde

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("mde-agent")

# Erfahrungswerte (frei erfunden, aber plausibel) je Körperteil: MdE-Basis in %
BASIS_KOERPERTEIL = {
    "Hand": 20, "Arm": 30, "Bein": 30, "Fuß": 20, "Auge": 25,
    "Wirbelsäule": 30, "Kopf": 35, "Schulter": 20, "Knie": 20, "Sonstiges": 15,
}
# Faktor je Schweregrad
FAKTOR_SCHWERE = {"leicht": 0.4, "mittel": 0.8, "schwer": 1.3, "sehr schwer": 1.8}


@activity.defn(name="ermittle_mde")
async def ermittle_mde(fall: dict) -> dict:
    fall_id = fall["id"]
    await db.verlauf_schreiben(
        fall_id, "MdE-Ermittlung", "mde-agent", "IN_ARBEIT",
        "Agent analysiert Körperteil, Schweregrad und Unfallhergang …",
        {"eingang": {"koerperteil": fall.get("koerperteil"),
                     "schwere": fall.get("schwere")}},
    )

    # Kuenstliche Denkzeit, damit man im Dashboard zusehen kann.
    dauer = random.uniform(config.AGENT_MIN_DAUER, config.AGENT_MAX_DAUER)
    await asyncio.sleep(dauer)

    koerperteil = fall.get("koerperteil") or "Sonstiges"
    schwere = (fall.get("schwere") or "mittel").lower()
    basis = BASIS_KOERPERTEIL.get(koerperteil, 15)
    faktor = FAKTOR_SCHWERE.get(schwere, 0.8)

    roh = basis * faktor * random.uniform(0.85, 1.15)
    # MdE wird in der Praxis in 5er-Schritten festgesetzt, gedeckelt bei 100 %.
    mde = int(min(100, max(10, round(roh / 5) * 5)))

    konfidenz = round(random.uniform(0.72, 0.95), 2)
    begruendung = (
        f"Körperteil '{koerperteil}' (Basiswert {basis} %) bei Schweregrad "
        f"'{schwere}' (Faktor {faktor}) ergibt rechnerisch {roh:.1f} %; "
        f"auf {mde} % gerundet (MdE wird in 5er-Schritten festgesetzt)."
    )

    ergebnis = {
        "mde_prozent": mde,
        "konfidenz": konfidenz,
        "begruendung": begruendung,
        "dauer_sekunden": round(dauer, 1),
    }

    await db.fall_aktualisieren(
        fall_id, mde_prozent=mde, mde_konfidenz=konfidenz,
        mde_begruendung=begruendung,
    )
    await db.verlauf_schreiben(
        fall_id, "MdE-Ermittlung", "mde-agent", "ABGESCHLOSSEN",
        f"MdE festgestellt: {mde} % (Konfidenz {int(konfidenz * 100)} %)",
        ergebnis,
    )
    log.info("Fall %s: MdE = %s %% (Konfidenz %s)", fall_id, mde, konfidenz)
    return ergebnis


async def main() -> None:
    await db.pool()
    client = await verbinde()
    worker = Worker(client, task_queue=config.QUEUE_MDE, activities=[ermittle_mde])
    log.info("mde-agent laeuft auf Queue '%s'", config.QUEUE_MDE)
    await worker.run()


if __name__ == "__main__":
    asyncio.run(main())
