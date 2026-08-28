"""jav-agent: ermittelt den Jahresarbeitsverdienst (JAV) in Euro.

DEMO-HINWEIS: Ebenfalls simuliert - abgeleitet aus Beruf und Alter,
mit Streuung, Konfidenzwert und Begruendung. Real kaeme der JAV aus
Entgeltmeldungen des Arbeitgebers bzw. dem Lohnkonto.
"""
import asyncio
import datetime as dt
import logging
import random

from temporalio import activity
from temporalio.worker import Worker

from common import config, db
from common.format import euro
from common.temporal_util import verbinde

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("jav-agent")

# Grobe Brutto-Jahresverdienste je Beruf (frei erfunden, aber plausibel)
BASIS_BERUF = {
    "Bauarbeiter": 42000, "Dachdecker": 44000, "Elektriker": 46000,
    "Krankenpfleger": 45000, "LKW-Fahrer": 40000, "Bürokaufmann": 43000,
    "Industriemechaniker": 48000, "Landwirt": 36000, "Sonstiges": 41000,
}
# Gesetzlicher Mindest-JAV-Gedanke, hier stark vereinfacht als Untergrenze.
JAV_MINDEST = 25000
JAV_HOECHST = 96000


@activity.defn(name="ermittle_jav")
async def ermittle_jav(fall: dict) -> dict:
    fall_id = fall["id"]
    await db.verlauf_schreiben(
        fall_id, "JAV-Ermittlung", "jav-agent", "IN_ARBEIT",
        "Agent wertet Beruf und Erwerbsbiografie aus …",
        {"eingang": {"beruf": fall.get("beruf"),
                     "geburtsjahr": fall.get("geburtsjahr")}},
    )

    dauer = random.uniform(config.AGENT_MIN_DAUER, config.AGENT_MAX_DAUER)
    await asyncio.sleep(dauer)

    beruf = fall.get("beruf") or "Sonstiges"
    basis = BASIS_BERUF.get(beruf, 41000)

    # Berufserfahrung: pro Jahr ueber 20 ein kleiner Aufschlag (max. +30 %).
    geburtsjahr = fall.get("geburtsjahr") or 1985
    alter = max(18, dt.date.today().year - int(geburtsjahr))
    erfahrungsjahre = max(0, alter - 20)
    erfahrungsfaktor = min(1.30, 1 + erfahrungsjahre * 0.012)

    roh = basis * erfahrungsfaktor * random.uniform(0.92, 1.12)
    jav = float(min(JAV_HOECHST, max(JAV_MINDEST, round(roh, 2))))

    konfidenz = round(random.uniform(0.80, 0.97), 2)
    begruendung = (
        f"Beruf '{beruf}' (Basis {euro(basis)} EUR) x Erfahrungsfaktor "
        f"{erfahrungsfaktor:.2f} (Alter {alter}, {erfahrungsjahre} Jahre "
        f"Berufserfahrung) ergibt {euro(jav)} EUR Jahresarbeitsverdienst."
    )

    ergebnis = {
        "jav_euro": jav,
        "konfidenz": konfidenz,
        "begruendung": begruendung,
        "dauer_sekunden": round(dauer, 1),
    }

    await db.fall_aktualisieren(
        fall_id, jav_euro=jav, jav_konfidenz=konfidenz, jav_begruendung=begruendung,
    )
    await db.verlauf_schreiben(
        fall_id, "JAV-Ermittlung", "jav-agent", "ABGESCHLOSSEN",
        f"JAV festgestellt: {euro(jav)} EUR (Konfidenz {int(konfidenz * 100)} %)",
        ergebnis,
    )
    log.info("Fall %s: JAV = %s EUR (Konfidenz %s)", fall_id, jav, konfidenz)
    return ergebnis


async def main() -> None:
    await db.pool()
    client = await verbinde()
    worker = Worker(client, task_queue=config.QUEUE_JAV, activities=[ermittle_jav])
    log.info("jav-agent laeuft auf Queue '%s'", config.QUEUE_JAV)
    await worker.run()


if __name__ == "__main__":
    asyncio.run(main())
