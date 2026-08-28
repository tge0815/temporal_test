"""rentenberechnung: DETERMINISTISCHER Rechenschritt - bewusst KEIN Agent.

Fachregel (vereinfacht nach SGB VII):
    Vollrente        = 2/3 x JAV                (bei 100 % Erwerbsminderung)
    Verletztenrente  = Vollrente x (MdE / 100)  (Teilrente je nach MdE)
    Monatsrente      = Jahresrente / 12

Kein Zufall, keine Simulation: gleiche Eingabe -> immer gleiches Ergebnis.
"""
import asyncio
import logging

from temporalio import activity
from temporalio.worker import Worker

from common import config, db
from common.format import euro
from common.temporal_util import verbinde

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("rentenberechnung")

ZWEI_DRITTEL = 2 / 3


def berechne(jav_euro: float, mde_prozent: float) -> dict:
    """Reine Rechenfunktion - genau hier steckt die echte Fachregel."""
    vollrente_jahr = jav_euro * ZWEI_DRITTEL
    rente_jahr = round(vollrente_jahr * (mde_prozent / 100), 2)
    rente_monat = round(rente_jahr / 12, 2)
    formel = (
        f"Rente/Jahr = 2/3 x JAV x (MdE / 100) = 2/3 x {euro(jav_euro)} EUR x "
        f"({mde_prozent:g} / 100) = {euro(rente_jahr)} EUR  |  "
        f"Rente/Monat = {euro(rente_jahr)} / 12 = {euro(rente_monat)} EUR"
    )
    return {
        "jav_euro": round(jav_euro, 2),
        "mde_prozent": mde_prozent,
        "vollrente_jahr_euro": round(vollrente_jahr, 2),
        "rente_jahr_euro": rente_jahr,
        "rente_monat_euro": rente_monat,
        "formel": formel,
        "deterministisch": True,
    }


@activity.defn(name="berechne_rente")
async def berechne_rente(fall_id: str, jav_euro: float, mde_prozent: float) -> dict:
    await db.verlauf_schreiben(
        fall_id, "Rentenberechnung", "rentenberechnung (deterministisch)",
        "IN_ARBEIT",
        "Wende feste Rechenregel an: Rente = 2/3 x JAV x (MdE / 100)",
        {"jav_euro": jav_euro, "mde_prozent": mde_prozent},
    )
    # Kurze Pause nur zur Sichtbarkeit im Dashboard – die Rechnung selbst
    # dauert Mikrosekunden.
    await asyncio.sleep(1.0)

    ergebnis = berechne(float(jav_euro), float(mde_prozent))

    await db.fall_aktualisieren(
        fall_id,
        rente_jahr=ergebnis["rente_jahr_euro"],
        rente_monat=ergebnis["rente_monat_euro"],
        rente_formel=ergebnis["formel"],
    )
    await db.verlauf_schreiben(
        fall_id, "Rentenberechnung", "rentenberechnung (deterministisch)",
        "ABGESCHLOSSEN",
        f"Monatliche Verletztenrente: {euro(ergebnis['rente_monat_euro'])} EUR",
        ergebnis,
    )
    log.info("Fall %s: Rente/Monat = %s EUR", fall_id, ergebnis["rente_monat_euro"])
    return ergebnis


async def main() -> None:
    await db.pool()
    client = await verbinde()
    worker = Worker(client, task_queue=config.QUEUE_RENTE,
                    activities=[berechne_rente])
    log.info("rentenberechnung laeuft auf Queue '%s'", config.QUEUE_RENTE)
    await worker.run()


if __name__ == "__main__":
    asyncio.run(main())
