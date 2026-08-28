"""jav-agent: ermittelt den Jahresarbeitsverdienst (JAV) in Euro.

Grundlage ist die **Entgeltmeldung**, die der Workflow zuvor bei Unternehmer
und Versichertem angefordert hat. Der Agent übernimmt die gemeldete Summe
nicht blind, sondern plausibilisiert sie: Er vergleicht sie mit dem üblichen
Verdienst im gemeldeten Beruf und wendet Mindest- und Höchst-JAV an. Genau
diese Prüfung macht in der Realität die Sachbearbeitung.

DEMO-HINWEIS: Die Vergleichswerte je Beruf sind frei erfunden, aber plausibel.
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
# Stark vereinfachte Unter- und Obergrenze (in der Realität satzungsabhängig).
JAV_MINDEST = 25000
JAV_HOECHST = 96000


def vergleichswert(beruf: str, geburtsjahr: int | None) -> tuple[float, float, int]:
    """Was verdient man in diesem Beruf üblicherweise? (Wert, Faktor, Alter)"""
    basis = BASIS_BERUF.get(beruf or "Sonstiges", 41000)
    alter = max(18, dt.date.today().year - int(geburtsjahr or 1985))
    erfahrungsjahre = max(0, alter - 20)
    faktor = min(1.30, 1 + erfahrungsjahre * 0.012)
    return basis * faktor, faktor, alter


def simulierte_meldung(fall: dict) -> dict:
    """Erzeugt eine plausible Entgeltmeldung – für den Simulieren-Knopf."""
    erwartet, _, _ = vergleichswert(fall.get("beruf"), fall.get("geburtsjahr"))
    return {
        "jav_euro": round(erwartet * random.uniform(0.92, 1.12), 2),
        "quelle": "Entgeltmeldung des Unternehmers (simuliert)",
    }


@activity.defn(name="ermittle_jav")
async def ermittle_jav(fall: dict, meldung: dict | None = None) -> dict:
    fall_id = fall["id"]
    gemeldet = float((meldung or {}).get("jav_euro") or 0)
    quelle = (meldung or {}).get("quelle") or "unbekannt"

    await db.verlauf_schreiben(
        fall_id, "JAV-Ermittlung", "jav-agent", "IN_ARBEIT",
        f"Agent prüft die gemeldete Summe von {euro(gemeldet)} EUR auf "
        f"Plausibilität …",
        {"gemeldet_euro": gemeldet, "quelle": quelle,
         "beruf": fall.get("beruf")},
    )

    await asyncio.sleep(random.uniform(config.AGENT_MIN_DAUER,
                                       config.AGENT_MAX_DAUER))

    erwartet, faktor, alter = vergleichswert(fall.get("beruf"),
                                             fall.get("geburtsjahr"))
    ergebnis = _pruefen(gemeldet, erwartet, faktor, alter, fall, quelle)

    await db.fall_aktualisieren(
        fall_id,
        jav_euro=ergebnis["jav_euro"],
        jav_konfidenz=ergebnis["konfidenz"],
        jav_begruendung=ergebnis["begruendung"],
        jav_gemeldet=gemeldet or None,
        jav_quelle=quelle,
    )
    await db.verlauf_schreiben(
        fall_id, "JAV-Ermittlung", "jav-agent", "ABGESCHLOSSEN",
        f"JAV festgestellt: {euro(ergebnis['jav_euro'])} EUR "
        f"(Konfidenz {int(ergebnis['konfidenz'] * 100)} %)",
        ergebnis,
    )
    log.info("Fall %s: JAV = %s EUR (gemeldet %s)", fall_id,
             ergebnis["jav_euro"], gemeldet)
    return ergebnis


def _pruefen(gemeldet: float, erwartet: float, faktor: float, alter: int,
             fall: dict, quelle: str) -> dict:
    beruf = fall.get("beruf") or "Sonstiges"
    hinweise: list[str] = []

    if gemeldet <= 0:
        # Keine verwertbare Meldung: mit dem Vergleichswert weiterrechnen.
        jav = round(erwartet, 2)
        konfidenz = 0.50
        hinweise.append("Keine verwertbare Entgeltangabe – ersatzweise wurde "
                        "der berufsübliche Vergleichswert angesetzt.")
    else:
        jav = round(gemeldet, 2)
        abweichung = (gemeldet - erwartet) / erwartet
        if abs(abweichung) <= 0.20:
            konfidenz = round(random.uniform(0.90, 0.98), 2)
            hinweise.append(f"Die Meldung liegt {abweichung * 100:+.0f} % vom "
                            f"berufsüblichen Vergleichswert entfernt und ist "
                            f"damit plausibel.")
        else:
            konfidenz = round(random.uniform(0.55, 0.72), 2)
            hinweise.append(f"Die Meldung weicht mit {abweichung * 100:+.0f} % "
                            f"deutlich vom berufsüblichen Vergleichswert ab – "
                            f"eine Rückfrage wäre angezeigt.")

    # Mindest- und Höchst-JAV anwenden.
    gekappt = None
    if jav < JAV_MINDEST:
        gekappt = f"auf den Mindest-JAV von {euro(JAV_MINDEST)} EUR angehoben"
        jav = float(JAV_MINDEST)
    elif jav > JAV_HOECHST:
        gekappt = f"auf den Höchst-JAV von {euro(JAV_HOECHST)} EUR begrenzt"
        jav = float(JAV_HOECHST)
    if gekappt:
        hinweise.append(f"Der Betrag wurde {gekappt}.")

    begruendung = (
        f"Gemeldet: {euro(gemeldet)} EUR ({quelle}). Vergleichswert für "
        f"'{beruf}' bei Alter {alter}: {euro(erwartet)} EUR "
        f"(Erfahrungsfaktor {faktor:.2f}). " + " ".join(hinweise)
    )
    return {
        "jav_euro": jav,
        "gemeldet_euro": round(gemeldet, 2),
        "vergleichswert_euro": round(erwartet, 2),
        "konfidenz": konfidenz,
        "begruendung": begruendung,
        "quelle": quelle,
    }


async def main() -> None:
    await db.pool()
    client = await verbinde()
    worker = Worker(client, task_queue=config.QUEUE_JAV, activities=[ermittle_jav])
    log.info("jav-agent läuft auf Queue '%s'", config.QUEUE_JAV)
    await worker.run()


if __name__ == "__main__":
    asyncio.run(main())
