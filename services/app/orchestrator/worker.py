"""Orchestrator-Service.

Haelt die Workflow-Definition und die organisatorischen Activities
(Statuswechsel, Abschluss). Die fachlichen Schritte laufen in den
Agenten-Services auf eigenen Task-Queues.
"""
import asyncio
import logging

from temporalio import activity
from temporalio.worker import Worker

from common import config, db
from common.format import euro
from common.temporal_util import verbinde
from orchestrator.prozess_workflow import ProzessWorkflow
from orchestrator.workflow import UnfallSachbearbeitungWorkflow

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("orchestrator")

SCHRITT_TEXTE = {
    "WARTE_AUF_GUTACHTEN": ("Gutachten", "Orchestrator wartet auf das Gutachten"),
    "MDE_ERMITTLUNG": ("MdE-Ermittlung", "Orchestrator übergibt an mde-agent"),
    "WARTE_AUF_ENTGELTMELDUNG": ("Entgeltmeldung",
                                 "Orchestrator wartet auf die Entgeltmeldung"),
    "JAV_ERMITTLUNG": ("JAV-Ermittlung", "Orchestrator übergibt an jav-agent"),
    "RENTENBERECHNUNG": ("Rentenberechnung",
                         "Orchestrator übergibt an rentenberechnung (deterministisch)"),
}

VORGANG_TEXTE = {
    "gutachten": ("Gutachten", "Gutachten"),
    "entgeltmeldung": ("Entgeltmeldung", "Entgeltmeldung"),
}


@activity.defn(name="status_setzen")
async def status_setzen(fall_id: str, status: str) -> None:
    schritt, text = SCHRITT_TEXTE.get(status, (status, status))
    await db.fall_aktualisieren(fall_id, status=status)
    await db.verlauf_schreiben(
        fall_id, schritt, "orchestrator (Temporal)", "GESTARTET", text,
        {"temporal_workflow": activity.info().workflow_id,
         "temporal_activity": activity.info().activity_type},
    )
    log.info("Fall %s -> %s", fall_id, status)


@activity.defn(name="gutachten_beauftragen")
async def gutachten_beauftragen(fall_id: str) -> None:
    """Beauftragt die Begutachtung. Ab hier wartet der Workflow."""
    fall = await db.fall_lesen(fall_id)
    await db.verlauf_schreiben(
        fall_id, "Gutachten", "orchestrator (Temporal)", "WARTET",
        "Gutachtenauftrag an die ärztliche Praxis versendet. Der Workflow "
        "wartet jetzt auf das Gutachten – im Dashboard hochladen.",
        {"empfaenger": "Fachärztliche Praxis",
         "koerperteil": (fall or {}).get("koerperteil"),
         "frist_sekunden": config.FRIST_GUTACHTEN,
         "hinweis": "Temporal hält den Fall an dieser Stelle fest. Das Warten "
                    "kostet keine Rechenzeit und übersteht einen Neustart."},
    )
    log.info("Fall %s: Gutachten beauftragt, warte auf Eingang", fall_id)


@activity.defn(name="entgeltmeldung_anfordern")
async def entgeltmeldung_anfordern(fall_id: str) -> None:
    """Fordert die Entgeltmeldung an. Ab hier wartet der Workflow erneut."""
    fall = await db.fall_lesen(fall_id)
    await db.verlauf_schreiben(
        fall_id, "Entgeltmeldung", "orchestrator (Temporal)", "WARTET",
        "Anfrage zum Jahresarbeitsverdienst an Unternehmer und Versicherten "
        "versendet. Der Workflow wartet auf die Antwort.",
        {"empfaenger": ["Unternehmer", "Versicherte Person"],
         "versicherter": (fall or {}).get("versicherter"),
         "frist_sekunden": config.FRIST_ENTGELTMELDUNG},
    )
    log.info("Fall %s: Entgeltmeldung angefordert, warte auf Eingang", fall_id)


@activity.defn(name="erinnerung_versenden")
async def erinnerung_versenden(fall_id: str, vorgang: str, empfaenger: str,
                               nummer: int) -> None:
    """Läuft die Frist ab, ohne dass etwas eingeht: erinnern und weiter warten."""
    schritt, betreff = VORGANG_TEXTE.get(vorgang, (vorgang, vorgang))
    await db.verlauf_schreiben(
        fall_id, schritt, "orchestrator (Temporal)", "ERINNERUNG",
        f"{nummer}. Erinnerung an {empfaenger} versendet – {betreff} steht "
        f"weiterhin aus. Der Workflow wartet weiter.",
        {"vorgang": vorgang, "erinnerung_nummer": nummer,
         "ausgeloest_durch": "Temporal-Timer"},
    )
    log.info("Fall %s: %s. Erinnerung zu '%s' versendet", fall_id, nummer, vorgang)


@activity.defn(name="schritt_melden")
async def schritt_melden(fall_id: str, status: str | None, schritt: str,
                         komponente: str, zustand: str, text: str,
                         details: dict | None = None) -> None:
    """Generischer Verlaufseintrag fuer den Prozess-Interpreter.

    Setzt optional den Fallstatus (damit das Dashboard weiss, worauf der Fall
    gerade wartet) und schreibt eine Zeile in den Verlauf.
    """
    if status:
        await db.fall_aktualisieren(fall_id, status=status)
    await db.verlauf_schreiben(
        fall_id, schritt, komponente, zustand, text,
        {**(details or {}),
         "temporal_workflow": activity.info().workflow_id,
         "temporal_activity": activity.info().activity_type},
    )
    log.info("Fall %s: %s [%s] %s", fall_id, schritt, zustand, text)


@activity.defn(name="fall_abschliessen")
async def fall_abschliessen(fall_id: str) -> None:
    await db.fall_aktualisieren(fall_id, status="ABGESCHLOSSEN")
    fall = await db.fall_lesen(fall_id)
    rente = float(fall["rente_monat"]) if fall and fall.get("rente_monat") else 0.0
    await db.verlauf_schreiben(
        fall_id, "Ergebnis", "orchestrator (Temporal)", "ABGESCHLOSSEN",
        f"Fall abgeschlossen. Monatliche Verletztenrente: {euro(rente)} EUR",
        {"rente_monat_eur": rente,
         "rente_jahr_eur": fall.get("rente_jahr") if fall else None},
    )
    log.info("Fall %s abgeschlossen", fall_id)


async def main() -> None:
    await db.pool()
    client = await verbinde()
    worker = Worker(
        client,
        task_queue=config.QUEUE_ORCHESTRATOR,
        # "Prozess" ist der Graph-Interpreter (Designer); "UnfallSachbearbeitung"
        # bleibt registriert, damit Faelle aus der Zeit davor weiterlaufen.
        workflows=[ProzessWorkflow, UnfallSachbearbeitungWorkflow],
        activities=[status_setzen, gutachten_beauftragen,
                    entgeltmeldung_anfordern, erinnerung_versenden,
                    schritt_melden, fall_abschliessen],
    )
    log.info("Orchestrator-Worker laeuft auf Queue '%s'", config.QUEUE_ORCHESTRATOR)
    await worker.run()


if __name__ == "__main__":
    asyncio.run(main())
