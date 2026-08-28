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
from orchestrator.workflow import UnfallSachbearbeitungWorkflow

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("orchestrator")

SCHRITT_TEXTE = {
    "MDE_ERMITTLUNG": ("MdE-Ermittlung", "Orchestrator übergibt an mde-agent"),
    "JAV_ERMITTLUNG": ("JAV-Ermittlung", "Orchestrator übergibt an jav-agent"),
    "RENTENBERECHNUNG": ("Rentenberechnung",
                         "Orchestrator übergibt an rentenberechnung (deterministisch)"),
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
        workflows=[UnfallSachbearbeitungWorkflow],
        activities=[status_setzen, fall_abschliessen],
    )
    log.info("Orchestrator-Worker laeuft auf Queue '%s'", config.QUEUE_ORCHESTRATOR)
    await worker.run()


if __name__ == "__main__":
    asyncio.run(main())
