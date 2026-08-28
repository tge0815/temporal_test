"""Der Temporal-Workflow: MdE -> JAV -> Rentenberechnung.

Wichtig: Diese Datei enthaelt KEINE Fachlogik und KEINEN Datenbankzugriff.
Sie steuert nur die Reihenfolge der Schritte. Die eigentliche Arbeit machen
die Activities, die in eigenen Services (eigene Task-Queues) laufen.
"""
from datetime import timedelta

from temporalio import workflow
from temporalio.common import RetryPolicy

with workflow.unsafe.imports_passed_through():
    from common import config

STANDARD_RETRY = RetryPolicy(
    initial_interval=timedelta(seconds=1),
    maximum_attempts=3,
)


@workflow.defn(name="UnfallSachbearbeitung")
class UnfallSachbearbeitungWorkflow:
    """Bearbeitet einen Unfall vom Eingang bis zum berechneten Rentenbetrag."""

    def __init__(self) -> None:
        self._zustand: dict = {
            "aktueller_schritt": "START",
            "erledigte_schritte": [],
            "mde": None,
            "jav": None,
            "rente": None,
        }

    @workflow.query(name="zustand")
    def zustand(self) -> dict:
        """Von aussen abfragbarer Workflow-Zustand (Temporal Query)."""
        return self._zustand

    @workflow.run
    async def run(self, fall: dict) -> dict:
        fall_id = fall["id"]

        # --- Schritt 1: MdE-Ermittlung durch den mde-agent ------------------
        self._zustand["aktueller_schritt"] = "MDE_ERMITTLUNG"
        await workflow.execute_activity(
            "status_setzen",
            args=[fall_id, "MDE_ERMITTLUNG"],
            task_queue=config.QUEUE_ORCHESTRATOR,
            start_to_close_timeout=timedelta(seconds=30),
            retry_policy=STANDARD_RETRY,
        )
        mde = await workflow.execute_activity(
            "ermittle_mde",
            args=[fall],
            task_queue=config.QUEUE_MDE,
            start_to_close_timeout=timedelta(seconds=60),
            retry_policy=STANDARD_RETRY,
        )
        self._zustand["mde"] = mde
        self._zustand["erledigte_schritte"].append("MDE_ERMITTLUNG")

        # --- Schritt 2: JAV-Ermittlung durch den jav-agent ------------------
        self._zustand["aktueller_schritt"] = "JAV_ERMITTLUNG"
        await workflow.execute_activity(
            "status_setzen",
            args=[fall_id, "JAV_ERMITTLUNG"],
            task_queue=config.QUEUE_ORCHESTRATOR,
            start_to_close_timeout=timedelta(seconds=30),
            retry_policy=STANDARD_RETRY,
        )
        jav = await workflow.execute_activity(
            "ermittle_jav",
            args=[fall],
            task_queue=config.QUEUE_JAV,
            start_to_close_timeout=timedelta(seconds=60),
            retry_policy=STANDARD_RETRY,
        )
        self._zustand["jav"] = jav
        self._zustand["erledigte_schritte"].append("JAV_ERMITTLUNG")

        # --- Schritt 3: Rentenberechnung (deterministisch, KEIN Agent) -----
        self._zustand["aktueller_schritt"] = "RENTENBERECHNUNG"
        await workflow.execute_activity(
            "status_setzen",
            args=[fall_id, "RENTENBERECHNUNG"],
            task_queue=config.QUEUE_ORCHESTRATOR,
            start_to_close_timeout=timedelta(seconds=30),
            retry_policy=STANDARD_RETRY,
        )
        rente = await workflow.execute_activity(
            "berechne_rente",
            args=[fall_id, jav["jav_euro"], mde["mde_prozent"]],
            task_queue=config.QUEUE_RENTE,
            start_to_close_timeout=timedelta(seconds=60),
            retry_policy=STANDARD_RETRY,
        )
        self._zustand["rente"] = rente
        self._zustand["erledigte_schritte"].append("RENTENBERECHNUNG")

        # --- Abschluss ------------------------------------------------------
        self._zustand["aktueller_schritt"] = "ABGESCHLOSSEN"
        await workflow.execute_activity(
            "fall_abschliessen",
            args=[fall_id],
            task_queue=config.QUEUE_ORCHESTRATOR,
            start_to_close_timeout=timedelta(seconds=30),
            retry_policy=STANDARD_RETRY,
        )
        return {"fall_id": fall_id, "mde": mde, "jav": jav, "rente": rente}
