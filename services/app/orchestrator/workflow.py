"""Der Temporal-Workflow: Gutachten → MdE → Entgeltmeldung → JAV → Rente.

Der interessante Teil sind die beiden Wartezeiten. Ein Gutachten kommt in der
Wirklichkeit nach Wochen zurück, eine Entgeltmeldung nach Tagen. Der Workflow
wartet währenddessen einfach – ohne dass irgendwo ein Prozess blockiert oder
ein Zustand verloren geht. Temporal hält den Fall an genau dieser Stelle fest;
das Warten überlebt einen Neustart aller Dienste.

Geweckt wird der Workflow durch ein Signal von außen (Dokument hochgeladen,
Entgeltmeldung erfasst). Kommt nichts, löst nach einer Frist ein Timer eine
Erinnerung aus – danach wird weiter gewartet.

Diese Datei enthält keine Fachlogik und keinen Datenbankzugriff; sie steuert
nur die Reihenfolge. Die Arbeit machen die Activities in eigenen Services.
"""
import asyncio
from datetime import timedelta
from typing import Callable

from temporalio import workflow
from temporalio.common import RetryPolicy

with workflow.unsafe.imports_passed_through():
    from common import config

STANDARD_RETRY = RetryPolicy(
    initial_interval=timedelta(seconds=1),
    maximum_attempts=3,
)

KURZ = timedelta(seconds=30)
LANG = timedelta(seconds=120)


@workflow.defn(name="UnfallSachbearbeitung")
class UnfallSachbearbeitungWorkflow:
    """Bearbeitet einen Unfall vom Eingang bis zum berechneten Rentenbetrag."""

    def __init__(self) -> None:
        self._gutachten: dict | None = None
        self._entgeltmeldung: dict | None = None
        self._zustand: dict = {
            "aktueller_schritt": "START",
            "erledigte_schritte": [],
            "wartet_auf": None,
            "erinnerungen": {"gutachten": 0, "entgeltmeldung": 0},
            "mde": None,
            "jav": None,
            "rente": None,
        }

    # --- Signale von außen -------------------------------------------------

    @workflow.signal(name="gutachten_eingegangen")
    def gutachten_eingegangen(self, dokument: dict) -> None:
        """Wird gerufen, wenn im Dashboard ein Gutachten hochgeladen wurde."""
        self._gutachten = dokument

    @workflow.signal(name="entgeltmeldung_eingegangen")
    def entgeltmeldung_eingegangen(self, meldung: dict) -> None:
        """Wird gerufen, wenn Unternehmer oder Versicherter geantwortet haben."""
        self._entgeltmeldung = meldung

    # --- Abfrage von außen -------------------------------------------------

    @workflow.query(name="zustand")
    def zustand(self) -> dict:
        """Von außen abfragbarer Workflow-Zustand (Temporal Query)."""
        return self._zustand

    # --- Ablauf ------------------------------------------------------------

    @workflow.run
    async def run(self, fall: dict) -> dict:
        fall_id = fall["id"]

        # --- Schritt 1: Gutachten beauftragen und darauf warten ------------
        await self._status(fall_id, "WARTE_AUF_GUTACHTEN")
        await workflow.execute_activity(
            "gutachten_beauftragen", args=[fall_id],
            task_queue=config.QUEUE_ORCHESTRATOR,
            start_to_close_timeout=KURZ, retry_policy=STANDARD_RETRY,
        )
        self._zustand["wartet_auf"] = "Gutachten der Ärztin/des Arztes"
        await self._warte_auf(
            pruefung=lambda: self._gutachten is not None,
            frist=config.FRIST_GUTACHTEN,
            vorgang="gutachten",
            fall_id=fall_id,
            empfaenger="die begutachtende Praxis",
        )
        self._zustand["wartet_auf"] = None
        self._zustand["erledigte_schritte"].append("GUTACHTEN_EINGEGANGEN")

        # --- Schritt 2: MdE aus dem Gutachten ermitteln --------------------
        await self._status(fall_id, "MDE_ERMITTLUNG")
        mde = await workflow.execute_activity(
            "ermittle_mde", args=[fall, self._gutachten],
            task_queue=config.QUEUE_MDE,
            start_to_close_timeout=LANG, retry_policy=STANDARD_RETRY,
        )
        self._zustand["mde"] = mde
        self._zustand["erledigte_schritte"].append("MDE_ERMITTLUNG")

        # --- Schritt 3: Entgeltmeldung anfordern und darauf warten ---------
        await self._status(fall_id, "WARTE_AUF_ENTGELTMELDUNG")
        await workflow.execute_activity(
            "entgeltmeldung_anfordern", args=[fall_id],
            task_queue=config.QUEUE_ORCHESTRATOR,
            start_to_close_timeout=KURZ, retry_policy=STANDARD_RETRY,
        )
        self._zustand["wartet_auf"] = "Entgeltmeldung von Unternehmer und Versichertem"
        await self._warte_auf(
            pruefung=lambda: self._entgeltmeldung is not None,
            frist=config.FRIST_ENTGELTMELDUNG,
            vorgang="entgeltmeldung",
            fall_id=fall_id,
            empfaenger="Unternehmer und Versicherten",
        )
        self._zustand["wartet_auf"] = None
        self._zustand["erledigte_schritte"].append("ENTGELTMELDUNG_EINGEGANGEN")

        # --- Schritt 4: JAV aus der Meldung ermitteln ----------------------
        await self._status(fall_id, "JAV_ERMITTLUNG")
        jav = await workflow.execute_activity(
            "ermittle_jav", args=[fall, self._entgeltmeldung],
            task_queue=config.QUEUE_JAV,
            start_to_close_timeout=LANG, retry_policy=STANDARD_RETRY,
        )
        self._zustand["jav"] = jav
        self._zustand["erledigte_schritte"].append("JAV_ERMITTLUNG")

        # --- Schritt 5: Rentenberechnung (deterministisch, KEIN Agent) -----
        await self._status(fall_id, "RENTENBERECHNUNG")
        rente = await workflow.execute_activity(
            "berechne_rente",
            args=[fall_id, jav["jav_euro"], mde["mde_prozent"]],
            task_queue=config.QUEUE_RENTE,
            start_to_close_timeout=KURZ, retry_policy=STANDARD_RETRY,
        )
        self._zustand["rente"] = rente
        self._zustand["erledigte_schritte"].append("RENTENBERECHNUNG")

        # --- Abschluss ------------------------------------------------------
        self._zustand["aktueller_schritt"] = "ABGESCHLOSSEN"
        await workflow.execute_activity(
            "fall_abschliessen", args=[fall_id],
            task_queue=config.QUEUE_ORCHESTRATOR,
            start_to_close_timeout=KURZ, retry_policy=STANDARD_RETRY,
        )
        return {"fall_id": fall_id, "mde": mde, "jav": jav, "rente": rente}

    # --- Hilfen ------------------------------------------------------------

    async def _status(self, fall_id: str, status: str) -> None:
        self._zustand["aktueller_schritt"] = status
        await workflow.execute_activity(
            "status_setzen", args=[fall_id, status],
            task_queue=config.QUEUE_ORCHESTRATOR,
            start_to_close_timeout=KURZ, retry_policy=STANDARD_RETRY,
        )

    async def _warte_auf(self, pruefung: Callable[[], bool], frist: float,
                         vorgang: str, fall_id: str, empfaenger: str) -> None:
        """Wartet, bis das Signal eintrifft – notfalls unbegrenzt lange.

        Läuft die Frist ab, wird eine Erinnerung verschickt und weiter gewartet.
        """
        while not pruefung():
            try:
                await workflow.wait_condition(
                    pruefung, timeout=timedelta(seconds=frist)
                )
            except asyncio.TimeoutError:
                self._zustand["erinnerungen"][vorgang] += 1
                nummer = self._zustand["erinnerungen"][vorgang]
                await workflow.execute_activity(
                    "erinnerung_versenden",
                    args=[fall_id, vorgang, empfaenger, nummer],
                    task_queue=config.QUEUE_ORCHESTRATOR,
                    start_to_close_timeout=KURZ, retry_policy=STANDARD_RETRY,
                )
