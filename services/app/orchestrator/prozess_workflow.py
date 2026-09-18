"""Der generische Prozess-Workflow: läuft einen Graphen aus dem Designer ab.

Statt einer fest programmierten Reihenfolge (siehe workflow.py) bekommt dieser
Workflow den Graphen als **Eingabe** und interpretiert ihn Knoten für Knoten.
Die eigentliche Arbeit machen weiterhin die Activities in den Diensten.

Versionierung – der wichtigste Punkt:
    Der Graph wird beim Start des Falls als Argument übergeben und liegt damit
    in der Temporal-Historie des Falls. Wird der Prozess im Designer später
    geändert, entsteht eine NEUE Version; laufende Fälle behalten ihre alte.
    Temporal spielt bei einem Neustart die Historie deterministisch nach, und
    weil der Graph Teil dieser Historie ist, kommt immer dasselbe heraus.
    Nichts in diesem Workflow liest den Graphen aus der Datenbank.

Signale von außen (Gutachten hochgeladen, Entgeltmeldung erfasst) werden
dynamisch entgegengenommen: Jeder Signalname wird im Postfach abgelegt, und
ein Signal-Knoten wartet auf den Namen, der in ihm eingetragen ist.
"""
import asyncio
from datetime import timedelta
from typing import Sequence

from temporalio import workflow
from temporalio.common import RawValue, RetryPolicy

with workflow.unsafe.imports_passed_through():
    from common import config, prozess

STANDARD_RETRY = RetryPolicy(
    initial_interval=timedelta(seconds=1),
    maximum_attempts=3,
)
KURZ = timedelta(seconds=30)
LANG = timedelta(seconds=120)
KOMPONENTE = "orchestrator (Prozess-Interpreter)"


@workflow.defn(name="Prozess")
class ProzessWorkflow:
    """Interpretiert einen Prozessgraphen für genau einen Fall."""

    def __init__(self) -> None:
        self._postfach: dict[str, dict] = {}
        self._kontext: dict = {}
        self._zustand: dict = {
            "prozess_name": None,
            "prozess_version": None,
            "aktueller_schritt": "START",
            "aktueller_knoten": None,
            "knoten_status": {},        # id -> fertig | aktiv | wartet | fehler
            "pfad": [],                 # besuchte Knoten in Reihenfolge
            "erledigte_schritte": [],
            "wartet_auf": None,
            "erinnerungen": {},
            "entscheidungen": {},       # id -> Erklärung der Bedingung
            "mde": None, "jav": None, "rente": None,
        }

    # --- Signale von außen (beliebiger Name) ------------------------------

    @workflow.signal(dynamic=True)
    def signal_empfangen(self, name: str, args: Sequence[RawValue]) -> None:
        nutzlast: dict = {}
        if args:
            nutzlast = workflow.payload_converter().from_payload(args[0].payload, dict)
        self._postfach[name] = nutzlast

    # --- Abfrage von außen ------------------------------------------------

    @workflow.query(name="zustand")
    def zustand(self) -> dict:
        return self._zustand

    @workflow.query(name="kontext")
    def kontext(self) -> dict:
        return self._kontext

    # --- Ablauf -----------------------------------------------------------

    @workflow.run
    async def run(self, fall: dict, definition: dict) -> dict:
        graph = definition["graph"]
        self._zustand["prozess_name"] = definition.get("name")
        self._zustand["prozess_version"] = definition.get("version")
        self._kontext = {"fall": fall}
        fall_id = fall["id"]

        knoten = {k["id"]: prozess.knoten_vervollstaendigen(k) for k in graph["knoten"]}
        kanten: dict[str, dict[str, str]] = {}
        for kante in graph["kanten"]:
            kanten.setdefault(kante["von"], {})[kante.get("port") or "weiter"] = kante["nach"]

        aktuell = next(k["id"] for k in graph["knoten"] if k["typ"] == "start")
        schritte = 0
        try:
            while True:
                schritte += 1
                if schritte > prozess.MAX_SCHRITTE:
                    raise RuntimeError(f"Abbruch: mehr als {prozess.MAX_SCHRITTE} "
                                       "Schritte – Schleife im Graphen?")
                k = knoten[aktuell]
                self._zustand["aktueller_knoten"] = aktuell
                self._zustand["pfad"].append(aktuell)
                self._zustand["knoten_status"][aktuell] = "aktiv"

                port = await self._knoten_ausfuehren(fall_id, k)
                self._zustand["knoten_status"][aktuell] = "fertig"
                if k["typ"] == "ende":
                    break
                self._zustand["erledigte_schritte"].append(k.get("titel") or aktuell)
                aktuell = kanten.get(aktuell, {}).get(port)
                if aktuell is None:
                    raise RuntimeError(f"Knoten '{k.get('titel')}' hat keinen "
                                       f"Ausgang '{port}'")
        except Exception as e:  # noqa: BLE001
            self._zustand["knoten_status"][aktuell] = "fehler"
            self._zustand["aktueller_schritt"] = "FEHLER"
            await self._melden(fall_id, "FEHLER", knoten[aktuell].get("titel", aktuell),
                               "FEHLER", f"Prozess abgebrochen: {e}",
                               {"knoten": aktuell})
            raise

        self._zustand["aktueller_schritt"] = "ABGESCHLOSSEN"
        self._zustand["aktueller_knoten"] = None
        return {
            "fall_id": fall_id,
            "mde": self._zustand["mde"], "jav": self._zustand["jav"],
            "rente": self._zustand["rente"],
            "pfad": self._zustand["pfad"],
        }

    # --- Ein Knoten -------------------------------------------------------

    async def _knoten_ausfuehren(self, fall_id: str, k: dict) -> str:
        """Führt einen Knoten aus und liefert den Namen des Ausgangs."""
        typ = k["typ"]
        titel = k.get("titel") or k["id"]

        if typ == "start":
            return "weiter"

        if typ == "aktivitaet":
            if k.get("status"):
                await self._status(fall_id, k["status"], titel)
            args = [prozess.eingabe_aufloesen(self._kontext, e) for e in k["eingaben"]]
            ergebnis = await workflow.execute_activity(
                k["aktivitaet"], args=args, task_queue=k["queue"],
                start_to_close_timeout=LANG if k.get("dauer") == "lang" else KURZ,
                retry_policy=STANDARD_RETRY,
            )
            self._ergebnis_ablegen(k.get("ergebnis"), ergebnis)
            return "weiter"

        if typ == "signal":
            await self._status(fall_id, k["status"], titel)
            self._zustand["wartet_auf"] = k.get("wartet_auf") or titel
            await self._warte_auf_signal(fall_id, k)
            self._zustand["wartet_auf"] = None
            self._ergebnis_ablegen(k.get("ergebnis"), self._postfach.get(k["signal"]))
            return "weiter"

        if typ == "bedingung":
            ergebnis, erklaerung = prozess.bedingung_auswerten(
                self._kontext, k.get("bedingung") or {})
            self._zustand["entscheidungen"][k["id"]] = erklaerung
            await self._melden(fall_id, None, titel, "ENTSCHIEDEN",
                               f"Bedingung geprüft: {erklaerung}",
                               {"bedingung": k.get("bedingung"), "ergebnis": ergebnis})
            return "ja" if ergebnis else "nein"

        if typ == "agent":
            await self._status(fall_id, "AGENT", titel)
            frage = prozess.prompt_fuellen(self._kontext, k.get("prompt", ""))
            ergebnis = await workflow.execute_activity(
                "agent_fragen", args=[fall_id, titel, frage],
                task_queue=k.get("queue") or config.QUEUE_AGENT,
                start_to_close_timeout=LANG, retry_policy=STANDARD_RETRY,
            )
            self._ergebnis_ablegen(k.get("ergebnis"), ergebnis)
            return "weiter"

        if typ == "notiz":
            text = prozess.prompt_fuellen(self._kontext, k.get("text", ""))
            await self._melden(fall_id, None, titel, "NOTIZ", text or titel, {})
            return "weiter"

        if typ == "ende":
            self._zustand["aktueller_schritt"] = "ABGESCHLOSSEN"
            await workflow.execute_activity(
                "fall_abschliessen", args=[fall_id],
                task_queue=config.QUEUE_ORCHESTRATOR,
                start_to_close_timeout=KURZ, retry_policy=STANDARD_RETRY,
            )
            return "weiter"

        raise RuntimeError(f"Unbekannter Knotentyp {typ!r}")

    # --- Hilfen -----------------------------------------------------------

    def _ergebnis_ablegen(self, name: str | None, wert) -> None:
        if not name:
            return
        self._kontext[name] = wert
        if name in ("mde", "jav", "rente"):
            self._zustand[name] = wert

    async def _status(self, fall_id: str, status: str, titel: str) -> None:
        self._zustand["aktueller_schritt"] = status
        await self._melden(fall_id, status, titel, "GESTARTET",
                           f"Prozessschritt '{titel}' gestartet", {})

    async def _melden(self, fall_id: str, status: str | None, schritt: str,
                      zustand: str, text: str, details: dict) -> None:
        await workflow.execute_activity(
            "schritt_melden",
            args=[fall_id, status, schritt, KOMPONENTE, zustand, text, details],
            task_queue=config.QUEUE_ORCHESTRATOR,
            start_to_close_timeout=KURZ, retry_policy=STANDARD_RETRY,
        )

    async def _warte_auf_signal(self, fall_id: str, k: dict) -> None:
        """Wartet auf das Signal – notfalls unbegrenzt, mit Erinnerungen."""
        name = k["signal"]
        vorgang = k.get("vorgang") or name
        self._postfach.pop(name, None)   # ein altes Signal zählt nicht
        self._zustand["knoten_status"][k["id"]] = "wartet"
        frist = float(k.get("frist") or 30)
        while name not in self._postfach:
            try:
                await workflow.wait_condition(
                    lambda: name in self._postfach, timeout=timedelta(seconds=frist))
            except asyncio.TimeoutError:
                nummer = self._zustand["erinnerungen"].get(vorgang, 0) + 1
                self._zustand["erinnerungen"][vorgang] = nummer
                await workflow.execute_activity(
                    "erinnerung_versenden",
                    args=[fall_id, vorgang, k.get("empfaenger") or "die Beteiligten", nummer],
                    task_queue=config.QUEUE_ORCHESTRATOR,
                    start_to_close_timeout=KURZ, retry_policy=STANDARD_RETRY,
                )
