"""Tests fuer Graph-Format, Validierung und den Interpreter-Workflow.

Ausfuehren (aus services/app):
    pip install temporalio==1.9.0 pytest
    python -m pytest tests -q

Der Workflow-Test nutzt Temporals Zeitraffer-Testserver (wird beim ersten
Lauf heruntergeladen). Die Activities sind hier Attrappen: Es geht um den
Ablauf durch den Graphen, nicht um die Fachlogik der Dienste.
"""
import asyncio
import os
import sys
import uuid
from datetime import timedelta

import pytest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from common import prozess  # noqa: E402


# --- reine Funktionen -------------------------------------------------------

def test_standardgraph_ist_gueltig():
    ergebnis = prozess.pruefen(prozess.standard_graph())
    assert ergebnis == {"fehler": [], "warnungen": []}


def test_pruefung_findet_typische_fehler():
    g = {
        "knoten": [
            {"id": "s", "typ": "start"},
            {"id": "b", "typ": "bedingung", "titel": "B",
             "bedingung": {"links": "mde.mde_prozent", "op": "<", "rechts": 20}},
            {"id": "a", "typ": "aktivitaet", "aktivitaet": "gibt_es_nicht"},
            {"id": "e", "typ": "ende"},
        ],
        "kanten": [{"von": "s", "nach": "b"}, {"von": "b", "nach": "e", "port": "ja"},
                   {"von": "a", "nach": "e"}],
    }
    f = prozess.pruefen(g)
    assert any("Ausgang 'nein'" in x for x in f["fehler"])
    assert any("unbekannte Aktivität" in x for x in f["fehler"])


def test_pruefung_ohne_ende_und_mit_zwei_starts():
    g = {"knoten": [{"id": "a", "typ": "start"}, {"id": "b", "typ": "start"}],
         "kanten": [{"von": "a", "nach": "b"}]}
    f = prozess.pruefen(g)["fehler"]
    assert any("genau einen Start" in x for x in f)
    assert any("Ende-Knoten" in x for x in f)


def test_schleife_ist_nur_warnung():
    g = {"knoten": [{"id": "s", "typ": "start"}, {"id": "n", "typ": "notiz", "text": "x"},
                    {"id": "e", "typ": "ende"}],
         "kanten": [{"von": "s", "nach": "n"}, {"von": "n", "nach": "n"}]}
    # n hat einen Ausgang (auf sich selbst), also kein Fehler – aber Zyklus + e unerreichbar
    f = prozess.pruefen(g)
    assert f["fehler"] == []
    assert any("Schleife" in w for w in f["warnungen"])
    assert any("nicht erreichbar" in w for w in f["warnungen"])


def test_pfade_bedingungen_und_prompts():
    kontext = {"fall": {"id": "1", "beruf": "Dachdecker"},
               "mde": {"mde_prozent": 30, "konfidenz": 0.9}}
    assert prozess.wert_aus_pfad(kontext, "mde.mde_prozent") == 30
    assert prozess.wert_aus_pfad(kontext, "mde.gibt_es_nicht") is None
    assert prozess.wert_aus_pfad(kontext, "nix.da") is None
    assert prozess.bedingung_auswerten(kontext, {"links": "mde.mde_prozent", "op": ">=", "rechts": 20})[0]
    assert not prozess.bedingung_auswerten(kontext, {"links": "mde.mde_prozent", "op": "<", "rechts": "20"})[0]
    assert prozess.bedingung_auswerten(kontext, {"links": "fall.beruf", "op": "==", "rechts": "Dachdecker"})[0]
    # fehlender Wert vergleicht als Text "" – kein Absturz
    assert prozess.bedingung_auswerten(kontext, {"links": "jav.jav_euro", "op": "==", "rechts": ""})[0]
    assert prozess.prompt_fuellen(kontext, "MdE {{ mde.mde_prozent }} % bei {{fall.beruf}}, {{x.y}}") \
        == "MdE 30 % bei Dachdecker, (unbekannt)"


def test_vervollstaendigen_nimmt_katalogwerte():
    k = prozess.knoten_vervollstaendigen({"id": "k", "typ": "aktivitaet",
                                          "aktivitaet": "berechne_rente"})
    assert k["queue"] == prozess.config.QUEUE_RENTE
    assert [e["pfad"] for e in k["eingaben"]] == ["fall.id", "jav.jav_euro", "mde.mde_prozent"]
    assert k["ergebnis"] == "rente"
    s = prozess.knoten_vervollstaendigen({"id": "s", "typ": "signal",
                                          "signal": "gutachten_eingegangen", "frist": 5})
    assert s["status"] == "WARTE_AUF_GUTACHTEN" and s["frist"] == 5


# --- Interpreter-Workflow gegen den Temporal-Testserver ---------------------

def _graph_mit_verzweigung() -> dict:
    """Start → Gutachten beauftragen → Warten → MdE → Bedingung (MdE >= 20)
       ja → Rente berechnen → Ende;  nein → Notiz → Ende"""
    g = {
        "name": "Test",
        "knoten": [
            {"id": "s", "typ": "start", "titel": "Start"},
            {"id": "a1", "typ": "aktivitaet", "aktivitaet": "gutachten_beauftragen"},
            {"id": "w", "typ": "signal", "signal": "gutachten_eingegangen", "frist": 3},
            {"id": "a2", "typ": "aktivitaet", "aktivitaet": "ermittle_mde"},
            {"id": "b", "typ": "bedingung", "titel": "MdE ab 20?",
             "bedingung": {"links": "mde.mde_prozent", "op": ">=", "rechts": 20}},
            {"id": "a3", "typ": "aktivitaet", "aktivitaet": "berechne_rente",
             "eingaben": [{"pfad": "fall.id"}, {"wert": 48000}, {"pfad": "mde.mde_prozent"}]},
            {"id": "n", "typ": "notiz", "text": "Bagatelle: MdE {{mde.mde_prozent}} %"},
            {"id": "e", "typ": "ende"},
        ],
        "kanten": [
            {"von": "s", "nach": "a1"}, {"von": "a1", "nach": "w"}, {"von": "w", "nach": "a2"},
            {"von": "a2", "nach": "b"}, {"von": "b", "nach": "a3", "port": "ja"},
            {"von": "b", "nach": "n", "port": "nein"}, {"von": "a3", "nach": "e"},
            {"von": "n", "nach": "e"},
        ],
    }
    assert prozess.pruefen(g)["fehler"] == []
    return g


async def _testumgebung():
    """Zeitraffer-Testserver von Temporal – oder ein laufender Server.

    Standard: `WorkflowEnvironment.start_time_skipping()` laedt den Testserver
    beim ersten Mal herunter. Ohne Internet zeigt TEMPORAL_TEST_SERVER auf ein
    vorhandenes Binary, oder TEMPORAL_TEST_ADDRESS (z. B. localhost:7233) auf
    einen laufenden Server – dann laeuft der Test in Echtzeit (ein paar
    Sekunden).
    """
    from temporalio.client import Client
    from temporalio.testing import WorkflowEnvironment

    adresse = os.getenv("TEMPORAL_TEST_ADDRESS")
    if adresse:
        return WorkflowEnvironment.from_client(await Client.connect(adresse))
    return await WorkflowEnvironment.start_time_skipping(
        test_server_existing_path=os.getenv("TEMPORAL_TEST_SERVER") or None)


@pytest.mark.parametrize("mde,erwarteter_pfad", [
    (30, ["s", "a1", "w", "a2", "b", "a3", "e"]),
    (10, ["s", "a1", "w", "a2", "b", "n", "e"]),
])
def test_interpreter_laeuft_graph_ab(mde, erwarteter_pfad):
    asyncio.run(_interpreter_lauf(mde, erwarteter_pfad))


async def _interpreter_lauf(mde: int, erwarteter_pfad: list[str]) -> None:
    from temporalio import activity
    from temporalio.client import Client
    from temporalio.worker import Worker

    from common import config
    from orchestrator.prozess_workflow import ProzessWorkflow

    aufrufe: list[tuple] = []

    @activity.defn(name="schritt_melden")
    async def schritt_melden(fall_id, status, schritt, komponente, zustand, text, details=None):
        aufrufe.append(("schritt_melden", status, schritt, zustand))

    @activity.defn(name="gutachten_beauftragen")
    async def gutachten_beauftragen(fall_id):
        aufrufe.append(("gutachten_beauftragen", fall_id))

    @activity.defn(name="erinnerung_versenden")
    async def erinnerung_versenden(fall_id, vorgang, empfaenger, nummer):
        aufrufe.append(("erinnerung", vorgang, nummer))

    @activity.defn(name="fall_abschliessen")
    async def fall_abschliessen(fall_id):
        aufrufe.append(("fall_abschliessen", fall_id))

    @activity.defn(name="ermittle_mde")
    async def ermittle_mde(fall, dokument):
        aufrufe.append(("ermittle_mde", dokument["dokument_id"]))
        return {"mde_prozent": mde, "konfidenz": 0.8}

    @activity.defn(name="berechne_rente")
    async def berechne_rente(fall_id, jav_euro, mde_prozent):
        aufrufe.append(("berechne_rente", jav_euro, mde_prozent))
        return {"rente_monat_euro": round(jav_euro * 2 / 3 * mde_prozent / 100 / 12, 2)}

    async with await _testumgebung() as env:
        client: Client = env.client
        # Alle Queues auf einen Test-Worker legen – im echten System sind das
        # getrennte Dienste, hier reicht es, dass die Namen stimmen.
        queues = {config.QUEUE_ORCHESTRATOR, config.QUEUE_MDE, config.QUEUE_RENTE}
        workers = [Worker(client, task_queue=q,
                          workflows=[ProzessWorkflow] if q == config.QUEUE_ORCHESTRATOR else [],
                          activities=[schritt_melden, gutachten_beauftragen,
                                      erinnerung_versenden, fall_abschliessen,
                                      ermittle_mde, berechne_rente])
                   for q in queues]
        async with workers[0], workers[1], workers[2]:
            fall = {"id": str(uuid.uuid4()), "fall_nummer": "UV-TEST", "beruf": "Dachdecker"}
            handle = await client.start_workflow(
                ProzessWorkflow.run,
                args=[fall, {"name": "Test", "version": 7, "graph": _graph_mit_verzweigung()}],
                id=f"test-{fall['id']}", task_queue=config.QUEUE_ORCHESTRATOR,
            )
            # Der Workflow steht jetzt im Signal-Knoten. Nach Ablauf der Frist
            # (3 s) muss eine Erinnerung verschickt worden sein.
            await env.sleep(timedelta(seconds=8))
            zustand = await handle.query("zustand")
            assert zustand["knoten_status"]["w"] == "wartet"
            assert zustand["wartet_auf"] == "Gutachten der Ärztin/des Arztes"
            assert zustand["erinnerungen"]["gutachten"] >= 1
            assert zustand["prozess_version"] == 7

            await handle.signal("gutachten_eingegangen", {"dokument_id": "dok-1"})
            ergebnis = await handle.result()
            # Queries brauchen einen Worker – also noch innerhalb des Blocks.
            zustand = await handle.query("zustand")

    assert ergebnis["pfad"] == erwarteter_pfad
    assert ("ermittle_mde", "dok-1") in aufrufe
    if mde >= 20:
        assert ("berechne_rente", 48000, mde) in aufrufe          # Literal + Pfad
        assert ergebnis["rente"]["rente_monat_euro"] > 0
    else:
        assert not any(a[0] == "berechne_rente" for a in aufrufe)
        assert any(a[0] == "schritt_melden" and a[3] == "NOTIZ" for a in aufrufe)
    assert aufrufe[-1] == ("fall_abschliessen", fall["id"])
    assert zustand["aktueller_schritt"] == "ABGESCHLOSSEN"
    assert all(zustand["knoten_status"][k] == "fertig" for k in erwarteter_pfad)
    assert "b" in zustand["entscheidungen"]
