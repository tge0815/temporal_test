"""Das Graph-Format des Prozessdesigners – und alles, was man damit rechnet.

Ein Prozess ist ein Graph aus Knoten und Kanten, gespeichert als JSON:

    {
      "name": "Unfallsachbearbeitung",
      "knoten": [
        {"id": "k1", "typ": "start", "titel": "Unfall eingegangen", "x": 40, "y": 40},
        {"id": "k2", "typ": "aktivitaet", "aktivitaet": "ermittle_mde",
         "eingaben": [{"pfad": "fall"}, {"pfad": "gutachten"}],
         "ergebnis": "mde", "status": "MDE_ERMITTLUNG", ...},
        {"id": "k3", "typ": "bedingung",
         "bedingung": {"links": "mde.mde_prozent", "op": "<", "rechts": 20}},
        ...
      ],
      "kanten": [{"von": "k1", "nach": "k2"}, {"von": "k3", "nach": "k4", "port": "ja"}]
    }

Knotentypen:
    start       genau einer; hier beginnt der Ablauf
    aktivitaet  ruft einen Dienst (Temporal-Activity) auf einer Task-Queue auf
    signal      wartet auf ein Ereignis von außen (Gutachten, Entgeltmeldung),
                mit Frist und Erinnerung
    bedingung   vergleicht einen Wert aus dem Kontext, zwei Ausgänge "ja"/"nein"
    agent       stellt Claude eine Frage mit Daten aus dem Kontext
    notiz       schreibt einen Eintrag in den Verlauf (für Verzweigungen nützlich)
    ende        schließt den Fall ab

Datenfluss: Es gibt einen gemeinsamen **Kontext** (ein Dict). Am Anfang steht
darin nur "fall". Jeder Knoten mit einem "ergebnis"-Namen legt sein Ergebnis
unter diesem Namen ab, spätere Knoten greifen per Pfad darauf zu
("mde.mde_prozent"). Das ist absichtlich simpel – genau so simpel ist es bei
den kommerziellen Werkzeugen unter der Haube auch.

Diese Datei enthält KEINE Ein-/Ausgabe und wird unverändert im Temporal-Workflow
(deterministische Sandbox) benutzt. Deshalb: keine Uhrzeit, kein Zufall,
kein Netz.
"""
from __future__ import annotations

import re
from typing import Any

from . import config

# --- Katalog: welche Bausteine gibt es? --------------------------------------
# Die Palette im Designer entsteht aus dieser Liste. Jede Aktivität ist eine
# echte Temporal-Activity, die in einem der Dienste registriert ist.

AKTIVITAETEN: list[dict] = [
    {
        "name": "gutachten_beauftragen",
        "titel": "Gutachten beauftragen",
        "dienst": "orchestrator",
        "queue": config.QUEUE_ORCHESTRATOR,
        "beschreibung": "Schickt den Gutachtenauftrag an die Praxis.",
        "eingaben": [{"name": "fall_id", "pfad": "fall.id"}],
        "ergebnis": None,
        "status": "WARTE_AUF_GUTACHTEN",
        "dauer": "kurz",
    },
    {
        "name": "ermittle_mde",
        "titel": "MdE ermitteln (mde-agent)",
        "dienst": "mde-agent",
        "queue": config.QUEUE_MDE,
        "beschreibung": "Liest die Minderung der Erwerbsfähigkeit aus dem Gutachten.",
        "eingaben": [{"name": "fall", "pfad": "fall"},
                     {"name": "dokument", "pfad": "gutachten"}],
        "ergebnis": "mde",
        "status": "MDE_ERMITTLUNG",
        "dauer": "lang",
    },
    {
        "name": "entgeltmeldung_anfordern",
        "titel": "Entgeltmeldung anfordern",
        "dienst": "orchestrator",
        "queue": config.QUEUE_ORCHESTRATOR,
        "beschreibung": "Fragt den Jahresverdienst bei Unternehmer und Versichertem an.",
        "eingaben": [{"name": "fall_id", "pfad": "fall.id"}],
        "ergebnis": None,
        "status": "WARTE_AUF_ENTGELTMELDUNG",
        "dauer": "kurz",
    },
    {
        "name": "ermittle_jav",
        "titel": "JAV ermitteln (jav-agent)",
        "dienst": "jav-agent",
        "queue": config.QUEUE_JAV,
        "beschreibung": "Plausibilisiert den gemeldeten Jahresarbeitsverdienst.",
        "eingaben": [{"name": "fall", "pfad": "fall"},
                     {"name": "meldung", "pfad": "entgeltmeldung"}],
        "ergebnis": "jav",
        "status": "JAV_ERMITTLUNG",
        "dauer": "lang",
    },
    {
        "name": "berechne_rente",
        "titel": "Rente berechnen (deterministisch)",
        "dienst": "rentenberechnung",
        "queue": config.QUEUE_RENTE,
        "beschreibung": "Feste Formel: 2/3 × JAV × MdE/100.",
        "eingaben": [{"name": "fall_id", "pfad": "fall.id"},
                     {"name": "jav_euro", "pfad": "jav.jav_euro"},
                     {"name": "mde_prozent", "pfad": "mde.mde_prozent"}],
        "ergebnis": "rente",
        "status": "RENTENBERECHNUNG",
        "dauer": "kurz",
    },
]

# Signale, auf die ein Knoten warten kann. Jedes Signal gehört zu einem
# Status – das Dashboard bietet bei diesem Status die passende Eingabe an.
SIGNALE: list[dict] = [
    {
        "name": "gutachten_eingegangen",
        "titel": "Warten auf Gutachten",
        "status": "WARTE_AUF_GUTACHTEN",
        "ergebnis": "gutachten",
        "vorgang": "gutachten",
        "empfaenger": "die begutachtende Praxis",
        "wartet_auf": "Gutachten der Ärztin/des Arztes",
        "frist": config.FRIST_GUTACHTEN,
    },
    {
        "name": "entgeltmeldung_eingegangen",
        "titel": "Warten auf Entgeltmeldung",
        "status": "WARTE_AUF_ENTGELTMELDUNG",
        "ergebnis": "entgeltmeldung",
        "vorgang": "entgeltmeldung",
        "empfaenger": "Unternehmer und Versicherten",
        "wartet_auf": "Entgeltmeldung von Unternehmer und Versichertem",
        "frist": config.FRIST_ENTGELTMELDUNG,
    },
]

VERGLEICHE = ("<", "<=", ">", ">=", "==", "!=")
KNOTENTYPEN = ("start", "aktivitaet", "signal", "bedingung", "agent",
               "notiz", "ende")

# Sicherheitsnetz gegen Endlosschleifen im Graphen.
MAX_SCHRITTE = 200

# Name des einen Prozesses, den diese Demo kennt. Der Designer versioniert
# ihn; jede Unfallmeldung laeuft mit einer bestimmten Version davon.
PROZESS_NAME = "Unfallsachbearbeitung"


def katalog() -> dict:
    """Alles, was die Palette im Designer wissen muss."""
    return {
        "knotentypen": [
            {"typ": "start", "titel": "Start", "beschreibung":
                "Hier beginnt der Ablauf. Im Kontext liegt 'fall'."},
            {"typ": "aktivitaet", "titel": "Aktivität", "beschreibung":
                "Ruft einen Dienst auf einer Task-Queue auf."},
            {"typ": "signal", "titel": "Warten auf Ereignis", "beschreibung":
                "Hält den Fall an, bis von außen etwas eintrifft. Mit Frist "
                "und Erinnerung."},
            {"typ": "bedingung", "titel": "Bedingung", "beschreibung":
                "Vergleicht einen Wert aus dem Kontext. Zwei Ausgänge: ja / nein."},
            {"typ": "agent", "titel": "Agent (Claude)", "beschreibung":
                "Stellt Claude eine Frage mit Daten aus dem Kontext."},
            {"typ": "notiz", "titel": "Notiz in den Verlauf", "beschreibung":
                "Schreibt einen Eintrag in den Verlauf – z. B. in einem Zweig."},
            {"typ": "ende", "titel": "Ende", "beschreibung":
                "Schließt den Fall ab."},
        ],
        "aktivitaeten": AKTIVITAETEN,
        "signale": SIGNALE,
        "vergleiche": list(VERGLEICHE),
        "claude_aktiv": bool(config.ANTHROPIC_API_KEY),
        "queue_agent": config.QUEUE_AGENT,
    }


def aktivitaet(name: str) -> dict | None:
    return next((a for a in AKTIVITAETEN if a["name"] == name), None)


def signal(name: str) -> dict | None:
    return next((s for s in SIGNALE if s["name"] == name), None)


# --- Der Standardprozess = das, was bisher fest im Code stand ---------------

def standard_graph() -> dict:
    """Gutachten → MdE → Entgeltmeldung → JAV → Rente, als Graph."""
    x = 60
    y = [40 + i * 140 for i in range(8)]
    return {
        "name": PROZESS_NAME,
        "beschreibung": "Der Standardablauf: Gutachten, MdE, Entgeltmeldung, "
                        "JAV, Rente. Entspricht dem fest programmierten "
                        "Workflow der ersten Version.",
        "knoten": [
            {"id": "start", "typ": "start", "titel": "Unfall eingegangen",
             "x": x, "y": y[0]},
            {"id": "k1", "typ": "aktivitaet", "titel": "Gutachten beauftragen",
             "aktivitaet": "gutachten_beauftragen", "x": x, "y": y[1]},
            {"id": "k2", "typ": "signal", "titel": "Warten auf Gutachten",
             "signal": "gutachten_eingegangen", "x": x, "y": y[2]},
            {"id": "k3", "typ": "aktivitaet", "titel": "MdE ermitteln",
             "aktivitaet": "ermittle_mde", "x": x, "y": y[3]},
            {"id": "k4", "typ": "aktivitaet", "titel": "Entgeltmeldung anfordern",
             "aktivitaet": "entgeltmeldung_anfordern", "x": x, "y": y[4]},
            {"id": "k5", "typ": "signal", "titel": "Warten auf Entgeltmeldung",
             "signal": "entgeltmeldung_eingegangen", "x": x, "y": y[5]},
            {"id": "k6", "typ": "aktivitaet", "titel": "JAV ermitteln",
             "aktivitaet": "ermittle_jav", "x": x, "y": y[6]},
            {"id": "k7", "typ": "aktivitaet", "titel": "Rente berechnen",
             "aktivitaet": "berechne_rente", "x": x, "y": y[7]},
            {"id": "ende", "typ": "ende", "titel": "Fall abgeschlossen",
             "x": x, "y": y[7] + 140},
        ],
        "kanten": [
            {"von": "start", "nach": "k1"}, {"von": "k1", "nach": "k2"},
            {"von": "k2", "nach": "k3"}, {"von": "k3", "nach": "k4"},
            {"von": "k4", "nach": "k5"}, {"von": "k5", "nach": "k6"},
            {"von": "k6", "nach": "k7"}, {"von": "k7", "nach": "ende"},
        ],
    }


# --- Knoten "vervollständigen": Defaults aus dem Katalog einsetzen -----------

def knoten_vervollstaendigen(k: dict) -> dict:
    """Füllt fehlende Felder eines Knotens aus dem Katalog auf.

    So muss der Designer nur das Nötigste speichern (welche Aktivität), und
    Eingaben, Queue, Status kommen aus dem Katalog – es sei denn, jemand hat
    sie im Knoten bewusst überschrieben.
    """
    k = dict(k)
    if k.get("typ") == "aktivitaet":
        vorlage = aktivitaet(k.get("aktivitaet", "")) or {}
        k.setdefault("queue", vorlage.get("queue"))
        k.setdefault("eingaben", [{"pfad": e["pfad"]} for e in vorlage.get("eingaben", [])])
        k.setdefault("ergebnis", vorlage.get("ergebnis"))
        k.setdefault("status", vorlage.get("status"))
        k.setdefault("dauer", vorlage.get("dauer", "kurz"))
        k.setdefault("titel", vorlage.get("titel", k.get("aktivitaet")))
    elif k.get("typ") == "signal":
        vorlage = signal(k.get("signal", "")) or {}
        for feld in ("status", "ergebnis", "vorgang", "empfaenger", "wartet_auf", "frist"):
            k.setdefault(feld, vorlage.get(feld))
        k.setdefault("titel", vorlage.get("titel", k.get("signal")))
    elif k.get("typ") == "agent":
        k.setdefault("ergebnis", "agent")
        k.setdefault("queue", config.QUEUE_AGENT)
        k.setdefault("titel", "Agent")
    elif k.get("typ") == "notiz":
        k.setdefault("titel", "Notiz")
    elif k.get("typ") == "bedingung":
        k.setdefault("titel", "Bedingung")
    return k


# --- Validierung ------------------------------------------------------------

def pruefen(graph: dict) -> dict:
    """Prüft einen Graphen. Liefert {'fehler': [...], 'warnungen': [...]}.

    Fehler verhindern das Speichern, Warnungen nicht.
    """
    fehler: list[str] = []
    warnungen: list[str] = []
    if not isinstance(graph, dict):
        return {"fehler": ["Graph muss ein Objekt sein"], "warnungen": []}

    knoten = graph.get("knoten") or []
    kanten = graph.get("kanten") or []
    if not isinstance(knoten, list) or not isinstance(kanten, list):
        return {"fehler": ["'knoten' und 'kanten' müssen Listen sein"], "warnungen": []}

    ids = [k.get("id") for k in knoten]
    if len(ids) != len(set(ids)) or any(not i for i in ids):
        fehler.append("Jeder Knoten braucht eine eindeutige id")
    nach_id = {k.get("id"): k for k in knoten}

    starts = [k for k in knoten if k.get("typ") == "start"]
    if len(starts) != 1:
        fehler.append(f"Es muss genau einen Start-Knoten geben (gefunden: {len(starts)})")
    if not any(k.get("typ") == "ende" for k in knoten):
        fehler.append("Es fehlt ein Ende-Knoten")

    ausgaenge: dict[str, dict[str, str]] = {i: {} for i in ids}
    for kante in kanten:
        von, nach = kante.get("von"), kante.get("nach")
        if von not in nach_id or nach not in nach_id:
            fehler.append(f"Kante {von!r} → {nach!r} zeigt auf einen unbekannten Knoten")
            continue
        port = kante.get("port") or "weiter"
        if port in ausgaenge[von]:
            fehler.append(f"Knoten {_name(nach_id[von])} hat zwei Ausgänge '{port}'")
        ausgaenge[von][port] = nach

    for k in knoten:
        typ = k.get("typ")
        name = _name(k)
        if typ not in KNOTENTYPEN:
            fehler.append(f"{name}: unbekannter Typ {typ!r}")
            continue
        aus = ausgaenge.get(k.get("id"), {})
        if typ == "ende":
            if aus:
                fehler.append(f"{name}: ein Ende-Knoten hat keinen Ausgang")
            continue
        if typ == "bedingung":
            for port in ("ja", "nein"):
                if port not in aus:
                    fehler.append(f"{name}: Ausgang '{port}' ist nicht verbunden")
            b = k.get("bedingung") or {}
            if not b.get("links"):
                fehler.append(f"{name}: kein Vergleichswert (links) angegeben")
            if b.get("op") not in VERGLEICHE:
                fehler.append(f"{name}: unbekannter Vergleich {b.get('op')!r}")
        else:
            if "weiter" not in aus:
                fehler.append(f"{name}: Ausgang ist nicht verbunden")
        if typ == "aktivitaet":
            vorlage = aktivitaet(k.get("aktivitaet", ""))
            if not vorlage:
                fehler.append(f"{name}: unbekannte Aktivität {k.get('aktivitaet')!r}")
            else:
                voll = knoten_vervollstaendigen(k)
                if len(voll["eingaben"]) != len(vorlage["eingaben"]):
                    fehler.append(f"{name}: {vorlage['name']} erwartet "
                                  f"{len(vorlage['eingaben'])} Eingaben")
        if typ == "signal" and not signal(k.get("signal", "")):
            fehler.append(f"{name}: unbekanntes Signal {k.get('signal')!r}")
        if typ == "agent" and not (k.get("prompt") or "").strip():
            fehler.append(f"{name}: der Agent braucht einen Prompt")

    # Erreichbarkeit und Zyklen – nur Warnungen, der Interpreter hat ein Limit.
    if starts and not fehler:
        erreichbar: set[str] = set()
        stapel = [starts[0]["id"]]
        while stapel:
            i = stapel.pop()
            if i in erreichbar:
                continue
            erreichbar.add(i)
            stapel.extend(ausgaenge.get(i, {}).values())
        for k in knoten:
            if k["id"] not in erreichbar:
                warnungen.append(f"{_name(k)} ist vom Start aus nicht erreichbar")
        if _hat_zyklus(ausgaenge, starts[0]["id"]):
            warnungen.append("Der Graph enthält eine Schleife. Der Interpreter "
                             f"bricht nach {MAX_SCHRITTE} Schritten ab.")
    return {"fehler": fehler, "warnungen": warnungen}


def _hat_zyklus(ausgaenge: dict[str, dict[str, str]], start: str) -> bool:
    farbe: dict[str, int] = {}

    def besuche(i: str) -> bool:
        farbe[i] = 1
        for n in ausgaenge.get(i, {}).values():
            if farbe.get(n) == 1 or (farbe.get(n) is None and besuche(n)):
                return True
        farbe[i] = 2
        return False

    return besuche(start)


def _name(k: dict) -> str:
    return f"Knoten '{k.get('titel') or k.get('id')}'"


# --- Kontext: Pfade auflösen, Bedingungen auswerten, Prompts füllen ---------

def wert_aus_pfad(kontext: dict, pfad: str) -> Any:
    """'mde.mde_prozent' → kontext['mde']['mde_prozent']. Fehlt etwas: None."""
    wert: Any = kontext
    for teil in str(pfad).split("."):
        if isinstance(wert, dict):
            wert = wert.get(teil)
        elif isinstance(wert, list) and teil.isdigit():
            wert = wert[int(teil)] if int(teil) < len(wert) else None
        else:
            return None
        if wert is None:
            return None
    return wert


def eingabe_aufloesen(kontext: dict, eingabe: Any) -> Any:
    """Eine Eingabe ist {'pfad': 'a.b'} oder {'wert': 5} (Literal)."""
    if isinstance(eingabe, dict):
        if "pfad" in eingabe:
            return wert_aus_pfad(kontext, eingabe["pfad"])
        return eingabe.get("wert")
    return eingabe


def bedingung_auswerten(kontext: dict, bedingung: dict) -> tuple[bool, str]:
    """Wertet {'links': 'mde.mde_prozent', 'op': '<', 'rechts': 20} aus.

    Liefert (Ergebnis, lesbare Erklärung).
    """
    links = wert_aus_pfad(kontext, bedingung.get("links", ""))
    rechts = bedingung.get("rechts")
    op = bedingung.get("op", "==")
    l_num, r_num = _als_zahl(links), _als_zahl(rechts)
    if l_num is not None and r_num is not None:
        a, b = l_num, r_num
    else:
        a, b = _als_text(links), _als_text(rechts)
    ergebnis = {
        "<": a < b, "<=": a <= b, ">": a > b, ">=": a >= b,
        "==": a == b, "!=": a != b,
    }[op]
    erklaerung = f"{bedingung.get('links')} = {links!r} {op} {rechts!r} → " \
                 f"{'ja' if ergebnis else 'nein'}"
    return ergebnis, erklaerung


def _als_zahl(w: Any) -> float | None:
    if isinstance(w, bool):
        return None
    if isinstance(w, (int, float)):
        return float(w)
    try:
        return float(str(w).replace(",", "."))
    except (TypeError, ValueError):
        return None


def _als_text(w: Any) -> str:
    return "" if w is None else str(w)


PLATZHALTER = re.compile(r"\{\{\s*([\w.]+)\s*\}\}")


def prompt_fuellen(kontext: dict, vorlage: str) -> str:
    """Ersetzt {{ mde.mde_prozent }} durch den Wert aus dem Kontext."""
    def ersetze(m: re.Match) -> str:
        wert = wert_aus_pfad(kontext, m.group(1))
        return "(unbekannt)" if wert is None else str(wert)
    return PLATZHALTER.sub(ersetze, vorlage or "")
