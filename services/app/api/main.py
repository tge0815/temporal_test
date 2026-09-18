"""dashboard-api: Weboberflaeche + REST-Schnittstelle + Kafka-Producer.

Der Klick auf "Unfall melden" erzeugt hier ein echtes Kafka-Event.
Dieser Service ruft NICHT direkt den Orchestrator auf - das macht der
event-consumer, nachdem er das Event von Kafka gelesen hat.
"""
import asyncio
import json
import logging
import random
import uuid
from contextlib import asynccontextmanager
from datetime import date, datetime, timezone
from pathlib import Path

from aiokafka import AIOKafkaProducer
from aiokafka.errors import KafkaError
from fastapi import FastAPI, File, HTTPException, UploadFile
from fastapi.responses import FileResponse, JSONResponse, Response
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel, Field

from common import config, db, gutachten, prozess
from common.temporal_util import verbinde
from agenten.jav_agent import simulierte_meldung

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("dashboard-api")

STATIC = Path(__file__).resolve().parent.parent / "static"

zustand: dict = {"producer": None, "temporal": None, "db": False}


async def kafka_producer() -> AIOKafkaProducer:
    for i in range(90):
        producer = AIOKafkaProducer(
            bootstrap_servers=config.KAFKA_BOOTSTRAP,
            value_serializer=lambda v: json.dumps(v, ensure_ascii=False,
                                                  default=str).encode("utf-8"),
        )
        try:
            await producer.start()
            log.info("Kafka-Producer verbunden (%s)", config.KAFKA_BOOTSTRAP)
            return producer
        except (KafkaError, OSError) as e:
            log.warning("Warte auf Kafka (%s/90): %s", i + 1, e)
            await producer.stop()
            await asyncio.sleep(2)
    raise RuntimeError("Kafka nicht erreichbar")


async def _verbindungen_aufbauen() -> None:
    """Baut die Verbindungen im Hintergrund auf.

    Bewusst NICHT im Startup-Hook blockierend: uvicorn nimmt sonst so lange
    keine HTTP-Anfragen an, und das Dashboard waere waehrend des Hochfahrens
    von Kafka und Temporal im Browser gar nicht erreichbar. So laedt die
    Seite sofort und zeigt oben rechts an, worauf noch gewartet wird.
    """
    try:
        await db.pool()
        await db.prozess_sicherstellen(prozess.PROZESS_NAME, prozess.standard_graph())
        zustand["db"] = True
    except Exception:  # noqa: BLE001
        log.exception("PostgreSQL nicht erreichbar")
    try:
        zustand["producer"] = await kafka_producer()
    except Exception:  # noqa: BLE001
        log.exception("Kafka nicht erreichbar - Unfallmeldung nicht moeglich")
    try:
        zustand["temporal"] = await verbinde(versuche=90)
    except Exception:  # noqa: BLE001
        log.warning("Temporal nicht erreichbar - Workflow-Abfrage inaktiv")


@asynccontextmanager
async def lifespan(app: FastAPI):
    aufgabe = asyncio.create_task(_verbindungen_aufbauen())
    yield
    aufgabe.cancel()
    if zustand["producer"]:
        await zustand["producer"].stop()


app = FastAPI(title="Unfallversicherung - ereignisgetriebene Sachbearbeitung",
              lifespan=lifespan)


# --- Eingabemodell ---------------------------------------------------------

class UnfallMeldung(BaseModel):
    versicherter: str = Field(min_length=2, max_length=120)
    geburtsjahr: int = Field(ge=1930, le=2015)
    beruf: str
    unfall_datum: date
    koerperteil: str
    schwere: str
    hergang: str = Field(default="", max_length=2000)
    # Optional: mit einer bestimmten Prozessversion starten (Designer).
    # Ohne Angabe gilt die im Designer aktivierte Version.
    prozess_version: int | None = Field(default=None, ge=1)


# --- API -------------------------------------------------------------------

@app.get("/api/stammdaten")
async def stammdaten() -> dict:
    """Auswahllisten fuer das Formular + Links auf die Infrastruktur."""
    return {
        "berufe": ["Bauarbeiter", "Dachdecker", "Elektriker", "Krankenpfleger",
                   "LKW-Fahrer", "Bürokaufmann", "Industriemechaniker",
                   "Landwirt", "Sonstiges"],
        "koerperteile": ["Hand", "Arm", "Bein", "Fuß", "Auge", "Wirbelsäule",
                         "Kopf", "Schulter", "Knie", "Sonstiges"],
        "schweregrade": ["leicht", "mittel", "schwer", "sehr schwer"],
        "kafka_topic": config.KAFKA_TOPIC_UNFALL,
        "claude_aktiv": bool(config.ANTHROPIC_API_KEY),
        "claude_modell": config.CLAUDE_MODELL if config.ANTHROPIC_API_KEY else None,
        # Nur gesetzt, wenn ausdruecklich per TEMPORAL_UI_URL vorgegeben.
        # Sonst baut die Oberflaeche den Link aus Hostname + Port zusammen.
        "temporal_ui": config.TEMPORAL_UI_URL or None,
        "temporal_ui_port": config.TEMPORAL_UI_PORT,
    }


@app.post("/api/unfaelle", status_code=202)
async def unfall_melden(meldung: UnfallMeldung) -> dict:
    """Nimmt die Meldung entgegen und legt sie als Event auf Kafka."""
    producer: AIOKafkaProducer | None = zustand["producer"]
    if producer is None:
        raise HTTPException(503, "Kafka-Producer nicht bereit")

    fall_id = str(uuid.uuid4())
    fall_nummer = (f"UV-{datetime.now().year}-"
                   f"{random.randint(100000, 999999)}")
    if meldung.prozess_version is not None:
        if not await db.prozess_lesen(prozess.PROZESS_NAME, meldung.prozess_version):
            raise HTTPException(404, f"Prozessversion {meldung.prozess_version} "
                                     "gibt es nicht")
    event = {
        "id": fall_id,
        "fall_nummer": fall_nummer,
        "ereignis_typ": "UNFALL_GEMELDET",
        "gemeldet_am": datetime.now(timezone.utc).isoformat(),
        **meldung.model_dump(mode="json", exclude_none=True),
        "prozess_name": prozess.PROZESS_NAME,
    }

    metadaten = await producer.send_and_wait(
        config.KAFKA_TOPIC_UNFALL, value=event, key=fall_nummer.encode("utf-8")
    )
    log.info("Event auf Kafka gelegt: %s (Partition %s, Offset %s)",
             fall_nummer, metadaten.partition, metadaten.offset)

    return {
        "fall_id": fall_id,
        "fall_nummer": fall_nummer,
        "kafka": {
            "topic": metadaten.topic,
            "partition": metadaten.partition,
            "offset": metadaten.offset,
        },
        "hinweis": "Event wurde auf Kafka veröffentlicht. Der event-consumer "
                   "übernimmt und startet den Temporal-Workflow.",
    }


def _db_bereit() -> None:
    if not zustand["db"]:
        raise HTTPException(503, "Datenbank noch nicht bereit")


@app.get("/api/faelle")
async def faelle() -> list[dict]:
    _db_bereit()
    return await db.faelle_lesen()


@app.get("/api/faelle/{fall_id}")
async def fall(fall_id: str) -> dict:
    _db_bereit()
    daten = await db.fall_lesen(fall_id)
    if not daten:
        raise HTTPException(404, "Fall nicht gefunden")
    daten["verlauf"] = await db.verlauf_lesen(fall_id)
    daten["dokumente"] = await db.dokumente_lesen(fall_id)
    return daten


@app.get("/api/faelle/{fall_id}/workflow")
async def workflow_zustand(fall_id: str) -> JSONResponse:
    """Fragt den echten Temporal-Workflow nach seinem Zustand (Query)."""
    _db_bereit()
    daten = await db.fall_lesen(fall_id)
    if not daten:
        raise HTTPException(404, "Fall nicht gefunden")
    if not daten.get("workflow_id") or zustand["temporal"] is None:
        return JSONResponse({"verfuegbar": False,
                             "grund": "Workflow noch nicht gestartet"})
    try:
        handle = zustand["temporal"].get_workflow_handle(daten["workflow_id"])
        beschreibung = await handle.describe()
        antwort = {
            "verfuegbar": True,
            "workflow_id": daten["workflow_id"],
            "run_id": beschreibung.run_id,
            "workflow_typ": beschreibung.workflow_type,
            "task_queue": beschreibung.task_queue,
            "status": beschreibung.status.name if beschreibung.status else None,
            "gestartet_am": beschreibung.start_time.isoformat()
            if beschreibung.start_time else None,
        }
        try:
            antwort["zustand"] = await handle.query("zustand")
        except Exception as e:  # noqa: BLE001
            antwort["zustand_fehler"] = str(e)
        return JSONResponse(antwort)
    except Exception as e:  # noqa: BLE001
        return JSONResponse({"verfuegbar": False, "grund": str(e)})


@app.get("/api/faelle/{fall_id}/prozess")
async def fall_prozess(fall_id: str) -> JSONResponse:
    """Der Graph, mit dem dieser Fall läuft – plus Live-Zustand je Knoten."""
    _db_bereit()
    daten = await db.fall_lesen(fall_id)
    if not daten:
        raise HTTPException(404, "Fall nicht gefunden")
    if not daten.get("prozess_name"):
        return JSONResponse({"verfuegbar": False,
                             "grund": "Fall läuft noch mit dem fest programmierten "
                                      "Workflow (vor dem Designer angelegt)"})
    definition = await db.prozess_lesen(daten["prozess_name"], daten["prozess_version"])
    if not definition:
        return JSONResponse({"verfuegbar": False, "grund": "Prozessversion nicht gefunden"})
    antwort = {
        "verfuegbar": True,
        "name": definition["name"], "version": definition["version"],
        "graph": definition["graph"],
        "knoten_status": {}, "entscheidungen": {}, "pfad": [],
    }
    if daten.get("workflow_id") and zustand["temporal"] is not None:
        try:
            handle = zustand["temporal"].get_workflow_handle(daten["workflow_id"])
            z = await handle.query("zustand")
            antwort["knoten_status"] = z.get("knoten_status", {})
            antwort["entscheidungen"] = z.get("entscheidungen", {})
            antwort["pfad"] = z.get("pfad", [])
            antwort["aktueller_knoten"] = z.get("aktueller_knoten")
        except Exception as e:  # noqa: BLE001
            antwort["zustand_fehler"] = str(e)
    return JSONResponse(antwort)


# --- Prozessdesigner -------------------------------------------------------

class ProzessVersion(BaseModel):
    graph: dict
    kommentar: str = Field(default="", max_length=500)
    aktivieren: bool = False


class GraphPruefung(BaseModel):
    graph: dict


@app.get("/api/katalog")
async def katalog() -> dict:
    """Bausteine für die Palette im Designer."""
    return prozess.katalog()


@app.get("/api/prozesse")
async def prozesse() -> list[dict]:
    """Alle Prozessversionen (Metadaten, ohne Graph)."""
    _db_bereit()
    return await db.prozesse_lesen()


@app.get("/api/prozesse/{name}/aktiv")
async def prozess_aktiv(name: str) -> dict:
    _db_bereit()
    d = await db.prozess_lesen(name)
    if not d:
        raise HTTPException(404, "Keine aktive Version")
    return d


@app.get("/api/prozesse/{name}/versionen/{version}")
async def prozess_version(name: str, version: int) -> dict:
    _db_bereit()
    d = await db.prozess_lesen(name, version)
    if not d:
        raise HTTPException(404, "Version nicht gefunden")
    return d


@app.post("/api/prozesse/pruefen")
async def prozess_pruefen(eingabe: GraphPruefung) -> dict:
    return prozess.pruefen(eingabe.graph)


@app.post("/api/prozesse/{name}/versionen", status_code=201)
async def prozess_speichern(name: str, eingabe: ProzessVersion) -> dict:
    """Legt IMMER eine neue Version an. Alte Versionen werden nie verändert –
    laufende Fälle verweisen auf sie."""
    _db_bereit()
    ergebnis = prozess.pruefen(eingabe.graph)
    if ergebnis["fehler"]:
        raise HTTPException(422, {"fehler": ergebnis["fehler"],
                                  "warnungen": ergebnis["warnungen"]})
    graph = {**eingabe.graph, "name": name}
    neu = await db.prozess_version_anlegen(name, graph, eingabe.kommentar or None,
                                           eingabe.aktivieren)
    neu["warnungen"] = ergebnis["warnungen"]
    log.info("Prozess '%s' Version %s gespeichert%s", name, neu["version"],
             " und aktiviert" if eingabe.aktivieren else "")
    return neu


@app.post("/api/prozesse/{name}/versionen/{version}/aktivieren")
async def prozess_aktivieren(name: str, version: int) -> dict:
    """Ab jetzt starten neue Fälle mit dieser Version. Laufende bleiben."""
    _db_bereit()
    if not await db.prozess_version_aktivieren(name, version):
        raise HTTPException(404, "Version nicht gefunden")
    return {"name": name, "version": version, "aktiv": True}


# --- Gutachten und Entgeltmeldung -----------------------------------------

MAX_UPLOAD = 10 * 1024 * 1024   # 10 MB reichen fuer ein Gutachten reichlich


async def _signal_senden(fall: dict, signal: str, nutzlast: dict) -> None:
    """Weckt den wartenden Temporal-Workflow."""
    if zustand["temporal"] is None:
        raise HTTPException(503, "Temporal nicht erreichbar")
    if not fall.get("workflow_id"):
        raise HTTPException(409, "Zu diesem Fall läuft noch kein Workflow")
    handle = zustand["temporal"].get_workflow_handle(fall["workflow_id"])
    await handle.signal(signal, nutzlast)
    log.info("Signal '%s' an Workflow %s gesendet", signal, fall["workflow_id"])


@app.get("/api/faelle/{fall_id}/beispiel-gutachten")
async def beispiel_gutachten(fall_id: str) -> Response:
    """Erzeugt ein Beispiel-Gutachten als PDF zum Herunterladen."""
    _db_bereit()
    fall = await db.fall_lesen(fall_id)
    if not fall:
        raise HTTPException(404, "Fall nicht gefunden")
    pdf, _ = gutachten.beispiel_gutachten_pdf(fall)
    dateiname = f"Gutachten-{fall['fall_nummer']}.pdf"
    return Response(
        content=pdf, media_type="application/pdf",
        headers={"Content-Disposition": f'attachment; filename="{dateiname}"'},
    )


@app.post("/api/faelle/{fall_id}/gutachten", status_code=202)
async def gutachten_hochladen(fall_id: str,
                              datei: UploadFile = File(...)) -> dict:
    """Nimmt das Gutachten entgegen und weckt damit den wartenden Workflow."""
    _db_bereit()
    fall = await db.fall_lesen(fall_id)
    if not fall:
        raise HTTPException(404, "Fall nicht gefunden")
    if fall["status"] != "WARTE_AUF_GUTACHTEN":
        raise HTTPException(
            409, f"Dieser Fall wartet gerade nicht auf ein Gutachten "
                 f"(Status: {fall['status']})")

    inhalt = await datei.read()
    if not inhalt:
        raise HTTPException(400, "Die Datei ist leer")
    if len(inhalt) > MAX_UPLOAD:
        raise HTTPException(413, "Die Datei ist größer als 10 MB")

    dok_id = str(uuid.uuid4())
    await db.dokument_speichern(dok_id, fall_id, "GUTACHTEN",
                               datei.filename or "gutachten",
                               datei.content_type, inhalt)
    await db.verlauf_schreiben(
        fall_id, "Gutachten", "dashboard-api", "ABGESCHLOSSEN",
        f"Gutachten '{datei.filename}' eingegangen ({len(inhalt) // 1024} KB). "
        f"Der wartende Workflow wird per Temporal-Signal geweckt.",
        {"dokument_id": dok_id, "dateiname": datei.filename,
         "medientyp": datei.content_type, "groesse_bytes": len(inhalt),
         "signal": "gutachten_eingegangen"},
    )
    await _signal_senden(fall, "gutachten_eingegangen", {
        "dokument_id": dok_id,
        "dateiname": datei.filename,
        "medientyp": datei.content_type,
    })
    return {"dokument_id": dok_id, "hinweis": "Gutachten gespeichert, "
                                              "Workflow geweckt."}


class Entgeltmeldung(BaseModel):
    jav_euro: float = Field(gt=0, le=1_000_000)
    quelle: str = Field(default="Entgeltmeldung des Unternehmers",
                        max_length=200)


@app.post("/api/faelle/{fall_id}/entgeltmeldung", status_code=202)
async def entgeltmeldung(fall_id: str, meldung: Entgeltmeldung) -> dict:
    """Erfasst die Entgeltmeldung und weckt den wartenden Workflow."""
    return await _entgeltmeldung_einspielen(
        fall_id, meldung.jav_euro, meldung.quelle)


@app.post("/api/faelle/{fall_id}/entgeltmeldung/simulieren", status_code=202)
async def entgeltmeldung_simulieren(fall_id: str) -> dict:
    """Spielt eine plausible Antwort des Unternehmers ein."""
    _db_bereit()
    fall = await db.fall_lesen(fall_id)
    if not fall:
        raise HTTPException(404, "Fall nicht gefunden")
    erzeugt = simulierte_meldung(fall)
    return await _entgeltmeldung_einspielen(
        fall_id, erzeugt["jav_euro"], erzeugt["quelle"])


async def _entgeltmeldung_einspielen(fall_id: str, jav_euro: float,
                                     quelle: str) -> dict:
    _db_bereit()
    fall = await db.fall_lesen(fall_id)
    if not fall:
        raise HTTPException(404, "Fall nicht gefunden")
    if fall["status"] != "WARTE_AUF_ENTGELTMELDUNG":
        raise HTTPException(
            409, f"Dieser Fall wartet gerade nicht auf eine Entgeltmeldung "
                 f"(Status: {fall['status']})")

    await db.verlauf_schreiben(
        fall_id, "Entgeltmeldung", "dashboard-api", "ABGESCHLOSSEN",
        f"Entgeltmeldung eingegangen: {jav_euro:.2f} EUR Jahresbrutto "
        f"({quelle}). Der wartende Workflow wird geweckt.",
        {"jav_euro": jav_euro, "quelle": quelle,
         "signal": "entgeltmeldung_eingegangen"},
    )
    await _signal_senden(fall, "entgeltmeldung_eingegangen",
                         {"jav_euro": jav_euro, "quelle": quelle})
    return {"jav_euro": jav_euro, "quelle": quelle,
            "hinweis": "Meldung erfasst, Workflow geweckt."}


@app.get("/api/faelle/{fall_id}/dokumente/{dok_id}")
async def dokument_herunterladen(fall_id: str, dok_id: str) -> Response:
    """Liefert ein hochgeladenes Dokument zurück."""
    _db_bereit()
    dok = await db.dokument_lesen(dok_id)
    if not dok or dok["fall_id"] != fall_id:
        raise HTTPException(404, "Dokument nicht gefunden")
    return Response(
        content=dok["inhalt"],
        media_type=dok.get("medientyp") or "application/octet-stream",
        headers={"Content-Disposition":
                 f'inline; filename="{dok["dateiname"]}"'},
    )


@app.get("/api/health")
async def health() -> dict:
    return {
        "status": "ok",
        "datenbank": zustand["db"],
        "kafka": zustand["producer"] is not None,
        "temporal": zustand["temporal"] is not None,
    }


# --- Statische Oberflaeche -------------------------------------------------

app.mount("/static", StaticFiles(directory=str(STATIC)), name="static")


@app.get("/")
async def index() -> FileResponse:
    return FileResponse(str(STATIC / "index.html"))


@app.get("/designer")
async def designer() -> FileResponse:
    return FileResponse(str(STATIC / "designer.html"))
