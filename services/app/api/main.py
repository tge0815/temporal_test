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
from fastapi import FastAPI, HTTPException
from fastapi.responses import FileResponse, JSONResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel, Field

from common import config, db
from common.temporal_util import verbinde

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
    event = {
        "id": fall_id,
        "fall_nummer": fall_nummer,
        "ereignis_typ": "UNFALL_GEMELDET",
        "gemeldet_am": datetime.now(timezone.utc).isoformat(),
        **meldung.model_dump(mode="json"),
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
