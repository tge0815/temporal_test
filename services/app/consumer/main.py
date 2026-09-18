"""event-consumer (Eingang).

Aufgabe:
 1. Unfall-Events vom Kafka-Topic 'unfall.gemeldet' entgegennehmen
 2. den Fall in PostgreSQL speichern
 3. den Orchestrator (Temporal) ueber den neuen Fall informieren,
    also den Workflow starten.

Dieser Service ist die Bruecke zwischen "Ereignis" und "Bearbeitung".
"""
import asyncio
import json
import logging

from aiokafka import AIOKafkaConsumer
from aiokafka.errors import KafkaError

from common import config, db, prozess
from common.temporal_util import verbinde

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("event-consumer")


async def kafka_consumer() -> AIOKafkaConsumer:
    """Verbindet zu Kafka - mit Retry, weil der Broker beim Start dauert."""
    for i in range(90):
        consumer = AIOKafkaConsumer(
            config.KAFKA_TOPIC_UNFALL,
            bootstrap_servers=config.KAFKA_BOOTSTRAP,
            group_id=config.KAFKA_GROUP_ID,
            auto_offset_reset="earliest",
            enable_auto_commit=True,
            value_deserializer=lambda b: json.loads(b.decode("utf-8")),
        )
        try:
            await consumer.start()
            log.info("Kafka verbunden, lausche auf Topic '%s'",
                     config.KAFKA_TOPIC_UNFALL)
            return consumer
        except (KafkaError, OSError) as e:
            log.warning("Warte auf Kafka (%s/90): %s", i + 1, e)
            await consumer.stop()
            await asyncio.sleep(2)
    raise RuntimeError("Kafka nicht erreichbar")


async def verarbeite(nachricht, temporal_client) -> None:
    event = nachricht.value
    fall_id = event["id"]
    log.info("Event empfangen: Fall %s (Partition %s, Offset %s)",
             event.get("fall_nummer"), nachricht.partition, nachricht.offset)

    # --- Schritt 1: Eingang speichern -------------------------------------
    await db.fall_anlegen({
        **event,
        "status": "EINGEGANGEN",
        "kafka_topic": nachricht.topic,
        "kafka_partition": nachricht.partition,
        "kafka_offset": nachricht.offset,
    })
    await db.verlauf_schreiben(
        fall_id, "Eingang", "event-consumer (Kafka)", "ABGESCHLOSSEN",
        f"Unfall-Event von Kafka angenommen und gespeichert "
        f"(Topic {nachricht.topic}, Partition {nachricht.partition}, "
        f"Offset {nachricht.offset})",
        {
            "kafka_topic": nachricht.topic,
            "kafka_partition": nachricht.partition,
            "kafka_offset": nachricht.offset,
            "kafka_key": nachricht.key.decode() if nachricht.key else None,
            "event": event,
        },
    )

    # --- Schritt 2: Prozessdefinition waehlen ------------------------------
    # Die Meldung darf eine bestimmte Version verlangen; sonst gilt die im
    # Designer aktivierte. Der Graph wird dem Workflow als Argument
    # mitgegeben und liegt damit fest in dessen Historie: spaetere
    # Aenderungen im Designer erreichen diesen Fall nicht mehr.
    name = event.get("prozess_name") or prozess.PROZESS_NAME
    version = event.get("prozess_version")
    definition = await db.prozess_lesen(name, version)
    if definition is None:
        raise RuntimeError(f"Prozess '{name}' Version {version or 'aktiv'} "
                           "nicht gefunden")
    await db.fall_aktualisieren(
        fall_id, prozess_name=definition["name"],
        prozess_version=definition["version"])

    # --- Schritt 3: Orchestrator informieren (Temporal-Workflow starten) --
    workflow_id = f"unfall-{event['fall_nummer']}"
    handle = await temporal_client.start_workflow(
        "Prozess",
        args=[event, {"name": definition["name"],
                      "version": definition["version"],
                      "graph": definition["graph"]}],
        id=workflow_id,
        task_queue=config.QUEUE_ORCHESTRATOR,
    )
    await db.fall_aktualisieren(
        fall_id, workflow_id=handle.id, workflow_run_id=handle.result_run_id,
        status="IN_BEARBEITUNG",
    )
    await db.verlauf_schreiben(
        fall_id, "Orchestrierung", "orchestrator (Temporal)", "GESTARTET",
        f"Temporal-Workflow '{workflow_id}' gestartet mit Prozess "
        f"'{definition['name']}' Version {definition['version']} "
        f"({len(definition['graph']['knoten'])} Knoten). Der Graph liegt "
        f"jetzt in der Workflow-Historie – spätere Änderungen im Designer "
        f"betreffen diesen Fall nicht.",
        {"workflow_id": handle.id, "run_id": handle.result_run_id,
         "task_queue": config.QUEUE_ORCHESTRATOR,
         "prozess_name": definition["name"],
         "prozess_version": definition["version"]},
    )
    log.info("Workflow %s gestartet (run %s, Prozess %s v%s)", handle.id,
             handle.result_run_id, definition["name"], definition["version"])


async def main() -> None:
    await db.pool()
    await db.prozess_sicherstellen(prozess.PROZESS_NAME, prozess.standard_graph())
    temporal_client = await verbinde()
    consumer = await kafka_consumer()
    try:
        async for nachricht in consumer:
            try:
                await verarbeite(nachricht, temporal_client)
            except Exception:  # noqa: BLE001
                # Ein kaputtes Event darf den Service nicht anhalten.
                log.exception("Fehler bei der Verarbeitung von Offset %s",
                              nachricht.offset)
    finally:
        await consumer.stop()


if __name__ == "__main__":
    asyncio.run(main())
