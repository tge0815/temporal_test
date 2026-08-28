"""Zentrale Konfiguration - alles per Umgebungsvariable ueberschreibbar."""
import os

# --- Kafka -----------------------------------------------------------------
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
KAFKA_TOPIC_UNFALL = os.getenv("KAFKA_TOPIC_UNFALL", "unfall.gemeldet")
KAFKA_GROUP_ID = os.getenv("KAFKA_GROUP_ID", "eingang-service")

# --- PostgreSQL ------------------------------------------------------------
DB_DSN = os.getenv(
    "DATABASE_URL", "postgresql://postgres:postgres@postgres:5432/unfall"
)

# --- Temporal --------------------------------------------------------------
TEMPORAL_HOST = os.getenv("TEMPORAL_HOST", "temporal:7233")
TEMPORAL_NAMESPACE = os.getenv("TEMPORAL_NAMESPACE", "default")
# Leer lassen (Normalfall): das Dashboard baut den Link zur Temporal-Oberflaeche
# selbst aus der Adresse, unter der Sie es aufgerufen haben - so funktioniert es
# ueber localhost genauso wie ueber die echte IP der VM.
TEMPORAL_UI_URL = os.getenv("TEMPORAL_UI_URL", "")
TEMPORAL_UI_PORT = os.getenv("TEMPORAL_UI_PORT", "28233")

# --- Task-Queues (je Agent eine eigene = je Agent ein eigener Service) -----
QUEUE_ORCHESTRATOR = "orchestrator-queue"
QUEUE_MDE = "mde-agent-queue"
QUEUE_JAV = "jav-agent-queue"
QUEUE_RENTE = "rentenberechnung-queue"

# --- Demo-Tempo ------------------------------------------------------------
# Kuenstliche Verzoegerung der Agenten, damit man im Dashboard zusehen kann.
AGENT_MIN_DAUER = float(os.getenv("AGENT_MIN_DAUER", "2.5"))
AGENT_MAX_DAUER = float(os.getenv("AGENT_MAX_DAUER", "5.0"))
