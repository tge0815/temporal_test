"""Datenbank-Zugriff. Bewusst schlank gehalten: nur das, was die Demo braucht."""
import asyncio
import json
import logging
from datetime import date, datetime, timezone
from typing import Any, Optional

import asyncpg

from . import config

log = logging.getLogger(__name__)

_pool: Optional[asyncpg.Pool] = None


async def pool() -> asyncpg.Pool:
    """Verbindungspool (wird beim ersten Aufruf mit Retry aufgebaut)."""
    global _pool
    if _pool is None:
        _pool = await _verbinde_mit_retry()
    return _pool


async def _verbinde_mit_retry(versuche: int = 60) -> asyncpg.Pool:
    letzter_fehler: Exception | None = None
    for i in range(versuche):
        try:
            # Klein halten: sechs Dienste teilen sich die Datenbank mit
            # Temporal, das eigene Pools mitbringt. 6 x 4 = 24 Verbindungen
            # lassen genug Luft unter PostgreSQLs max_connections (100).
            p = await asyncpg.create_pool(config.DB_DSN, min_size=1, max_size=4)
            await _schema_anlegen(p)
            log.info("PostgreSQL verbunden")
            return p
        except Exception as e:  # noqa: BLE001
            letzter_fehler = e
            log.warning("Warte auf PostgreSQL (%s/%s): %s", i + 1, versuche, e)
            await asyncio.sleep(2)
    raise RuntimeError(f"PostgreSQL nicht erreichbar: {letzter_fehler}")


SCHEMA = """
CREATE TABLE IF NOT EXISTS faelle (
    id                UUID PRIMARY KEY,
    fall_nummer       TEXT UNIQUE NOT NULL,
    versicherter      TEXT NOT NULL,
    geburtsjahr       INT,
    beruf             TEXT,
    unfall_datum      DATE,
    koerperteil       TEXT,
    schwere           TEXT,
    hergang           TEXT,
    status            TEXT NOT NULL,
    mde_prozent       NUMERIC,
    mde_konfidenz     NUMERIC,
    mde_begruendung   TEXT,
    jav_euro          NUMERIC,
    jav_konfidenz     NUMERIC,
    jav_begruendung   TEXT,
    rente_jahr        NUMERIC,
    rente_monat       NUMERIC,
    rente_formel      TEXT,
    workflow_id       TEXT,
    workflow_run_id   TEXT,
    kafka_topic       TEXT,
    kafka_partition   INT,
    kafka_offset      BIGINT,
    erstellt_am       TIMESTAMPTZ NOT NULL DEFAULT now(),
    geaendert_am      TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS fall_verlauf (
    id          BIGSERIAL PRIMARY KEY,
    fall_id     UUID NOT NULL REFERENCES faelle(id) ON DELETE CASCADE,
    schritt     TEXT NOT NULL,
    komponente  TEXT NOT NULL,
    status      TEXT NOT NULL,
    nachricht   TEXT,
    details     JSONB,
    zeitpunkt   TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS dokumente (
    id             UUID PRIMARY KEY,
    fall_id        UUID NOT NULL REFERENCES faelle(id) ON DELETE CASCADE,
    art            TEXT NOT NULL,
    dateiname      TEXT NOT NULL,
    medientyp      TEXT,
    groesse        INT,
    inhalt         BYTEA NOT NULL,
    hochgeladen_am TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_verlauf_fall ON fall_verlauf (fall_id, id);
CREATE INDEX IF NOT EXISTS idx_dokumente_fall ON dokumente (fall_id, id);

-- Nachtraeglich ergaenzte Spalten (aeltere Datenbanken aktualisieren sich so
-- beim naechsten Start von selbst).
ALTER TABLE faelle ADD COLUMN IF NOT EXISTS mde_quelle    TEXT;
ALTER TABLE faelle ADD COLUMN IF NOT EXISTS jav_gemeldet  NUMERIC;
ALTER TABLE faelle ADD COLUMN IF NOT EXISTS jav_quelle    TEXT;
"""


# Beliebige, aber feste Zahl - nur dieses Projekt benutzt sie.
SCHEMA_SPERRE = 728301


async def _schema_anlegen(p: asyncpg.Pool) -> None:
    """Legt Tabellen und Spalten an - aber immer nur ein Dienst gleichzeitig.

    Alle sechs Dienste starten zusammen und wuerden das DDL sonst parallel
    ausfuehren. ALTER TABLE nimmt eine exklusive Sperre auf die Tabelle, und
    sobald mehrere davon in der Warteschlange stehen, blockieren sie auch
    lesende Anfragen - das Dashboard haengt dann sekundenlang. Die
    Advisory-Sperre serialisiert den Start sauber.
    """
    async with p.acquire() as con:
        await con.execute("SELECT pg_advisory_lock($1)", SCHEMA_SPERRE)
        try:
            await con.execute(SCHEMA)
        finally:
            await con.execute("SELECT pg_advisory_unlock($1)", SCHEMA_SPERRE)


# --- Schreiboperationen ----------------------------------------------------

def _als_datum(wert: Any):
    """'2026-08-25' -> date(2026, 8, 25). asyncpg erwartet ein echtes date."""
    if wert is None or isinstance(wert, date):
        return wert
    return date.fromisoformat(str(wert)[:10])


async def fall_anlegen(daten: dict[str, Any]) -> None:
    p = await pool()
    async with p.acquire() as con:
        await con.execute(
            """
            INSERT INTO faelle (id, fall_nummer, versicherter, geburtsjahr, beruf,
                                unfall_datum, koerperteil, schwere, hergang, status,
                                kafka_topic, kafka_partition, kafka_offset)
            VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13)
            ON CONFLICT (id) DO NOTHING
            """,
            daten["id"], daten["fall_nummer"], daten["versicherter"],
            daten.get("geburtsjahr"), daten.get("beruf"),
            _als_datum(daten.get("unfall_datum")),
            daten.get("koerperteil"), daten.get("schwere"), daten.get("hergang"),
            daten.get("status", "EINGEGANGEN"),
            daten.get("kafka_topic"), daten.get("kafka_partition"),
            daten.get("kafka_offset"),
        )


async def verlauf_schreiben(fall_id: str, schritt: str, komponente: str,
                            status: str, nachricht: str,
                            details: dict | None = None) -> None:
    p = await pool()
    async with p.acquire() as con:
        await con.execute(
            """INSERT INTO fall_verlauf (fall_id, schritt, komponente, status,
                                         nachricht, details)
               VALUES ($1,$2,$3,$4,$5,$6::jsonb)""",
            fall_id, schritt, komponente, status, nachricht,
            json.dumps(details or {}, ensure_ascii=False, default=str),
        )


async def fall_aktualisieren(fall_id: str, **felder: Any) -> None:
    """Setzt beliebige Spalten des Falls (nur bekannte Spaltennamen)."""
    erlaubt = {
        "status", "mde_prozent", "mde_konfidenz", "mde_begruendung",
        "jav_euro", "jav_konfidenz", "jav_begruendung",
        "mde_quelle", "jav_gemeldet", "jav_quelle",
        "rente_jahr", "rente_monat", "rente_formel",
        "workflow_id", "workflow_run_id",
    }
    felder = {k: v for k, v in felder.items() if k in erlaubt}
    if not felder:
        return
    zuweisungen = ", ".join(f"{k} = ${i+2}" for i, k in enumerate(felder))
    p = await pool()
    async with p.acquire() as con:
        await con.execute(
            f"UPDATE faelle SET {zuweisungen}, geaendert_am = now() WHERE id = $1",
            fall_id, *felder.values(),
        )


async def dokument_speichern(dok_id: str, fall_id: str, art: str, dateiname: str,
                             medientyp: str | None, inhalt: bytes) -> None:
    p = await pool()
    async with p.acquire() as con:
        await con.execute(
            """INSERT INTO dokumente (id, fall_id, art, dateiname, medientyp,
                                      groesse, inhalt)
               VALUES ($1,$2,$3,$4,$5,$6,$7)""",
            dok_id, fall_id, art, dateiname, medientyp, len(inhalt), inhalt,
        )


async def dokument_lesen(dok_id: str) -> dict | None:
    p = await pool()
    async with p.acquire() as con:
        z = await con.fetchrow("SELECT * FROM dokumente WHERE id = $1", dok_id)
    if not z:
        return None
    d = dict(z)
    inhalt = d.pop("inhalt")
    d = _zeile_zu_dict(d)
    d["inhalt"] = bytes(inhalt)
    return d


async def dokumente_lesen(fall_id: str) -> list[dict]:
    """Ohne den Dateiinhalt - nur die Metadaten fuer die Anzeige."""
    p = await pool()
    async with p.acquire() as con:
        zeilen = await con.fetch(
            """SELECT id, fall_id, art, dateiname, medientyp, groesse,
                      hochgeladen_am
               FROM dokumente WHERE fall_id = $1 ORDER BY hochgeladen_am""",
            fall_id,
        )
    return [_zeile_zu_dict(z) for z in zeilen]


# --- Leseoperationen -------------------------------------------------------

async def faelle_lesen(limit: int = 100) -> list[dict]:
    p = await pool()
    async with p.acquire() as con:
        zeilen = await con.fetch(
            "SELECT * FROM faelle ORDER BY erstellt_am DESC LIMIT $1", limit
        )
    return [_zeile_zu_dict(z) for z in zeilen]


async def fall_lesen(fall_id: str) -> dict | None:
    p = await pool()
    async with p.acquire() as con:
        z = await con.fetchrow("SELECT * FROM faelle WHERE id = $1", fall_id)
    return _zeile_zu_dict(z) if z else None


async def verlauf_lesen(fall_id: str) -> list[dict]:
    p = await pool()
    async with p.acquire() as con:
        zeilen = await con.fetch(
            "SELECT * FROM fall_verlauf WHERE fall_id = $1 ORDER BY id", fall_id
        )
    return [_zeile_zu_dict(z) for z in zeilen]


def _zeile_zu_dict(zeile) -> dict:
    d = dict(zeile)  # akzeptiert asyncpg-Record und dict
    for k, v in list(d.items()):
        if isinstance(v, datetime):
            d[k] = v.astimezone(timezone.utc).isoformat()
        elif hasattr(v, "isoformat"):
            d[k] = v.isoformat()
        elif isinstance(v, str) and k == "details":
            try:
                d[k] = json.loads(v)
            except Exception:  # noqa: BLE001
                pass
        elif v is not None and type(v).__name__ == "Decimal":
            d[k] = float(v)
        elif isinstance(v, __import__("uuid").UUID):
            d[k] = str(v)
    return d
