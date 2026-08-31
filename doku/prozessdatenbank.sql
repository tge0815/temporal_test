-- ===========================================================================
--  Prozessdatenbank für einen Leistungsfall der gesetzlichen Unfallversicherung
--  (PostgreSQL 14+)
--
--  Das ist NICHT das Schema der Demo, sondern wie es in einer echten
--  Sachbearbeitung aussehen würde. Die wichtigsten Unterschiede:
--
--   * Keine Kopien von Personendaten. Der Fall verweist über IDs auf das
--     Kernsystem; Name, Anschrift und Geburtsdatum werden dort gelesen.
--   * Keine Dokumentinhalte. Nur Doxis-Verweise; die Bytes bleiben im DMS.
--   * MdE und JAV sind keine Spalten, sondern zeitlich gültige Feststellungen
--     mit Herkunft und Begründung. Eine Neufeststellung ergänzt, sie
--     überschreibt nicht.
--   * Nichts wird gelöscht. Aufhebung ist ein Status, kein DELETE.
--
--  Anlegen mit:  psql -d leistungswesen -f prozessdatenbank.sql
-- ===========================================================================

-- Wird für die EXCLUDE-Bedingung auf Gültigkeitszeiträumen gebraucht.
CREATE EXTENSION IF NOT EXISTS btree_gist;


-- ---------------------------------------------------------------------------
--  Wertebereiche
--
--  Als ENUM statt als Schlüsseltabelle: die Mengen sind klein und stabil, und
--  ein Tippfehler fällt schon beim INSERT auf. Erweitern geht mit
--  ALTER TYPE ... ADD VALUE, Umbenennen mit ALTER TYPE ... RENAME VALUE.
-- ---------------------------------------------------------------------------

CREATE TYPE fallart AS ENUM (
    'ARBEITSUNFALL', 'WEGEUNFALL', 'BERUFSKRANKHEIT');

CREATE TYPE fallstatus AS ENUM (
    'ANGELEGT',            -- Eingang erfasst, noch nichts veranlasst
    'IN_ERMITTLUNG',       -- Sachverhalt wird aufgeklärt
    'WARTET_EXTERN',       -- wartet auf Gutachten, Entgeltmeldung, Auskunft
    'ENTSCHEIDUNGSREIF',   -- alle Feststellungen liegen vor
    'BESCHIEDEN',          -- Bescheid erlassen
    'RECHTSBEHELF',        -- Widerspruch oder Klage anhängig
    'ABGESCHLOSSEN',
    'STORNIERT');

CREATE TYPE feststellungsart AS ENUM (
    'MDE',                 -- Minderung der Erwerbsfähigkeit in Prozent
    'JAV',                 -- Jahresarbeitsverdienst in Euro
    'ARBEITSUNFAEHIGKEIT', -- Tage
    'PFLEGEBEDARF',        -- Stufe
    'VERSICHERUNGSFALL');  -- dem Grunde nach anerkannt: ja/nein

CREATE TYPE feststellungsstatus AS ENUM (
    'VORLAEUFIG',          -- Vorschlag, z. B. von einem Agenten
    'FESTGESTELLT',        -- verbindlich, Grundlage für Leistungen
    'AUFGEHOBEN');         -- durch eine spätere Feststellung ersetzt

CREATE TYPE herkunft AS ENUM (
    'GUTACHTEN', 'ENTGELTMELDUNG', 'UNFALLANZEIGE', 'AKTENLAGE',
    'AGENT_VORSCHLAG', 'MANUELL', 'SCHAETZUNG');

CREATE TYPE leistungsart AS ENUM (
    'VERLETZTENRENTE', 'VERLETZTENGELD', 'PFLEGEGELD',
    'HEILBEHANDLUNG', 'ABFINDUNG', 'STERBEGELD');

CREATE TYPE leistungsstatus AS ENUM (
    'BERECHNET', 'BESCHIEDEN', 'LAUFEND', 'RUHEND', 'BEENDET', 'AUFGEHOBEN');

CREATE TYPE beteiligtenrolle AS ENUM (
    'VERSICHERTER', 'UNTERNEHMER', 'GUTACHTER', 'BEHANDELNDER_ARZT',
    'KRANKENKASSE', 'BEVOLLMAECHTIGTER', 'HINTERBLIEBENER');

CREATE TYPE fristart AS ENUM (
    'WIEDERVORLAGE',       -- selbst gesetzt
    'GESETZLICHE_FRIST',   -- z. B. Bescheidungsfrist § 88 SGG
    'WIDERSPRUCHSFRIST',   -- ein Monat ab Bekanntgabe
    'NACHUNTERSUCHUNG');


-- ---------------------------------------------------------------------------
--  1. fall – der Vorgang
--
--  Enthält bewusst KEINE Personendaten. versicherten_id und unternehmen_id
--  sind Fremdschlüssel in das Kernsystem; Name und Anschrift werden bei
--  Bedarf dort gelesen. Das hält die Prozessdatenbank frei von Sozialdaten
--  und sorgt dafür, dass immer der aktuelle Stand verwendet wird.
-- ---------------------------------------------------------------------------

CREATE TABLE fall (
    id                 BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,

    -- Fachlicher Schlüssel. Auch der Workflow läuft unter diesem Zeichen,
    -- dadurch ist die Verarbeitung idempotent: ein doppelt eingelieferter
    -- Eingang startet keinen zweiten Workflow.
    aktenzeichen       TEXT        NOT NULL UNIQUE,

    -- Verweise ins Kernsystem, keine Kopien.
    versicherten_id    TEXT        NOT NULL,
    unternehmen_id     TEXT,

    -- Verweis auf die elektronische Akte im DMS.
    doxis_akte_id      TEXT,

    fallart            fallart     NOT NULL,
    status             fallstatus  NOT NULL DEFAULT 'ANGELEGT',

    ereignis_datum     DATE        NOT NULL,   -- Tag des Unfalls
    eingang_datum      DATE        NOT NULL DEFAULT CURRENT_DATE,
    abschluss_datum    DATE,

    zustaendige_stelle TEXT,                   -- Bezirksverwaltung o. Ä.
    bearbeiter_id      TEXT,                   -- Sachbearbeitung

    -- Orchestrierung: welcher Temporal-Workflow bearbeitet diesen Fall.
    workflow_id        TEXT,
    workflow_run_id    TEXT,

    erstellt_am        TIMESTAMPTZ NOT NULL DEFAULT now(),
    erstellt_von       TEXT        NOT NULL DEFAULT current_user,
    geaendert_am       TIMESTAMPTZ NOT NULL DEFAULT now(),
    geaendert_von      TEXT        NOT NULL DEFAULT current_user,

    CONSTRAINT fall_ereignis_nicht_zukunft
        CHECK (ereignis_datum <= CURRENT_DATE),
    CONSTRAINT fall_abschluss_nach_eingang
        CHECK (abschluss_datum IS NULL OR abschluss_datum >= eingang_datum)
);

CREATE INDEX fall_status_idx      ON fall (status)
    WHERE status <> 'ABGESCHLOSSEN';
CREATE INDEX fall_versicherter_idx ON fall (versicherten_id);
CREATE INDEX fall_workflow_idx     ON fall (workflow_id);

COMMENT ON TABLE  fall IS
    'Vorgang der Leistungssachbearbeitung. Personendaten liegen im Kernsystem.';
COMMENT ON COLUMN fall.aktenzeichen IS
    'Fachlicher Schlüssel, zugleich Workflow-ID – sorgt für Idempotenz.';


-- ---------------------------------------------------------------------------
--  2. beteiligter – wer hat am Fall mitzureden
--
--  Auch hier nur Verweise. name_anzeige ist ein bewusst kurzer Cache für
--  Listenansichten, damit nicht jede Trefferliste das Kernsystem befragen
--  muss. Er ist als Cache gekennzeichnet und nie entscheidungsrelevant.
-- ---------------------------------------------------------------------------

CREATE TABLE beteiligter (
    id             BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    fall_id        BIGINT NOT NULL REFERENCES fall(id) ON DELETE RESTRICT,
    rolle          beteiligtenrolle NOT NULL,
    partner_id     TEXT   NOT NULL,          -- Verweis ins Kernsystem
    name_anzeige   TEXT,                     -- nur Anzeige-Cache
    cache_stand    TIMESTAMPTZ,
    gueltig_ab     DATE   NOT NULL DEFAULT CURRENT_DATE,
    gueltig_bis    DATE,
    erstellt_am    TIMESTAMPTZ NOT NULL DEFAULT now(),

    UNIQUE (fall_id, rolle, partner_id, gueltig_ab)
);

CREATE INDEX beteiligter_fall_idx ON beteiligter (fall_id);

COMMENT ON COLUMN beteiligter.name_anzeige IS
    'Nur Anzeige-Cache für Listen. Verbindlich ist immer das Kernsystem.';


-- ---------------------------------------------------------------------------
--  3. dokument – Verweise auf die Akte im DMS
--
--  Kein BYTEA. Die Bytes liegen in Doxis, das dafür Revisionssicherheit,
--  Aufbewahrungsfristen und Aktenstruktur mitbringt.
-- ---------------------------------------------------------------------------

CREATE TABLE dokument (
    id             BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    fall_id        BIGINT NOT NULL REFERENCES fall(id) ON DELETE RESTRICT,
    doxis_id       TEXT   NOT NULL UNIQUE,
    dokumentart    TEXT   NOT NULL,       -- GUTACHTEN, UNFALLANZEIGE, BESCHEID …
    titel          TEXT,
    eingang_am     DATE   NOT NULL DEFAULT CURRENT_DATE,
    erstellt_von   TEXT,                  -- absendende Stelle
    seiten         INT,
    ausgehend      BOOLEAN NOT NULL DEFAULT FALSE,   -- von uns erzeugt?
    erfasst_am     TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX dokument_fall_idx ON dokument (fall_id, eingang_am);

COMMENT ON TABLE dokument IS
    'Verweise auf Dokumente im DMS. Inhalte werden hier nicht gespeichert.';


-- ---------------------------------------------------------------------------
--  4. feststellung – das Herzstück
--
--  MdE und JAV sind keine Spalten am Fall, weil sie sich ändern: eine
--  Verschlimmerung führt zur Neufeststellung nach § 48 SGB X, eine
--  Nachuntersuchung zu einer neuen MdE ab einem Stichtag. Der alte Wert
--  bleibt gültig für den Zeitraum davor – Renten der Vergangenheit werden
--  danach berechnet.
--
--  Deshalb: jede Feststellung ist eine Zeile mit Gültigkeitszeitraum,
--  Herkunft, Begründung und Beleg. Nie ein UPDATE des alten Wertes.
-- ---------------------------------------------------------------------------

CREATE TABLE feststellung (
    id                 BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    fall_id            BIGINT NOT NULL REFERENCES fall(id) ON DELETE RESTRICT,
    art                feststellungsart NOT NULL,

    wert               NUMERIC(12,2) NOT NULL,
    einheit            TEXT   NOT NULL,        -- 'PROZENT', 'EUR', 'TAGE'

    -- Zeitraum, für den dieser Wert gilt. Obere Grenze offen = bis auf
    -- Weiteres. daterange ist halboffen: [ab, bis)
    gueltigkeit        DATERANGE NOT NULL,

    status             feststellungsstatus NOT NULL DEFAULT 'VORLAEUFIG',
    herkunft           herkunft NOT NULL,

    -- Nachvollziehbarkeit: § 35 SGB X verlangt eine Begründung. Bei
    -- maschinellen Vorschlägen kommt der Konfidenzwert dazu.
    begruendung        TEXT   NOT NULL,
    konfidenz          NUMERIC(3,2),
    beleg_dokument_id  BIGINT REFERENCES dokument(id),

    -- Wer hat festgestellt: Kennung eines Menschen oder eines Dienstes.
    festgestellt_von   TEXT   NOT NULL,
    festgestellt_am    TIMESTAMPTZ NOT NULL DEFAULT now(),

    -- Bei Neufeststellung: worauf sie sich bezieht.
    ersetzt_id         BIGINT REFERENCES feststellung(id),

    CONSTRAINT feststellung_konfidenz_bereich
        CHECK (konfidenz IS NULL OR konfidenz BETWEEN 0 AND 1),
    CONSTRAINT feststellung_mde_bereich
        CHECK (art <> 'MDE' OR (wert >= 0 AND wert <= 100)),
    CONSTRAINT feststellung_jav_positiv
        CHECK (art <> 'JAV' OR wert > 0),
    -- Maschinelle Vorschläge dürfen nicht ohne Prüfung verbindlich werden.
    CONSTRAINT feststellung_agent_nur_vorlaeufig
        CHECK (herkunft <> 'AGENT_VORSCHLAG' OR status = 'VORLAEUFIG'),

    -- Zwei verbindliche MdE-Feststellungen dürfen sich zeitlich nicht
    -- überschneiden. Die Datenbank verhindert das, nicht der Anwendungscode.
    -- Vorläufige Vorschläge dürfen sich überlappen – mehrere Agenten dürfen
    -- unterschiedlicher Meinung sein.
    EXCLUDE USING gist (
        fall_id WITH =, art WITH =, gueltigkeit WITH &&
    ) WHERE (status = 'FESTGESTELLT')
);

CREATE INDEX feststellung_fall_idx ON feststellung (fall_id, art, status);
CREATE INDEX feststellung_zeit_idx ON feststellung USING gist (gueltigkeit);

COMMENT ON TABLE feststellung IS
    'Zeitlich gültige Feststellungen (MdE, JAV …) mit Herkunft und Begründung.';
COMMENT ON COLUMN feststellung.gueltigkeit IS
    'Halboffener Zeitraum [ab, bis). Obere Grenze NULL = bis auf Weiteres.';


-- ---------------------------------------------------------------------------
--  5. leistung – was bewilligt wurde
--
--  berechnung hält die Eingangsgrößen fest, aus denen der Betrag entstanden
--  ist. Damit lässt sich Jahre später nachvollziehen, warum genau dieser
--  Betrag ausgezahlt wurde – auch wenn sich die Rechenregel geändert hat.
-- ---------------------------------------------------------------------------

CREATE TABLE leistung (
    id                  BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    fall_id             BIGINT NOT NULL REFERENCES fall(id) ON DELETE RESTRICT,
    art                 leistungsart NOT NULL,
    status              leistungsstatus NOT NULL DEFAULT 'BERECHNET',

    gueltigkeit         DATERANGE NOT NULL,
    betrag_monatlich    NUMERIC(12,2),
    betrag_jaehrlich    NUMERIC(12,2),
    waehrung            CHAR(3) NOT NULL DEFAULT 'EUR',

    -- Welche Feststellungen sind eingeflossen und mit welcher Formel.
    berechnung          JSONB  NOT NULL,
    mde_feststellung_id BIGINT REFERENCES feststellung(id),
    jav_feststellung_id BIGINT REFERENCES feststellung(id),

    bescheid_dokument_id BIGINT REFERENCES dokument(id),
    bescheid_am         DATE,
    bekanntgabe_am      DATE,               -- Fristbeginn für den Widerspruch

    erstellt_am         TIMESTAMPTZ NOT NULL DEFAULT now(),
    erstellt_von        TEXT NOT NULL DEFAULT current_user,

    CONSTRAINT leistung_betrag_nicht_negativ
        CHECK (COALESCE(betrag_monatlich, 0) >= 0
           AND COALESCE(betrag_jaehrlich, 0) >= 0),
    -- Ein Bescheid braucht ein Dokument – sonst ist er nicht nachweisbar.
    CONSTRAINT leistung_bescheid_belegt
        CHECK (status <> 'BESCHIEDEN' OR bescheid_dokument_id IS NOT NULL),

    -- Dieselbe Leistungsart darf für denselben Zeitraum nur einmal laufen.
    EXCLUDE USING gist (
        fall_id WITH =, art WITH =, gueltigkeit WITH &&
    ) WHERE (status IN ('BESCHIEDEN', 'LAUFEND'))
);

CREATE INDEX leistung_fall_idx ON leistung (fall_id, art);

COMMENT ON COLUMN leistung.berechnung IS
    'Eingangsgrößen und Formel der Berechnung, für die spätere Nachprüfung.';


-- ---------------------------------------------------------------------------
--  6. zahlung – was tatsächlich angewiesen wurde
--
--  Getrennt von der Leistung: eine laufende Rente erzeugt monatlich eine
--  Zahlung, und Rückforderungen sind eigene Vorgänge.
-- ---------------------------------------------------------------------------

CREATE TABLE zahlung (
    id             BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    leistung_id    BIGINT NOT NULL REFERENCES leistung(id) ON DELETE RESTRICT,
    faellig_am     DATE   NOT NULL,
    betrag         NUMERIC(12,2) NOT NULL,
    zahlungslauf   TEXT,                       -- Kennung des Zahllaufs
    angewiesen_am  DATE,
    storniert_am   DATE,
    stornogrund    TEXT,
    erstellt_am    TIMESTAMPTZ NOT NULL DEFAULT now(),

    CONSTRAINT zahlung_storno_begruendet
        CHECK (storniert_am IS NULL OR stornogrund IS NOT NULL)
);

CREATE INDEX zahlung_leistung_idx ON zahlung (leistung_id, faellig_am);
CREATE INDEX zahlung_offen_idx    ON zahlung (faellig_am)
    WHERE angewiesen_am IS NULL AND storniert_am IS NULL;


-- ---------------------------------------------------------------------------
--  7. frist – Wiedervorlagen und gesetzliche Fristen
--
--  Der Temporal-Workflow kennt seine eigenen Timer, aber die Sachbearbeitung
--  braucht eine abfragbare Liste: "Was ist diese Woche fällig?"
-- ---------------------------------------------------------------------------

CREATE TABLE frist (
    id              BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    fall_id         BIGINT NOT NULL REFERENCES fall(id) ON DELETE RESTRICT,
    art             fristart NOT NULL,
    bezeichnung     TEXT   NOT NULL,
    faellig_am      DATE   NOT NULL,
    erledigt_am     DATE,
    erledigt_durch  TEXT,
    mahnstufe       INT    NOT NULL DEFAULT 0,
    erstellt_am     TIMESTAMPTZ NOT NULL DEFAULT now(),

    CONSTRAINT frist_mahnstufe_plausibel CHECK (mahnstufe BETWEEN 0 AND 5)
);

CREATE INDEX frist_offen_idx ON frist (faellig_am)
    WHERE erledigt_am IS NULL;


-- ---------------------------------------------------------------------------
--  8. rechtsbehelf – Widerspruch und Klage
-- ---------------------------------------------------------------------------

CREATE TABLE rechtsbehelf (
    id                 BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    fall_id            BIGINT NOT NULL REFERENCES fall(id) ON DELETE RESTRICT,
    leistung_id        BIGINT REFERENCES leistung(id),
    art                TEXT   NOT NULL,     -- WIDERSPRUCH, KLAGE, BERUFUNG
    eingang_am         DATE   NOT NULL,
    fristgerecht       BOOLEAN,
    gegenstand         TEXT   NOT NULL,
    ergebnis           TEXT,                -- ABGEHOLFEN, ZURUECKGEWIESEN …
    abgeschlossen_am   DATE,
    dokument_id        BIGINT REFERENCES dokument(id),
    erstellt_am        TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX rechtsbehelf_fall_idx ON rechtsbehelf (fall_id);


-- ---------------------------------------------------------------------------
--  9. vorgangsschritt – der Verlauf
--
--  Reines Anhängen, nie ändern. Beantwortet später die Frage, wer wann was
--  veranlasst hat – und bei maschinellen Schritten, mit welchem Ergebnis.
-- ---------------------------------------------------------------------------

CREATE TABLE vorgangsschritt (
    id           BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    fall_id      BIGINT NOT NULL REFERENCES fall(id) ON DELETE RESTRICT,
    schritt      TEXT   NOT NULL,
    komponente   TEXT   NOT NULL,     -- mde-agent, orchestrator, Sachbearbeitung
    akteur_art   TEXT   NOT NULL,     -- SYSTEM, AGENT, MENSCH
    akteur_id    TEXT,
    status       TEXT   NOT NULL,
    nachricht    TEXT   NOT NULL,
    details      JSONB  NOT NULL DEFAULT '{}'::jsonb,
    zeitpunkt    TIMESTAMPTZ NOT NULL DEFAULT now(),

    CONSTRAINT vorgangsschritt_akteur_art
        CHECK (akteur_art IN ('SYSTEM', 'AGENT', 'MENSCH'))
);

CREATE INDEX vorgangsschritt_fall_idx ON vorgangsschritt (fall_id, id);
CREATE INDEX vorgangsschritt_details_idx ON vorgangsschritt USING gin (details);


-- ---------------------------------------------------------------------------
--  Sichten für die tägliche Arbeit
-- ---------------------------------------------------------------------------

-- Die heute gültigen Feststellungen je Fall.
CREATE VIEW aktuelle_feststellung AS
SELECT DISTINCT ON (fall_id, art)
       fall_id, art, wert, einheit, gueltigkeit, herkunft,
       begruendung, konfidenz, festgestellt_von, festgestellt_am
FROM   feststellung
WHERE  status = 'FESTGESTELLT'
  AND  gueltigkeit @> CURRENT_DATE
ORDER  BY fall_id, art, lower(gueltigkeit) DESC;

-- Fälle, die auf etwas Externes warten, mit Wartedauer.
CREATE VIEW offene_wartefaelle AS
SELECT f.id, f.aktenzeichen, f.status,
       fr.art          AS fristart,
       fr.bezeichnung,
       fr.faellig_am,
       fr.mahnstufe,
       CURRENT_DATE - fr.faellig_am AS tage_ueberfaellig
FROM   fall f
JOIN   frist fr ON fr.fall_id = f.id AND fr.erledigt_am IS NULL
WHERE  f.status = 'WARTET_EXTERN'
ORDER  BY fr.faellig_am;

-- Maschinelle Vorschläge, die noch niemand geprüft hat.
CREATE VIEW ungeprüfte_vorschlaege AS
SELECT f.aktenzeichen, fs.art, fs.wert, fs.einheit, fs.konfidenz,
       fs.begruendung, fs.festgestellt_von, fs.festgestellt_am
FROM   feststellung fs
JOIN   fall f ON f.id = fs.fall_id
WHERE  fs.status = 'VORLAEUFIG'
  AND  fs.herkunft = 'AGENT_VORSCHLAG'
  AND  NOT EXISTS (
         SELECT 1 FROM feststellung neu
         WHERE  neu.ersetzt_id = fs.id)
ORDER  BY fs.konfidenz NULLS FIRST, fs.festgestellt_am;
