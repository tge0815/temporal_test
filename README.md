# Unfallversicherung – ereignisgetriebene Sachbearbeitung (MVP)

Diese Demo zeigt, wie ein Unfall in der gesetzlichen Unfallversicherung **automatisch**
durch mehrere Bearbeitungsschritte läuft – ausgelöst durch ein Ereignis („Event"),
gesteuert von einem Orchestrator, ausgeführt von mehreren eigenständigen Diensten.

Sie können den ganzen Ablauf im Browser mitverfolgen. Programmierkenntnisse sind
zum Ausprobieren **nicht** nötig.

---

## 1. Starten

Voraussetzung: **Docker Desktop** ist installiert und läuft.

Im Ordner dieses Projekts ein Terminal öffnen und eingeben:

```bash
docker compose up
```

Beim allerersten Mal dauert das ein paar Minuten (Docker lädt Kafka, Temporal,
PostgreSQL herunter). Danach startet es in wenigen Sekunden.

Warten Sie, bis in der Ausgabe Zeilen wie diese erscheinen:

```
uv-dashboard        | Uvicorn running on http://0.0.0.0:8080
uv-event-consumer   | Kafka verbunden, lausche auf Topic 'unfall.gemeldet'
uv-orchestrator     | Orchestrator-Worker läuft auf Queue 'orchestrator-queue'
uv-mde-agent        | mde-agent läuft auf Queue 'mde-agent-queue'
uv-jav-agent        | jav-agent läuft auf Queue 'jav-agent-queue'
uv-rentenberechnung | rentenberechnung läuft auf Queue 'rentenberechnung-queue'
```

## 2. Welche Seite öffne ich?

| Was | URL |
| --- | --- |
| **Dashboard – hier klicken Sie** | **http://localhost:8080** |
| Temporal-Oberfläche (der Blick „unter die Motorhaube") | http://localhost:8233 |

Alles Weitere passiert auf **http://localhost:8080**.

## 3. Was klicke ich an?

1. Links im Formular **„Unfall melden"** stehen bereits sinnvolle Beispielwerte.
   Sie können sie übernehmen oder ändern (Name, Beruf, Körperteil, Schweregrad …).
2. Auf **„Unfall melden → Event auslösen"** klicken.
3. Unter dem Knopf erscheint eine grüne Bestätigung mit der Fallnummer und der
   Stelle, an der das Ereignis in Kafka gelandet ist
   (z. B. `Topic unfall.gemeldet, Partition 0, Offset 3`).
4. Rechts taucht der Fall auf und wandert **von selbst** durch fünf Stufen.
   Die Leiste färbt sich Schritt für Schritt grün. Das dauert insgesamt ca. 10–15 Sekunden.
5. Auf die Fallkarte **klicken** → es öffnet sich die Detailansicht mit dem
   kompletten Verlauf: jeder Schritt mit Uhrzeit, Ergebnis, Konfidenz und Begründung,
   dazu der Rechenweg der Rente und der Zustand des Temporal-Workflows.

---

## 4. Was passiert in jedem Schritt?

| # | Schritt | Wer macht es? | Was passiert? |
|---|---------|---------------|---------------|
| 0 | Meldung | **dashboard-api** | Ihr Klick erzeugt ein **echtes Kafka-Event** auf dem Topic `unfall.gemeldet`. Die Weboberfläche ruft *keinen* Bearbeitungsdienst direkt auf. |
| 1 | **Eingang** | **event-consumer** | Liest das Event von Kafka, speichert den Fall in **PostgreSQL** und informiert den Orchestrator, indem er einen **Temporal-Workflow** startet. |
| 2 | **MdE-Ermittlung** | **mde-agent** | Ermittelt die *Minderung der Erwerbsfähigkeit* in Prozent, mit **Konfidenzwert** und kurzer **Begründung**. |
| 3 | **JAV-Ermittlung** | **jav-agent** | Ermittelt den *Jahresarbeitsverdienst* in Euro, ebenfalls mit Konfidenz und Begründung. |
| 4 | **Rentenberechnung** | **rentenberechnung** | **Kein Agent, keine Simulation.** Feste Rechenregel: `Rente/Jahr = 2/3 × JAV × (MdE / 100)`, `Rente/Monat = Rente/Jahr ÷ 12`. Formel und Ergebnis stehen in der Detailansicht. |
| 5 | **Ergebnis** | **orchestrator** | Der Fall wird auf „Abgeschlossen" gesetzt. Der Rentenbetrag steht fest. |

### Ehrlich gesagt: was ist echt, was ist simuliert?

* **Echt:** Kafka als Message Broker, Temporal als Orchestrator, PostgreSQL als Speicher,
  die getrennten Dienste, und die **Rentenformel**.
* **Simuliert:** Die Werte von `mde-agent` und `jav-agent`. Sie werden aus den
  Eingabedaten plausibel abgeleitet (Körperteil, Schweregrad, Beruf, Alter) und leicht
  zufällig gestreut. Jeder Agent wartet zusätzlich 2,5–5 Sekunden, damit man im
  Dashboard **zusehen** kann. In der Realität säßen hier ein KI-Agent bzw. eine
  Anbindung an Gutachten- und Entgeltdaten.

Es geht in dieser Demo um den **Ablauf**, nicht um fachliche Korrektheit.

---

## 5. Die Architektur

```
       Browser  ──  Dashboard  http://localhost:8080
          │
          │ (1) Formular abschicken
          ▼
   ┌──────────────┐  (2) Event schreiben   ┌─────────────────────┐
   │ dashboard-api│ ─────────────────────► │  Apache Kafka       │
   │  (FastAPI)   │                        │  unfall.gemeldet    │
   └──────────────┘                        └─────────┬───────────┘
          ▲                                          │ (3) Event lesen
          │ (8) Fälle + Verlauf anzeigen             ▼
          │                                ┌─────────────────────┐
          │                                │  event-consumer     │
          │                                │  „Eingang"          │
          │                                └───┬─────────────┬───┘
          │                        (4) speichern│             │(5) Workflow starten
          │                                     ▼             ▼
   ┌──────┴───────────────────────────────────────┐   ┌───────────────────┐
   │              PostgreSQL                      │   │  Temporal-Server  │
   │        faelle · fall_verlauf                 │   │  (Orchestrator)   │
   └──────▲──────────▲──────────▲─────────────────┘   └─────────┬─────────┘
          │          │          │                               │
          │ (7) jeder Schritt schreibt seinen Verlauf           │ (6) Activities
          │          │          │                               │     nacheinander
     ┌────┴────┐ ┌───┴─────┐ ┌──┴──────────────┐                │
     │mde-agent│ │jav-agent│ │rentenberechnung │ ◄──────────────┘
     │ Agent   │ │ Agent   │ │ deterministisch │
     └─────────┘ └─────────┘ └─────────────────┘
```

### Services und Ports

| Service | Port (auf Ihrem Rechner) | Aufgabe |
|---|---|---|
| `dashboard-api` | **8080** | Weboberfläche, REST-API, **Kafka-Producer** |
| `kafka` | 9092 | Message Broker (Apache Kafka im **KRaft**-Modus, ohne Zookeeper) |
| `temporal` | 7233 | Orchestrator-Engine (echter Temporal-Server) |
| `temporal-ui` | **8233** | Weboberfläche von Temporal |
| `postgres` | 5432 | Fälle und Verlauf (und die Temporal-Datenbank) |
| `event-consumer` | – | liest Kafka-Events, speichert, startet den Workflow |
| `orchestrator` | – | hält die Workflow-Definition (Reihenfolge der Schritte) |
| `mde-agent` | – | Temporal-Worker auf eigener Queue `mde-agent-queue` |
| `jav-agent` | – | Temporal-Worker auf eigener Queue `jav-agent-queue` |
| `rentenberechnung` | – | Temporal-Worker auf eigener Queue `rentenberechnung-queue` |

Jeder Agent ist ein **eigener Container mit eigener Task-Queue**. Der Orchestrator
kennt nur die Namen der Schritte, nicht deren Code – man könnte `mde-agent` austauschen,
ohne die anderen Dienste anzufassen.

---

## 6. Woran sehe ich, dass es wirklich Kafka und Temporal sind?

**Kafka:** In der grünen Bestätigung im Dashboard und im Verlaufseintrag „Eingang"
stehen Topic, Partition und Offset – das sind echte Kafka-Koordinaten. Zum Nachsehen
auf der Kommandozeile:

```bash
# Welche Topics gibt es?
docker compose exec kafka /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server localhost:9092 --list

# Die tatsächlichen Ereignisse im Topic mitlesen
docker compose exec kafka /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 --topic unfall.gemeldet --from-beginning
```

**Temporal:** Öffnen Sie http://localhost:8233. Dort sehen Sie zu jedem Fall einen
Workflow `UnfallSachbearbeitung` mit der ID `unfall-UV-2026-…`, die Event-History
und jede einzelne Activity mit Dauer. Im Dashboard zeigt die Detailansicht unter
„Temporal-Workflow" den Zustand, der per **Temporal-Query** live aus dem laufenden
Workflow geholt wird.

**Datenbank:**

```bash
docker compose exec postgres psql -U postgres -d unfall \
  -c "SELECT fall_nummer, status, mde_prozent, jav_euro, rente_monat FROM faelle;"
```

---

## 7. Warum Temporal – und was wäre die Alternative?

Temporal ist hier bewusst gewählt, weil es genau das Problem löst, das eine
Sachbearbeitungskette hat: Ein Fall läuft über Minuten, Stunden oder Tage, einzelne
Schritte können scheitern und müssen wiederholt werden, und man muss jederzeit sagen
können, wo ein Fall gerade steht. Temporal merkt sich den Fortschritt, wiederholt
fehlgeschlagene Schritte automatisch (hier: bis zu 3 Versuche) und macht den Zustand
abfragbar. Der Aufwand ist überschaubar: zwei Container (Server + UI) und die
Workflow-Datei `services/app/orchestrator/workflow.py`.

Falls Temporal in Ihrer Umgebung nicht laufen darf, wären die naheliegenden
Alternativen: **(a)** Kafka-Topics je Schritt, bei denen jeder Agent das Ergebnis des
Vorgängers liest – einfacher aufzusetzen, aber der Fallzustand ist dann über viele
Topics verteilt und schwer abzufragen; **(b)** eine Workflow-Engine wie **Camunda**
(BPMN, sehr gut für fachlich modellierte Prozesse); **(c)** eine Zustandsspalte in
PostgreSQL plus ein Scheduler-Dienst – am einfachsten, aber Wiederholungen,
Zeitüberschreitungen und Nachvollziehbarkeit muss man selbst bauen. Für diese Demo
läuft echtes Temporal, es war kein Hindernis.

---

## 8. Testfall durchspielen – Schritt für Schritt

1. Terminal im Projektordner öffnen, `docker compose up` eingeben, Enter.
2. Warten, bis die Logzeilen aus Abschnitt 1 erscheinen (erster Start: einige Minuten).
3. Browser öffnen: **http://localhost:8080**. Oben rechts sollte grün
   „Kafka ✓ Temporal ✓" stehen.
4. Im Formular z. B. eintragen: Name `Max Mustermann`, Geburtsjahr `1985`,
   Beruf `Dachdecker`, Körperteil `Bein`, Schweregrad `schwer`.
5. Auf **„Unfall melden → Event auslösen"** klicken.
6. Grüne Bestätigung lesen: Fallnummer + Kafka-Offset. → *Das Event liegt jetzt auf Kafka.*
7. Rechts zusieht: Der Fall erscheint mit Status „Eingegangen", danach wandert die
   gelbe Markierung durch **MdE-Ermittlung → JAV-Ermittlung → Rentenberechnung**.
   Bereits erledigte Stufen werden grün.
8. Nach ca. 10–15 Sekunden: Status **„Abgeschlossen"**, und in der Kachel stehen
   MdE (%), JAV (€) und **Rente/Monat (€)**.
9. Auf die Fallkarte klicken. In der Detailansicht prüfen:
   * **Rechenweg (deterministisch)** – die Formel mit Ihren konkreten Zahlen.
   * **Temporal-Workflow** – Workflow-ID, Status `COMPLETED`, erledigte Schritte 3/3.
   * **Verarbeitungsverlauf** – jeder Schritt mit Uhrzeit, ausführender Komponente,
     Konfidenzbalken und Begründung. „Rohdaten anzeigen" öffnet die Details.
10. Zur Gegenprobe http://localhost:8233 öffnen → Workflow `unfall-UV-…` anklicken →
    dort sieht man dieselben Schritte als echte Temporal-Event-History.
11. Gerne noch 2–3 weitere Fälle melden: gleiche Eingaben ergeben leicht
    unterschiedliche MdE/JAV-Werte (die Agenten streuen), die **Rentenformel** rechnet
    aber immer exakt gleich.

## 9. Beenden und aufräumen

```bash
# Beenden (Strg+C im laufenden Fenster, dann):
docker compose down

# Zusätzlich alle gespeicherten Fälle löschen:
docker compose down -v
```

## 10. Wo steht welcher Code?

```
docker-compose.yml                     alle Services, Ports, Startreihenfolge
services/app/
  api/main.py                          Dashboard-API + Kafka-Producer
  consumer/main.py                     Eingang: Kafka lesen → speichern → Workflow starten
  orchestrator/workflow.py             ► die Reihenfolge der Schritte (Temporal-Workflow)
  orchestrator/worker.py               Statuswechsel + Abschluss
  agenten/mde_agent.py                 MdE-Ermittlung   (simuliert)
  agenten/jav_agent.py                 JAV-Ermittlung   (simuliert)
  agenten/rentenberechnung.py          ► die Rentenformel (deterministisch)
  common/db.py                         Datenbankzugriff + Tabellen
  static/index.html · app.js · stil.css  die Weboberfläche
```

Die zwei fachlich interessantesten Dateien sind mit ► markiert.

## 11. Wenn etwas klemmt

* **Port belegt** (`address already in use`): In `docker-compose.yml` die linke Zahl
  im `ports:`-Eintrag ändern, z. B. `"8081:8080"`, und `http://localhost:8081` öffnen.
* **Oben rechts steht dauerhaft „Kafka … Temporal …"**: Der erste Start braucht
  etwas. Falls es länger als zwei Minuten bleibt: `docker compose logs kafka temporal`.
* **Ein Fall bleibt hängen**: `docker compose logs orchestrator mde-agent jav-agent`
  zeigt den Grund. Temporal wiederholt fehlgeschlagene Schritte bis zu dreimal;
  in der Temporal-Oberfläche sieht man die Fehlermeldung im Klartext.
* **Ganz von vorn**: `docker compose down -v && docker compose up --build`
