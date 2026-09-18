# Unfallversicherung – ereignisgetriebene Sachbearbeitung (MVP)

Diese Demo zeigt, wie ein Unfall in der gesetzlichen Unfallversicherung **automatisch**
durch mehrere Bearbeitungsschritte läuft – ausgelöst durch ein Ereignis („Event"),
gesteuert von einem Orchestrator, ausgeführt von mehreren eigenständigen Diensten.

Sie können den ganzen Ablauf im Browser mitverfolgen. Programmierkenntnisse sind
zum Ausprobieren **nicht** nötig.

---

## 1. Starten

Voraussetzung: **Docker** ist installiert und läuft.

### Zuerst: die IP Ihrer Docker-VM eintragen

Wenn Sie das Ganze **nicht** auf Ihrem eigenen Rechner, sondern auf einer VM
laufen lassen und von außen darauf zugreifen wollen, müssen Sie einmal die
IP-Adresse dieser VM eintragen. Auf der VM ausgeben lassen mit:

```bash
hostname -I
```

Das gibt oft **mehrere** Adressen aus, zum Beispiel:

```
192.168.1.46 172.19.0.1 172.21.0.1 172.17.0.1
```

Nehmen Sie die **erste**. Die Adressen mit `172.` gehören Docker selbst und
funktionieren von außen nicht. Falls Sie unsicher sind: Es ist die Adresse aus
demselben Zahlenbereich wie Ihr eigener Rechner (die ersten drei Zahlen stimmen
überein).

Dann einmalig Ihre eigene Einstellungsdatei anlegen und die IP eintragen –
mit **Ihrer** Adresse, nicht der aus diesem Beispiel:

```bash
cp .env.beispiel .env
nano .env                 # HOST_IP=192.168.1.46 eintragen
```

Nur diese eine Zeile.

> Die Datei `.env` liegt bewusst **nicht** im Git. Dadurch bleiben Ihre
> Einstellungen bei jedem späteren `git pull` unangetastet. Die Ports darunter können so bleiben – sie sind bewusst
nicht die Standardports, damit sie nicht mit anderen Diensten kollidieren.

> Wenn Sie alles direkt auf Ihrem eigenen Rechner starten, können Sie
> `HOST_IP=localhost` stehen lassen.

### Dann starten

Im Ordner dieses Projekts ein Terminal öffnen und eingeben:

```bash
docker compose up
```

### Später aktualisieren

```bash
./update.sh                  # auf main wechseln, neuesten Stand holen, Container neu bauen und starten
./update.sh <branch>         # dasselbe mit einem anderen Branch
```

Das Skript sichert vorher Ihre `.env`, holt den neuen Stand, stellt die `.env`
wieder her und baut das Anwendungs-Image neu. Danach zeigt es die Adressen von
Dashboard und Designer an. Logs: `docker compose logs -f`.

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
uv-llm-agent        | llm-agent läuft auf Queue 'llm-agent-queue' (ohne Schluessel)
```

## 2. Welche Seite öffne ich?

Setzen Sie für `<HOST_IP>` die IP ein, die Sie in die `.env` eingetragen haben
(also z. B. `192.168.1.46`):

| Was | URL |
| --- | --- |
| **Dashboard – hier klicken Sie** | **http://\<HOST_IP\>:28080** |
| **Prozessdesigner** – den Ablauf selbst zusammenstecken (Abschnitt 4c) | http://\<HOST_IP\>:28080/designer |
| Temporal-Oberfläche (der Blick „unter die Motorhaube") | http://\<HOST_IP\>:28233 |

Alles Weitere passiert auf **http://\<HOST_IP\>:28080**.

Der Link zur Temporal-Oberfläche oben rechts im Dashboard richtet sich
automatisch nach der Adresse, mit der Sie das Dashboard aufgerufen haben –
Sie müssen dafür nichts einstellen.

> **Kommen Sie nicht durch?** Auf der VM prüfen, ob die Ports offen sind:
> `sudo ufw allow 28080/tcp` und `sudo ufw allow 28233/tcp` (bei aktiver
> Firewall). Bei VirtualBox/VMware zusätzlich die Portweiterleitung bzw. den
> Netzwerkmodus „Bridged" prüfen.

## 3. Was klicke ich an?

1. Links im Formular **„Unfall melden"** stehen bereits sinnvolle Beispielwerte.
   Sie können sie übernehmen oder ändern (Name, Beruf, Körperteil, Schweregrad …).
2. Auf **„Unfall melden → Event auslösen"** klicken.
3. Unter dem Knopf erscheint eine grüne Bestätigung mit der Fallnummer und der
   Stelle, an der das Ereignis in Kafka gelandet ist
   (z. B. `Topic unfall.gemeldet, Partition 0, Offset 3`).
4. Rechts taucht der Fall auf. Nach wenigen Sekunden bleibt er bei
   **„Wartet auf Gutachten"** stehen – das ist Absicht: In der Wirklichkeit
   wird jetzt ein Gutachter beauftragt, und das dauert Wochen.
5. In der lila umrandeten Box: auf **„Beispiel-Gutachten als PDF herunterladen"**
   klicken. Sie bekommen ein fertiges Gutachten passend zu diesem Fall.
6. Dieselbe Datei über **„Datei auswählen"** und **„Gutachten einreichen"**
   wieder hochladen. Damit wird der wartende Workflow geweckt und der
   **mde-agent** liest die MdE aus dem Dokument.
7. Kurz darauf bleibt der Fall erneut stehen: **„Wartet auf Entgeltmeldung"**.
   Tragen Sie ein Jahresbrutto ein und klicken **„Meldung erfassen"** – oder
   klicken Sie **„Antwort simulieren"**.
8. Der Rest läuft von selbst: JAV-Prüfung, Rentenberechnung, Ergebnis.
9. Auf die Fallkarte **klicken** → Detailansicht mit dem kompletten Verlauf:
   jeder Schritt mit Uhrzeit, Ergebnis, Konfidenz und Begründung, das
   hochgeladene Gutachten zum Öffnen, die Fundstelle im Gutachtentext, der
   Rechenweg der Rente und der Zustand des Temporal-Workflows.

> **Warten Sie ruhig einmal ab, ohne etwas hochzuladen.** Nach 30 Sekunden
> erscheint im Verlauf „1. Erinnerung versendet" – ausgelöst von einem
> Temporal-Timer. Danach wartet der Fall einfach weiter. Sie können auch alle
> Container neu starten: der Fall wartet danach an genau derselben Stelle
> weiter, weil Temporal den Zustand hält.

---

## 4. Was passiert in jedem Schritt?

| # | Schritt | Wer macht es? | Was passiert? |
|---|---------|---------------|---------------|
| 0 | Meldung | **dashboard-api** | Ihr Klick erzeugt ein **echtes Kafka-Event** auf dem Topic `unfall.gemeldet`. Die Weboberfläche ruft *keinen* Bearbeitungsdienst direkt auf. |
| 1 | **Eingang** | **event-consumer** | Liest das Event von Kafka, speichert den Fall in **PostgreSQL** und informiert den Orchestrator, indem er einen **Temporal-Workflow** startet. |
| 2 | **Gutachten** | **orchestrator** | Beauftragt die ärztliche Begutachtung – und **wartet**. Der Fall bleibt stehen, bis Sie ein Gutachten hochladen. Nach Fristablauf verschickt ein Temporal-Timer eine Erinnerung, danach wird weiter gewartet. |
| 3 | **MdE-Ermittlung** | **mde-agent** | Liest die *Minderung der Erwerbsfähigkeit* **aus dem hochgeladenen Gutachten**, mit Konfidenzwert, Begründung und der Fundstelle im Text. Mit hinterlegtem Claude-Schlüssel wertet Claude das Dokument aus, sonst greift eine Textsuche. |
| 4 | **Entgeltmeldung** | **orchestrator** | Fordert den Jahresverdienst bei Unternehmer und Versichertem an – und **wartet** erneut, mit derselben Erinnerungslogik. |
| 5 | **JAV-Ermittlung** | **jav-agent** | Prüft die gemeldete Summe auf Plausibilität: Vergleich mit dem berufsüblichen Verdienst, Mindest- und Höchst-JAV. Liefert Konfidenz und Begründung. |
| 6 | **Rentenberechnung** | **rentenberechnung** | **Kein Agent, keine Simulation.** Feste Rechenregel: `Rente/Jahr = 2/3 × JAV × (MdE / 100)`, `Rente/Monat = Rente/Jahr ÷ 12`. Formel und Ergebnis stehen in der Detailansicht. |
| 7 | **Ergebnis** | **orchestrator** | Der Fall wird auf „Abgeschlossen" gesetzt. Der Rentenbetrag steht fest. |

### Warum die Wartezeiten der interessanteste Teil sind

Genau hier verdient sich Temporal seinen Platz. Zwischen „Gutachten beauftragt"
und „Gutachten da" liegen in der Realität Wochen. Während dieser Zeit:

* blockiert **kein** Prozess, **kein** Thread, **keine** Datenbankverbindung,
* geht **kein** Zustand verloren – Sie dürfen alle Container neu starten,
* läuft die **Frist** trotzdem weiter, und nach Ablauf verschickt ein Timer
  automatisch eine Erinnerung,
* wird der Fall **sofort** fortgesetzt, sobald das Dokument eintrifft
  (technisch: ein *Signal* an den laufenden Workflow).

Ohne so eine Engine müsste man das mit Statusspalten, Cronjobs und viel
Fehlerbehandlung selbst bauen.

### Ehrlich gesagt: was ist echt, was ist simuliert?

* **Echt:** Kafka als Message Broker, Temporal als Orchestrator (samt Signalen
  und Timern), PostgreSQL als Speicher, die getrennten Dienste, das Hochladen
  und Auslesen des Gutachtens, und die **Rentenformel**.
* **Simuliert:** Der *Inhalt* des Beispiel-Gutachtens (frei erfundener Befund
  mit plausiblem MdE-Wert) und die *Vergleichswerte je Beruf*, an denen der
  `jav-agent` die gemeldete Summe misst. Die Agenten warten zusätzlich 2,5–5
  Sekunden, damit man ihnen im Dashboard zusehen kann. Und die Fristen sind auf
  30 Sekunden gestaucht statt auf Wochen.
* **Nicht simuliert, aber vereinfacht:** Der `mde-agent` liest einen echten
  Wert aus einem echten Dokument. Wie gut er das tut, hängt davon ab, ob ein
  Claude-Schlüssel hinterlegt ist (siehe unten).

Es geht in dieser Demo um den **Ablauf**, nicht um fachliche Korrektheit.

---

## 4b. Optional: Claude die MdE aus dem Gutachten lesen lassen

**Ohne Schlüssel läuft alles.** Der `mde-agent` sucht dann im Gutachtentext
nach Formulierungen wie „Die MdE beträgt 30 v. H." und übernimmt die Zahl.
Für die Demo reicht das vollkommen.

**Mit Schlüssel** liest Claude das Dokument inhaltlich: Es findet den Wert
auch bei abweichender Formulierung, begründet ihn mit Bezug auf den Befund
und meldet einen eigenen Konfidenzwert.

So hinterlegen Sie den Schlüssel:

```bash
cp geheim.env.beispiel geheim.env
nano geheim.env          # ANTHROPIC_API_KEY=sk-ant-... eintragen
docker compose up -d --force-recreate mde-agent
```

Ob es greift, sehen Sie unten links im Dashboard („Der mde-agent liest das
Gutachten mit …") und im Log: `mde-agent läuft auf Queue … (Extraktion: Claude)`.

> **Der Schlüssel gehört ausschließlich in `geheim.env`.** Diese Datei steht in
> der `.gitignore` und wird nie mitversioniert. Tragen Sie ihn **nicht** in die
> `.env` ein – die liegt im Git. Schicken Sie einen Schlüssel auch nie per Chat
> oder Ticket; wenn doch einmal einer abhandenkommt, in der
> [Anthropic Console](https://console.anthropic.com/settings/keys) widerrufen.

Kosten: Ein Gutachten ist etwa zwei Seiten Text, also ein sehr kleiner Aufruf.
Die Extraktion läuft ausschließlich beim Hochladen eines Gutachtens – im
Leerlauf entstehen keine Kosten.

---

## 4c. Der Prozessdesigner – den Ablauf selbst zusammenstecken

Bis hierhin stand die Reihenfolge der Schritte fest im Code. Der
**Prozessdesigner** unter http://\<HOST_IP\>:28080/designer macht daraus
einen Graphen, den man im Browser umbaut: Bausteine aus der Palette einfügen,
verschieben, verbinden, speichern. Neue Unfallmeldungen laufen dann mit dem
neuen Ablauf – als ganz normaler Temporal-Workflow.

Der Punkt dieser Ergänzung ist nicht, dass man das braucht. Der Punkt ist zu
zeigen, **wie wenig dazu nötig ist**, wenn die Engine darunter schon da ist:

* ein JSON-Format für Knoten und Kanten (`common/prozess.py`),
* ein Workflow, der den Graphen Knoten für Knoten abläuft und dabei dieselben
  Activities aufruft wie vorher (`orchestrator/prozess_workflow.py`, ~200 Zeilen),
* eine Zeichenfläche ohne Framework und ohne Build-Schritt
  (`static/prozessgraph.js`, `static/designer.js`).

### Bausteine

| Baustein | Was er tut | Was dahinter steckt |
|---|---|---|
| **Aktivität** | ruft einen der vorhandenen Dienste auf | `execute_activity` auf der Task-Queue des Dienstes |
| **Warten auf Ereignis** | hält den Fall an, bis Gutachten bzw. Entgeltmeldung eintreffen; erinnert nach Ablauf der Frist | `wait_condition` + Timer + Signal – genau wie im festen Workflow |
| **Bedingung** | vergleicht einen Wert aus dem Kontext, Ausgänge *ja* / *nein* | ein `if` |
| **Agent (Claude)** | stellt Claude eine Frage mit Werten aus dem Kontext, liefert Antwort, Einschätzung (ja/nein/unklar) und Konfidenz | eine Activity im Dienst `llm-agent`; ohne Schlüssel antwortet er ehrlich „unklar" |
| **Notiz** | schreibt einen Eintrag in den Verlauf | nützlich, um einen Zweig sichtbar zu machen |
| **Start / Ende** | Anfang und Abschluss des Falls | |

**Datenfluss:** Alle Knoten teilen sich einen **Kontext** – ein einfaches
Wörterbuch. Am Anfang liegt darin nur `fall`; jeder Baustein legt sein
Ergebnis unter einem Namen ab (`mde`, `jav`, `rente`, `agent` …), und spätere
Bausteine greifen per Pfad darauf zu: `mde.mde_prozent`, `rente.rente_monat_euro`,
`agent.einschaetzung`. In Prompts und Notizen schreibt man `{{mde.mde_prozent}}`.

### Versionen – der eigentlich wichtige Teil

Jedes Speichern legt eine **neue Version** an; alte Versionen werden nie
verändert. Beim Start eines Falls wird der Graph der gewählten Version dem
Temporal-Workflow **als Argument** übergeben und liegt damit in dessen
Historie. Deshalb gilt:

* Ein Fall läuft bis zum Ende mit der Version, mit der er gestartet wurde –
  auch wenn er wochenlang auf ein Gutachten wartet und der Prozess in der
  Zwischenzeit dreimal umgebaut wurde.
* Ein Neustart aller Container ändert daran nichts: Temporal spielt die
  Historie nach, und der Graph ist Teil der Historie.
* Im Formular „Unfall melden" lässt sich die Version wählen; ohne Auswahl gilt
  die im Designer **aktivierte** Version.

Ohne diese Regel bricht eine Prozess-Engine bei der ersten Änderung an einem
laufenden Prozess. Das ist bei jedem Werkzeug dieser Art so, egal wie hübsch der
Canvas ist.

### So probiert man es aus

1. http://\<HOST_IP\>:28080/designer öffnen. Version 1 ist der Standardablauf.
2. Aus der Palette **Bedingung** einfügen, z. B. `rente.rente_monat_euro >= 100`.
   Die Kante von „Rente berechnen" auf die Bedingung ziehen (vom Punkt unten am
   Knoten zum Zielknoten), Ausgang **ja** auf „Ende", Ausgang **nein** auf eine
   neue **Notiz** („Bagatellrente, bitte prüfen") und von dort auf „Ende".
3. **Prüfen**, dann **Als neue Version speichern** (Kommentar eintragen).
   Version 2 ist jetzt aktiv.
4. Im Dashboard einen Unfall melden. In der Fallkarte steht „Prozess v2";
   in der Detailansicht unter **Prozess (live)** färbt sich der Graph mit
   dem Fortschritt und zeigt, wie die Bedingung entschieden wurde.
5. Gegenprobe zur Versionierung: einen Fall mit v1 starten (Auswahl im
   Formular), bei „Wartet auf Gutachten" stehen lassen, im Designer eine
   Version 3 speichern und aktivieren – der wartende Fall läuft weiterhin mit
   v1 durch, ein neuer Fall mit v3.

### Was der Designer bewusst nicht kann

Parallele Zweige, Schleifen mit Abbruchbedingung, Fehlerpfade je Knoten,
Undo, Rechte, mehrere Prozesse. Jedes davon ist machbar, jedes verdoppelt
etwa den Interpreter. Eine Schleife im Graphen wird beim Prüfen als Warnung
gemeldet; der Interpreter bricht nach 200 Schritten ab.

---

## 5. Die Architektur

```
       Browser  ──  Dashboard  http://<HOST_IP>:28080
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
     │ liest   │ │ prüft   │ │ deterministisch │
     │Gutachten│ │ Meldung │ │                 │
     └─────────┘ └─────────┘ └─────────────────┘
```

Und die beiden Wartepunkte, an denen der Workflow stehen bleibt, bis Sie
etwas einreichen:

```
   Orchestrator                     Sie im Dashboard
        │
        │ Gutachten beauftragt
        ▼
   ┌──────────────┐
   │   WARTET     │ ◄── Timer: nach 30 s „Erinnerung versendet", dann weiter warten
   │              │
   │              │ ◄────── Signal „gutachten_eingegangen"  ◄── PDF hochgeladen
   └──────┬───────┘
          │ weiter mit dem mde-agent
          ▼
   ┌──────────────┐
   │   WARTET     │ ◄── Timer: dieselbe Erinnerungslogik
   │              │
   │              │ ◄────── Signal „entgeltmeldung_eingegangen" ◄── Betrag erfasst
   └──────┬───────┘
          │ weiter mit dem jav-agent
          ▼
```

### Services und Ports

Alle Ports stehen in der `.env` und lassen sich dort ändern.

| Service | Port auf der VM | in `.env` | Aufgabe |
|---|---|---|---|
| `dashboard-api` | **28080** | `DASHBOARD_PORT` | Weboberfläche, REST-API, **Kafka-Producer** |
| `kafka` | 29092 | `KAFKA_PORT` | Message Broker (Apache Kafka im **KRaft**-Modus, ohne Zookeeper) |
| `temporal` | 27233 | `TEMPORAL_PORT` | Orchestrator-Engine (echter Temporal-Server) |
| `temporal-ui` | **28233** | `TEMPORAL_UI_PORT` | Weboberfläche von Temporal |
| `postgres` | 25432 | `POSTGRES_PORT` | Fälle und Verlauf (und die Temporal-Datenbank) |
| `event-consumer` | – | – | liest Kafka-Events, speichert, startet den Workflow |
| `orchestrator` | – | – | führt den Prozessgraphen aus (Interpreter-Workflow `Prozess`) |
| `mde-agent` | – | – | Temporal-Worker auf eigener Queue `mde-agent-queue` |
| `jav-agent` | – | – | Temporal-Worker auf eigener Queue `jav-agent-queue` |
| `rentenberechnung` | – | – | Temporal-Worker auf eigener Queue `rentenberechnung-queue` |
| `llm-agent` | – | – | generischer Claude-Agent für den Baustein „Agent" im Designer, Queue `llm-agent-queue` |

Untereinander reden die Container über das interne Compose-Netz (`kafka:9092`,
`temporal:7233`, `postgres:5432`). Die Ports oben sind nur dafür da, dass **Sie**
von außen darauf zugreifen können.

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

**Temporal:** Öffnen Sie http://\<HOST_IP\>:28233. Dort sehen Sie zu jedem Fall einen
Workflow `Prozess` mit der ID `unfall-UV-2026-…` (in der Eingabe steht der
komplette Prozessgraph), die Event-History
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
abfragbar. Der Aufwand ist überschaubar: zwei Container (Server + UI) und eine
Workflow-Datei (`services/app/orchestrator/prozess_workflow.py`; der ursprüngliche,
fest programmierte Ablauf steht zum Vergleich weiter in `workflow.py`).

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
3. Browser öffnen: **http://\<HOST_IP\>:28080** (also z. B.
   `http://192.168.1.46:28080` – Ihre Adresse aus `hostname -I`). Oben rechts sollte grün
   „Kafka ✓ Temporal ✓" stehen.
4. Im Formular z. B. eintragen: Name `Max Mustermann`, Geburtsjahr `1985`,
   Beruf `Dachdecker`, Körperteil `Bein`, Schweregrad `schwer`.
5. Auf **„Unfall melden → Event auslösen"** klicken.
6. Grüne Bestätigung lesen: Fallnummer + Kafka-Offset. → *Das Event liegt jetzt auf Kafka.*
7. Rechts erscheint der Fall. Die Stufen **Eingang** wird grün, dann bleibt er
   bei **„Wartet auf Gutachten"** stehen (lila). Das ist der eingebaute
   Wartepunkt – hier würde in echt der Gutachter arbeiten.
8. **Jetzt einmal 30 Sekunden nichts tun.** Im Verlauf (Fallkarte anklicken)
   erscheint „1. Erinnerung an die begutachtende Praxis versendet". Ausgelöst
   hat das ein Temporal-Timer, nicht Sie. Der Fall wartet danach weiter.
9. In der lila Box auf **„Beispiel-Gutachten als PDF herunterladen"** klicken.
   Öffnen Sie das PDF ruhig – im Abschnitt 5 steht „Die Minderung der
   Erwerbsfähigkeit (MdE) beträgt **XX** v. H.". Merken Sie sich die Zahl.
10. Dieselbe Datei über **„Datei auswählen"** wählen und
    **„Gutachten einreichen"** klicken.
11. Der Fall läuft sofort weiter: **MdE** wird grün, und in der Kachel steht
    genau die Zahl aus dem PDF. → *Der Agent hat sie wirklich aus dem
    Dokument gelesen.*
12. Nächster Wartepunkt: **„Wartet auf Entgeltmeldung"**. Tragen Sie
    z. B. `52000` ein und klicken **„Meldung erfassen"** – oder klicken Sie
    **„Antwort simulieren"**.
13. Der Rest läuft von selbst durch. Nach wenigen Sekunden: **„Abgeschlossen"**
    mit MdE (%), JAV (€) und **Rente/Monat (€)**.
14. Auf die Fallkarte klicken. In der Detailansicht prüfen:
    * **Eingegangene Dokumente** – Ihr hochgeladenes Gutachten, anklickbar.
    * **Rechenweg (deterministisch)** – die Formel mit Ihren konkreten Zahlen.
    * **Temporal-Workflow** – Status `COMPLETED`, erledigte Schritte 5/5,
      Anzahl der Erinnerungen.
    * **Verarbeitungsverlauf** – jeder Schritt mit Uhrzeit, Komponente,
      Konfidenzbalken, Begründung und der **Fundstelle im Gutachten**.
15. Zur Gegenprobe http://\<HOST_IP\>:28233 öffnen → Workflow `unfall-UV-…`
    anklicken → dort sieht man dieselben Schritte als echte Temporal-Event-History,
    inklusive der Timer und der beiden Signale.

**Die eindrucksvollste Probe:** Melden Sie einen Unfall, warten Sie bis
„Wartet auf Gutachten", und stoppen Sie dann **alles**:

```bash
docker compose down          # ohne -v, sonst sind die Daten weg
docker compose up -d
```

Der Fall steht danach unverändert auf „Wartet auf Gutachten" – laden Sie das
Gutachten hoch, und er läuft weiter, als wäre nichts gewesen.

## 9. Beenden und aufräumen

```bash
# Beenden (Strg+C im laufenden Fenster, dann):
docker compose down

# Zusätzlich alle gespeicherten Fälle löschen:
docker compose down -v
```

## 10. Wo steht welcher Code?

```
.env.beispiel                          Vorlage: IP-Adresse und Ports
geheim.env.beispiel                    Vorlage für den Claude-API-Schlüssel
docker-compose.yml                     alle Services, Ports, Startreihenfolge
config/dynamicconfig/                  Einstellungen für den Temporal-Server
services/app/
  api/main.py                          Dashboard-API + Kafka-Producer
  consumer/main.py                     Eingang: Kafka lesen → speichern → Workflow starten
  orchestrator/prozess_workflow.py     ► der Interpreter: läuft den Prozessgraphen ab
  orchestrator/workflow.py             der ursprüngliche, fest programmierte Ablauf
  orchestrator/worker.py               Statuswechsel, Verlauf, Abschluss
  common/prozess.py                    ► Graph-Format, Bausteinkatalog, Prüfung, Standardprozess
  agenten/llm_agent.py                 generischer Claude-Agent (Baustein „Agent")
  agenten/mde_agent.py                 MdE aus dem Gutachten lesen
  agenten/jav_agent.py                 gemeldeten JAV plausibilisieren
  common/gutachten.py                  ► Beispiel-PDF, Textextraktion, Claude
  agenten/rentenberechnung.py          ► die Rentenformel (deterministisch)
  common/db.py                         Datenbankzugriff + Tabellen
  static/index.html · app.js · stil.css  die Weboberfläche
  static/designer.html · designer.js   der Prozessdesigner
  static/prozessgraph.js               Zeichenfläche (Designer + Live-Ansicht im Dialog)
  tests/test_prozess.py                Prüfung + Interpreter gegen einen Temporal-Testserver
```

Die zwei fachlich interessantesten Dateien sind mit ► markiert.

## 11. Wenn etwas klemmt

* **„Der Fall wartet und wartet"** – das ist so gewollt. Der Workflow bleibt bei
  „Wartet auf Gutachten" bzw. „Wartet auf Entgeltmeldung" stehen, bis Sie in der
  lila Box etwas einreichen. Nach 30 Sekunden erscheint eine Erinnerung im
  Verlauf, danach wartet er weiter. Die Frist lässt sich in der `.env` über
  `FRIST_GUTACHTEN_SEKUNDEN` ändern.
* **Der Upload wird mit „Dieser Fall wartet gerade nicht auf ein Gutachten"
  abgelehnt**: Der Fall ist über diesen Schritt schon hinaus. Ein Gutachten
  lässt sich nur einreichen, solange der Fall auch darauf wartet.
* **`git pull` meldet „Ihre lokalen Änderungen … würden überschrieben"**:
  Das betrifft alte Stände, in denen `.env` noch mitversioniert war. Einmalig
  das Update-Skript aus dem Git holen und ab dann immer damit aktualisieren:
  ```bash
  git fetch origin
  git checkout -B main origin/main
  ./update.sh
  ```
  Das Skript sichert Ihre `.env` und stellt sie danach wieder her.
* **`Unable to create dynamic config client`** und der Temporal-Container
  startet immer wieder neu: Die Datei `config/dynamicconfig/development-sql.yaml`
  fehlt oder wird nicht hineingemountet. Sie gehört zum Projekt und muss
  vorhanden sein – prüfen mit `ls config/dynamicconfig/`. Fehlt sie, hilft
  ein `git pull`.
* **Alle Dienste melden `Warte auf Temporal … Name or service not known`**:
  Der Temporal-Container läuft nicht. Der Hostname `temporal` existiert im
  Docker-Netz nur, solange der Container läuft. Nachsehen mit
  `docker compose ps -a` und `docker compose logs temporal --tail=50` – die
  Ursache steht immer in den letzten Zeilen dieses Logs.
* **`pull access denied for unfall-mvp-app`**: Docker versucht, das Image der
  Anwendung herunterzuladen, statt es zu bauen. Es gibt dieses Image in keiner
  Registry – es entsteht erst beim Start aus `services/app/Dockerfile`. In der
  `docker-compose.yml` ist dafür `pull_policy: build` gesetzt. Bei einer
  älteren Compose-Version, die das ignoriert, hilft:
  `docker compose up --build`.
* **Port belegt** (`address already in use`): In der Datei `.env` den betroffenen
  Port auf eine freie Zahl ändern (z. B. `DASHBOARD_PORT=28081`), dann
  `docker compose up -d` erneut ausführen.
* **Seite lädt nicht von einem anderen Rechner aus**: Die Dienste hören auf allen
  Netzwerkschnittstellen (`0.0.0.0`), es liegt also fast immer an der Firewall
  der VM oder am Netzwerkmodus der VM. Prüfen mit
  `curl http://localhost:28080/api/health` **auf der VM selbst** – kommt dort
  `{"status":"ok",…}`, läuft alles und es fehlt nur der Weg von außen.
* **Im Dashboard steht der Temporal-Link falsch**: Er wird aus der Adresse
  gebaut, mit der Sie das Dashboard aufgerufen haben. Wenn Sie ihn fest
  vorgeben wollen, setzen Sie in der `.env` zusätzlich
  `TEMPORAL_UI_URL=http://192.168.1.46:28233`.
* **Oben rechts steht dauerhaft „Kafka … Temporal …"**: Der erste Start braucht
  etwas. Falls es länger als zwei Minuten bleibt: `docker compose logs kafka temporal`.
* **Ein Fall bleibt hängen**: `docker compose logs orchestrator mde-agent jav-agent`
  zeigt den Grund. Temporal wiederholt fehlgeschlagene Schritte bis zu dreimal;
  in der Temporal-Oberfläche sieht man die Fehlermeldung im Klartext.
* **Ganz von vorn**: `docker compose down -v && docker compose up --build`
