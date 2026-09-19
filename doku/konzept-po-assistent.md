# PO-Assistent auf der Prozessplattform – Konzept

Version 0.2 · Stand 2026-09-19 · Status: Diskussionsgrundlage

Ersetzt das Konzept v0.1 (eigenständige Next.js-App). Der Unterschied: v0.1
hätte eine zweite Workflow-Engine gebaut. Diese Engine existiert schon in
diesem Repository. Das Konzept beschreibt deshalb, was dem vorhandenen
System fehlt, damit es zum PO-Assistenten wird, und nicht ein neues System.

---

## 0. Kurzfassung

**Ein Eingabefeld.** Was darin landet (Text, Sprachmemo, Screenshot, Datei),
wird zu einem Ereignis auf Kafka. Ein Prozess „Eingang", der wie alle anderen
im Prozessdesigner liegt, ordnet das Ereignis ein und startet den passenden
Fachprozess als Unterprozess: Epic/Feature/Story-Schnitt, Recherche,
Statusfrage, später mehr. Jeder Fachprozess ist ein versionierter Graph aus
Agenten-Knoten, Rückfragen, Bedingungen und Konnektoren. Ergebnisse sind
Artefakte am Vorgang, sichtbar im Dashboard, exportierbar nach Jira.

Das Bild, das du beschrieben hast, ist genau das, was der Code heute schon
zur Hälfte tut: Kafka-Event → Consumer → Temporal-Workflow interpretiert
einen Graphen → Agenten-Knoten fragt Claude. Was fehlt, ist in Abschnitt 3
als konkrete Lücke im Code benannt.

Drei Entscheidungen, die ich treffe und die du kippen kannst:

1. **Kein drittes Repository.** Der PO-Assistent ist eine Generalisierung
   dieses Projekts, keine neue App. Begründung in Abschnitt 8.
2. **Der Router ist selbst ein Prozess im Designer**, kein Code. Dafür braucht
   der Interpreter einen Knoten „Unterprozess starten".
3. **Prompts und Ausgabeschemata der Agenten leben nicht im Graphen**, sondern
   als versionierte Agentenvorlagen mit Tests. Der Graph referenziert sie.
   Begründung in Abschnitt 9, Punkt 1. Das ist der Punkt, an dem ich am
   deutlichsten von „alles im Designer" abweiche.

---

## 1. Was heute da ist

Stand `main` (5de76fe), damit wir über dieselbe Basis reden:

| Baustein | Datei | Kann | Kann nicht |
|---|---|---|---|
| Ereigniseingang | `consumer/main.py` | Kafka-Topic lesen, Fall anlegen, Workflow mit aktiver Graph-Version starten | Nur ein Topic (`unfall.gemeldet`), nur ein Prozessname, Nutzlast ist ein Unfall |
| Interpreter | `orchestrator/prozess_workflow.py` | Graph Knoten für Knoten, Kontext-Dict, Signale, Fristen, Bedingungen | Unterprozesse, Parallelität, Schleifen mit Abbruch, generische Rückfragen |
| Graph-Format | `common/prozess.py` | 7 Knotentypen, Katalog, Validierung, Pfadauflösung, `{{ }}`-Vorlagen | Katalog ist fest kodiert; Signale sind fachlich (Gutachten, Entgeltmeldung) |
| Agent-Knoten | `agenten/llm_agent.py` | Prompt mit Kontextwerten an Claude, Antwort/Einschätzung/Konfidenz | Kein eigenes Ausgabeschema, kein System-Prompt je Knoten, keine Dokumente/Bilder, keine Tools |
| Designer | `static/designer.js`, `prozessgraph.js` | Palette, Canvas, Eigenschaften, Versionen, Prüfen, Aktivieren | Ein Prozess (`const PROZESS`), Pfadvorschläge fest kodiert |
| Persistenz | `common/db.py` | `prozess_definitionen` versioniert, `faelle`, `fall_verlauf`, Dokumente | Vorgänge sind Unfälle; keine Artefakte |
| Konnektoren | Spielwiese, n8n, Jira-Stub | Mail → Agent → Jira als n8n-Flow | Nicht aus dem Temporal-Graphen aufrufbar |

Das ist mehr Substanz, als es ein neues Repo in zwei Wochen hätte.

## 2. Zielbild

```
 ┌──────────────────────────────────────────────────────────────────────┐
 │  Dashboard: EIN Eingabefeld (Text · Mikro · Datei · Screenshot)      │
 └──────────────┬───────────────────────────────────────────────────────┘
                │ POST /api/eingang
 ┌──────────────▼───────────────┐
 │ Normalisierung (dashboard-api)│  STT für Audio · Dateien ablegen ·
 │                               │  Vorgang anlegen · Event schreiben
 └──────────────┬───────────────┘
                │ Kafka  topic: eingang
 ┌──────────────▼───────────────┐
 │ event-consumer                │  startet Workflow "Prozess" mit der
 │                               │  aktiven Version von "Eingang"
 └──────────────┬───────────────┘
                │
 ┌──────────────▼──────────────────────────────────────────────────────┐
 │ Prozess "Eingang" (im Designer, versioniert)                        │
 │                                                                     │
 │  start → agent "Einordnen" → bedingung konfidenz ≥ 0.8 ─ja─┐        │
 │                                   │nein                     │        │
 │                          rueckfrage "Meintest du…?"         │        │
 │                                   │                         ▼        │
 │                                   └────────► unterprozess {{einordnung.prozess}}
 │                                                            │        │
 │                                                          ende       │
 └────────────────────────────────────────────────────────────┬────────┘
                                                              │ Child-Workflow
        ┌───────────────────────┬───────────────────────┬─────┴─────────────┐
        ▼                       ▼                       ▼                   ▼
 po.story-schnitt        po.recherche           po.status-frage      (weitere, im
 Epic→Feature→Story      Web + Kontext          Antwort aus           Designer
 → Rückfrage → Jira      → Kritik → Memo        Historie              angelegt)
```

Alles unterhalb des Consumers ist Graph im Designer. Der Consumer selbst
kennt nur noch: „Topic `eingang` → Prozess `Eingang`".

### Regel: Prozess oder Agentenaufruf?

Du hast gefragt, ob manches „nur ein Agent-Aufruf" ist. Ja, und die Grenze
ist einfach:

- **Ein Agenten-Knoten reicht**, wenn es keine Wartezeit, keine Verzweigung,
  keine Nebenwirkung (Jira, Mail) und nur einen Denkschritt gibt. Beispiel:
  Statusfrage. Das ist dann ein kurzer Prozess mit einem Knoten, oder ein
  Zweig direkt im Prozess „Eingang".
- **Ein eigener Prozess** ist nötig, sobald eines davon vorkommt. Beispiel:
  Story-Schnitt, weil eine Rückfrage drin ist und am Ende Jira geschrieben wird.

Beides wird im Designer gebaut. Der Unterschied ist nur die Anzahl Knoten.

## 3. Was fehlt: die Lücken, konkret

Sortiert nach Abhängigkeit. Jede Lücke nennt die Stelle im Code.

### 3.1 Vom Fall zum Vorgang

Heute ist alles ein Unfall: Tabelle `faelle`, Kontext `fall`, Formular mit
Beruf und Körperteil. Nötig ist ein generischer **Vorgang** mit beliebiger
Nutzlast:

```
vorgang       id, nummer, prozess_name, prozess_version, status, titel,
              eingang (jsonb: text, anhaenge[], quelle, erstellt_am),
              workflow_id, workflow_run_id, uebergeordneter_vorgang_id
vorgang_verlauf   wie fall_verlauf
artefakt      id, vorgang_id, typ, version, titel, inhalt (markdown/json),
              erzeugt_von_knoten, erstellt_am
anhang        id, vorgang_id, dateiname, mime, bytes, transkript/extrakt
```

Der Unfall-Demoprozess bleibt als **ein Prozess unter vielen** erhalten; sein
Formular schreibt dann einen Vorgang mit `eingang.quelle = "unfallformular"`.
Er ist ein guter Regressionstest für den Interpreter, und die Demo bleibt
vorführbar.

### 3.2 Mehrere Prozesse

- `common/prozess.py`: `PROZESS_NAME` weg. Prozesse haben Name, Beschreibung,
  Trigger (welches Topic oder „nur als Unterprozess").
- `designer.js`: `const PROZESS` wird zu einer Prozessauswahl mit „Neuer
  Prozess". `BEKANNTE_PFADE` kommt aus dem Katalog plus den `ergebnis`-Namen
  des geladenen Graphen, nicht aus einer festen Liste.
- `consumer/main.py`: Zuordnung Topic → Prozess aus der Datenbank, nicht aus
  `config`.

### 3.3 Der Eingang

Neuer Endpunkt `POST /api/eingang` (multipart): Text, 0..n Dateien, optional
Audio. Was er tut, bevor das Event geschrieben wird:

| Eingabe | Verarbeitung | Was im Kontext landet |
|---|---|---|
| Text | nichts | `eingang.text` |
| Audio | STT (whisper.cpp als eigener Container, Queue `stt-queue`) | `eingang.text` (Transkript), `eingang.anhaenge[0].transkript`; Transkript wird dem Nutzer **vor** dem Routing gezeigt |
| Bild / Screenshot | ablegen; Beschreibung + sichtbarer Text erzeugt der erste Agenten-Knoten mit Bildeingabe | `eingang.anhaenge[i].id`, `.mime` |
| PDF | ablegen; Claude liest es nativ | `eingang.anhaenge[i].id` |
| DOCX/XLSX | serverseitig zu Text/Tabelle wandeln | `eingang.anhaenge[i].extrakt` |

Wichtige technische Regel: **Dateiinhalte gehören nie in den Kontext.** Der
Kontext liegt in der Temporal-Historie (Argument des Workflows, Ergebnis
jeder Activity). Temporal hat pro Nachricht ein Limit von wenigen MB und pro
Historie eines von ~50 MB. Im Kontext steht nur die `anhang.id`; die Activity
lädt die Bytes aus der Datenbank und gibt sie Claude als `document`- bzw.
`image`-Block mit.

Der Mikrofon-Button ist der Grund, warum das Dashboard als PWA installierbar
sein sollte: Sprachmemo vom Handy, ohne App-Store.

### 3.4 Neue Knotentypen

Fünf Knoten, alle nach demselben Muster wie die vorhandenen (Katalog-Eintrag,
`knoten_vervollstaendigen`, `pruefen`, Zweig in `_knoten_ausfuehren`,
Eigenschaften-Panel im Designer).

**`agent` v2** (Erweiterung des vorhandenen)

```jsonc
{
  "typ": "agent",
  "vorlage": "po.einordnen",            // Agentenvorlage aus dem Katalog (System-Prompt, Schema, Modell, Effort, Tools)
  "eingaben": [{"pfad": "eingang.text"}, {"pfad": "eingang.anhaenge"}],
  "prompt": "Ordne diese Eingabe ein: {{ eingang.text }}",   // optional: überschreibt den Nutzer-Prompt der Vorlage
  "ergebnis": "einordnung"
}
```

Die Activity `agent_fragen` v2 bekommt: Vorlage, gefüllten Prompt, Anhang-IDs.
Sie lädt Anhänge, baut den Aufruf (System-Prompt mit Prompt-Caching, Dokument-
und Bildblöcke, strukturierte Ausgabe nach dem Schema der Vorlage, Server-Tools
wie Websuche, wenn die Vorlage es erlaubt) und legt das Ergebnis als Dict unter
`ergebnis` ab. Das Ergebnis ist genau das JSON des Schemas; damit können
nachfolgende Bedingungen auf `einordnung.prozess` oder `schnitt.stories.0.titel`
zugreifen.

**`rueckfrage`** (generisches Signal mit Formular)

```jsonc
{
  "typ": "rueckfrage",
  "titel": "Passt der Schnitt?",
  "fragen": [
    {"name": "freigabe", "art": "auswahl", "optionen": ["ja", "ändern", "verwerfen"]},
    {"name": "hinweis", "art": "text", "optional": true}
  ],
  "vorschlag": "{{ schnitt.zusammenfassung }}",   // wird im Dashboard über dem Formular angezeigt
  "frist": 86400, "ergebnis": "antwort"
}
```

Technisch ist das der vorhandene Signal-Knoten mit dynamischem Namen
(`rueckfrage_<knoten_id>`) und einem Formular, das das Dashboard aus `fragen`
rendert. Der Fristmechanismus mit Erinnerung bleibt. Das ersetzt die fest
kodierten Signale `gutachten_eingegangen` und `entgeltmeldung_eingegangen`
langfristig; die bleiben vorerst für die Demo.

**`unterprozess`**

```jsonc
{"typ": "unterprozess", "prozess": {"pfad": "einordnung.prozess"},   // oder {"wert": "po.recherche"}
 "version": null,                                                     // null = aktive Version
 "warten": true, "ergebnis": "unter"}
```

Temporal `execute_child_workflow("Prozess", ...)` mit der aktiven Graph-Version
des Zielprozesses, geladen in einer Activity (der Workflow selbst liest keine
DB). Der Untervorgang bekommt `uebergeordneter_vorgang_id`, sein Kontext startet
mit dem Kontext des Elternknotens. `warten: false` startet und geht weiter.
Das ist der Knoten, der aus dem Designer einen Router macht.

**`artefakt`**

```jsonc
{"typ": "artefakt", "art": "story-schnitt", "titel": "{{ schnitt.epic.titel }}",
 "inhalt": {"pfad": "schnitt"}, "darstellung": "markdown-vorlage:story-schnitt"}
```

Schreibt ein versioniertes Artefakt an den Vorgang. Das Dashboard zeigt
Artefakte als Dokument, nicht als Verlaufszeile. Export (Markdown, DOCX)
hängt am Artefakt.

**`http`** (generischer Konnektor)

```jsonc
{"typ": "http", "methode": "POST", "url": "http://n8n:5678/webhook/jira-anlegen",
 "body": {"pfad": "schnitt"}, "ergebnis": "jira"}
```

Eine Activity, die einen HTTP-Aufruf macht. Damit werden Konnektoren zur
Frage „welcher n8n-Flow", nicht „welcher Python-Code". Siehe 3.6.

### 3.5 Agentenvorlagen

Eine Vorlage ist eine Datei im Repo (`agenten/vorlagen/po.einordnen.yaml` oder
Python-Modul), mit:

- System-Prompt (wird gecacht, ändert sich selten),
- Ausgabeschema (Pydantic-Modell, daraus strukturierte Ausgabe),
- Modell und Effort (Standard `claude-opus-5`, adaptives Thinking; Effort
  `low` für Einordnung, `high` für Schnitt und Recherche),
- erlaubte Tools (z. B. Websuche für `po.recherche`),
- ob Anhänge mitgegeben werden,
- Testfälle (Eingabe → erwartete Felder), ausführbar mit `pytest`.

Der Designer zeigt Vorlagen in der Palette und lässt den Nutzer-Prompt und
das `ergebnis` je Knoten anpassen. Wer eine Vorlage ändern will, ändert die
Datei und committet. Warum nicht im Designer: Abschnitt 9, Punkt 1.

Vorlagen für den Start:

| Vorlage | Ausgabe | Effort | Tools |
|---|---|---|---|
| `po.einordnen` | `{absicht, prozess, konfidenz, zusammenfassung, rueckfragen[]}`; Prozessliste wird aus den aktiven Prozessen in den Prompt eingesetzt | low | keine |
| `po.bild-lesen` | `{beschreibung, sichtbarer_text, ui_elemente[]}` | low | keine |
| `po.story-schnitt` | `{epic:{titel,ziel,nutzen}, features:[{titel, stories:[{titel, als, moechte, damit, akzeptanzkriterien[], groesse}]}], annahmen[], offene_fragen[]}` | high | keine |
| `po.schnitt-kritik` | `{befunde:[{story, problem, vorschlag}], invest_verstoesse[], freigabe_empfohlen}` | medium | keine |
| `po.recherche` | `{zusammenfassung, befunde:[{aussage, quelle, verlaesslichkeit}], offene_punkte[]}` | high | Websuche, Web-Abruf |
| `po.status` | `{antwort, verweise[]}` | low | keine |

Kontext, der jeder PO-Vorlage in den System-Prompt kommt: eine **Produktakte**
je Produkt (Markdown: Zweck, Nutzer, Systeme, Konventionen für Stories,
Definition of Ready, Glossar). Ohne die Akte schneidet Claude generische
Stories. Die Akte ist eine Tabelle `produkt` mit einem Markdown-Feld, gepflegt
im Dashboard, und über Prompt-Caching billig.

### 3.6 Konnektoren: Temporal orchestriert, n8n verbindet

n8n läuft schon im Compose. Es hat einige hundert Konnektoren, dieses Repo
keinen. Vorschlag: **Konnektoren sind n8n-Flows mit Webhook-Trigger, der
Temporal-Graph ruft sie über den `http`-Knoten.** Jira anlegen, Confluence-
Seite schreiben, Mail senden, Slack posten: je ein n8n-Flow, importierbar
aus `doku/`.

Preis dafür: zwei Designer. Regel, damit das nicht ausfranst: **In n8n liegt
keine Fachlogik und keine Wartezeit.** Ein n8n-Flow bekommt fertige Daten,
schreibt sie in ein System und antwortet synchron. Alles, was entscheidet,
wartet oder Claude fragt, liegt im Temporal-Graphen. Wenn ein n8n-Flow länger
als ein paar Sekunden braucht, ist er falsch geschnitten.

Alternative: Konnektoren als Python-Activities in diesem Repo. Sauberer, aber
jeder Konnektor ist ein Nachmittag plus Pflege. Für Jira allein wäre das
vertretbar; sobald Confluence und Mail dazukommen, gewinnt n8n.

Umgekehrt kann n8n auch **Eingänge** liefern: der vorhandene IMAP-Trigger
postet die Mail an `POST /api/eingang`. Damit ist „Mail kommt" derselbe Weg
wie „Prompt getippt".

### 3.7 Dashboard

Das heutige Dashboard ist ein Unfallformular plus Fallliste. Umbau:

- **Eingabefeld** oben, groß, mit Mikro, Datei, Einfügen. Darunter die
  Rückmeldung des Routers als Chips („Story-Schnitt · Recherche · Status ·
  Anderes"), sobald der Prozess „Eingang" an der Rückfrage steht.
- **Vorgangsliste** rechts, wie heute die Fallliste, gruppiert nach Prozess
  und Status; wartende Rückfragen oben.
- **Vorgangsdetail**: Artefakte als Dokument, Rückfrage-Formular, Verlauf,
  Live-Graph (gibt es schon).
- Unfallformular wandert in einen Reiter „Demo".

Kein Framework, kein Build-Schritt, wie bisher. Das trägt noch eine Weile.

## 4. Die ersten Prozesse

### 4.1 `Eingang` (Router)

```
start
 → agent  po.bild-lesen         (nur wenn eingang.anhaenge nicht leer; sonst Bedingung überspringt)
 → agent  po.einordnen          → einordnung
 → bedingung  einordnung.konfidenz >= 0.8
     ja   → unterprozess {{einordnung.prozess}} (warten) → ende
     nein → rueckfrage "Ich lese das als {{einordnung.zusammenfassung}}. Welcher Prozess?"
              (Auswahl: Vorschläge aus einordnung, plus "Anderes")
           → unterprozess {{antwort.prozess}} → ende
```

Die Konfidenzschwelle ist eine Zahl im Graphen; wer Vollautomatik will, setzt
sie auf 0. Jede Korrektur an der Rückfrage wird gespeichert (Vorgang enthält
`einordnung` und `antwort`), also lässt sich später messen, wie oft der
Router danebenliegt.

### 4.2 `po.story-schnitt`

```
start
 → agent  po.story-schnitt   (Eingang + Produktakte + Anhänge)      → schnitt
 → agent  po.schnitt-kritik  (schnitt)                              → kritik
 → bedingung kritik.freigabe_empfohlen == true
     nein → agent po.story-schnitt (Prompt: "Überarbeite nach: {{kritik.befunde}}") → schnitt
 → artefakt "Story-Schnitt"  (schnitt, Markdown-Vorlage)
 → rueckfrage "Passt der Schnitt?"  [ja | ändern | verwerfen] + Hinweis     → antwort
 → bedingung antwort.freigabe == "ändern"
     ja → agent po.story-schnitt (Prompt: "Überarbeite nach: {{antwort.hinweis}}") → zurück zur Kritik
 → bedingung antwort.freigabe == "ja"
     ja → http  n8n "jira-anlegen" (schnitt)   → jira
        → artefakt "Jira-Verweise" (jira)
 → ende
```

Die Schleife „Kritik → Überarbeiten" ist eine Schleife im Graphen. Der
Interpreter erlaubt sie heute mit Warnung und 200-Schritte-Limit. Für den
Anfang reicht das; ein Zähler im Kontext (`runde`) plus Bedingung `runde < 3`
ist eine Notiz-Erweiterung, kein neuer Knotentyp.

### 4.3 `po.recherche`

```
start → agent po.recherche (Websuche erlaubt) → recherche
      → agent po.kritik-recherche ("Welche Aussagen sind unbelegt?") → kritik
      → artefakt "Recherche-Memo" (recherche + kritik) → ende
```

### 4.4 `po.status-frage`

```
start → agent po.status (Eingang + Verlauf und Artefakte der letzten N Vorgänge) → artefakt "Antwort" → ende
```

Die Historie kommt über eine Activity `vorgaenge_suchen` (Volltext in
Postgres), die der Agent-Knoten als Eingabe bekommt. Kein Vektorindex, solange
ein Nutzer ein paar hundert Vorgänge hat.

## 5. Ablauf eines Prompts, Ende zu Ende

Eingabe: Screenshot aus Teams plus „Stories dafür bitte".

1. Dashboard `POST /api/eingang` (Text + PNG). Vorgang V-0042 angelegt,
   Anhang gespeichert, Event auf `eingang`.
2. Consumer liest, startet Workflow `Prozess` mit Graph „Eingang" v3.
3. `po.bild-lesen` beschreibt den Screenshot (Chat, Fehlermeldung, Absender).
4. `po.einordnen`: `{prozess: "po.story-schnitt", konfidenz: 0.91, …}`.
5. Bedingung ja → Unterprozess `po.story-schnitt` v7, Vorgang V-0042/1.
6. Schnitt, Kritik, Artefakt. Rückfrage: das Dashboard zeigt den Schnitt als
   Dokument und drei Knöpfe.
7. Nutzer klickt „ändern", schreibt „Login-Story raus, die gibt es schon".
8. Überarbeitung, Kritik, neues Artefakt v2, Rückfrage. Nutzer: „ja".
9. `http` → n8n → Jira-Stub → drei Tickets. Artefakt „Jira-Verweise".
10. Unterprozess endet, Router endet. V-0042 steht auf „Abgeschlossen", im
    Detail: Screenshot, Einordnung, Schnitt v1 und v2, Kritik, Jira-Keys,
    Live-Graph beider Prozesse.

Zwischen 6 und 7 können Tage liegen. Der Workflow wartet, Temporal hält den
Zustand, nach Frist kommt eine Erinnerung. Das ist der Teil, für den die
Plattform gebaut wurde.

## 6. Arbeitspakete

Größen als T-Shirt, relativ zueinander, nicht in Tagen. Reihenfolge ist
Abhängigkeitsreihenfolge.

| # | Paket | Größe | Inhalt |
|---|---|---|---|
| 1 | Vorgang statt Fall | M | Tabellen `vorgang`, `artefakt`, `anhang`; Consumer und Interpreter auf generische Nutzlast; Unfalldemo als ein Prozess |
| 2 | Mehrere Prozesse | S | Prozessauswahl im Designer, Trigger-Zuordnung, Pfadvorschläge aus Katalog |
| 3 | Eingang | M | `POST /api/eingang`, Topic `eingang`, Anhänge ablegen, DOCX/XLSX-Extrakt; Dashboard-Eingabefeld |
| 4 | Knoten `unterprozess`, `rueckfrage`, `artefakt` | M | Interpreter, Katalog, Validierung, Designer-Panels, Dashboard-Formular |
| 5 | Agent v2 + Vorlagen | M | Vorlagenformat, Activity mit Anhängen/Schema/Tools/Caching, sechs Vorlagen, Tests |
| 6 | Knoten `http` + n8n-Flow Jira | S | Activity, Flow-Export in `doku/` |
| 7 | Prozesse Eingang, Story-Schnitt, Status | S | Graphen als Standardversionen, Produktakte |
| 8 | Recherche mit Websuche | S | Vorlage mit Server-Tool, Prozess |
| 9 | STT-Container | M | whisper.cpp-Worker, Mikro im Dashboard, Transkript-Bestätigung |
| 10 | Mail-Eingang über n8n | S | IMAP-Trigger → `/api/eingang` |

Pakete 1–7 sind das MVP: Text und Screenshot rein, Stories in Jira raus.
Erfolgskriterium: zwei Wochen echte Nutzung, ohne dass du Stories danach
noch von Hand umschreibst.

Nicht im MVP, bewusst: Parallelität im Graphen, Rechte und Mehrbenutzer,
Undo im Designer, Budget- und Portfolio-Prozesse (siehe 9.6).

## 7. Technik

Bleibt, was da ist: Python, FastAPI, temporalio, aiokafka, asyncpg, Anthropic
SDK, Vanilla-JS-Frontend, Docker Compose, n8n. Neu: whisper.cpp-Container,
`python-docx`/`openpyxl` für Extrakte, `mammoth` gibt es für Python nicht,
`python-docx` reicht.

Claude-Aufrufe: `claude-opus-5`, adaptives Thinking, Effort je Vorlage,
strukturierte Ausgabe über `output_config.format` aus dem Pydantic-Schema,
Dokument- und Bildblöcke für Anhänge, System-Prompt mit `cache_control`.
Websuche als Server-Tool `web_search_20260209`. Streaming braucht der
Activity-Aufruf nicht; die Activity läuft im Hintergrund und darf Minuten
dauern, `start_to_close_timeout` entsprechend hochsetzen (heute 120 s).

Kosten grob: Router-Aufruf mit Effort low deutlich unter einem Cent; ein
Story-Schnitt mit Kritik und einer Überarbeitung wenige Cent bis einige
zehn Cent, je nach Anhängen; Recherche mit Websuche am teuersten, im
Bereich eines Dollars. Bei einem Nutzer irrelevant.

## 8. Repository

Empfehlung: **kein neues Repository.** Dieses Projekt wird generalisiert.
Gründe:

- Interpreter, Designer, Versionierung, Kafka-Eingang, Verlauf: alles ist
  da und funktioniert. Neu bauen heißt 3.000 Zeilen kopieren und zwei
  Engines pflegen.
- Die Unfalldemo bleibt als Prozess erhalten und dient als Regressionstest.
- Was du „zusammenführen" nennst, ist genau das: der Prozessdesigner ist die
  Plattform, der PO-Assistent ist ihre erste ernsthafte Domäne.

Umbenennen (`temporal_test` → z. B. `prozessplattform`) ist kosmetisch und
jederzeit möglich; GitHub leitet die alte Adresse um. Wenn du trotzdem ein
separates Repo willst, geht das nur sinnvoll als Fork mit dem Unfallteil
entfernt; dann verlierst du die Demo und gewinnst nichts.

Das Konzept v0.1 (eigene Next.js-App) ist damit hinfällig. Was daraus
weiterlebt: Produktakte als Kontext, Kritik-Schritt nach jedem schreibenden
Agenten, Rückfragen als Formular statt Freitext, und die Warnung vor
Budgetzahlen ohne Historie.

## 9. Risiken und Widerspruch

1. **„Alles im Designer" hat eine Grenze, und die liegt beim Prompt.** Ein
   Prompt in einem Textfeld im Browser, gespeichert als String im Graph-JSON,
   ohne Test, ohne Diff, ohne Review, ist die Stelle, an der die Qualität
   still verrottet. Der Graph beschreibt *Ablauf*; die Intelligenz sitzt in
   Prompt und Schema. Darum Agentenvorlagen im Repo mit Tests, und der
   Designer wählt nur aus. Wenn du das anders willst, dann bitte mit dem
   Wissen, dass Prompt-Änderungen dann nur über Graph-Versionen
   nachvollziehbar sind und niemand sie testet.

2. **Der Router ist der am wenigsten wertvolle Teil, und trotzdem kommt er
   zuerst.** Nicht weil er wichtig ist, sondern weil er billig ist: ein
   Prozess mit drei Knoten, sobald `unterprozess` existiert. Wert entsteht in
   den Fachprozessen. Wenn der Router nervt, Schwelle auf 0 oder Chips ohne
   Agent. Beides ist eine Graphänderung.

3. **Zwei Designer (Temporal, n8n) sind ein echtes Risiko.** Ohne die Regel
   aus 3.6 landet Logik in n8n, weil es dort schneller geht, und in drei
   Monaten weiß niemand mehr, wo ein Vorgang gerade steht. Die Regel muss
   in der README stehen und im Code-Review gelten.

4. **Kontext in der Temporal-Historie.** Alles, was ein Knoten als `ergebnis`
   ablegt, liegt in der Workflow-Historie. Ein Agent, der 40 Stories mit
   Akzeptanzkriterien liefert, ist noch klein. Ein Recherche-Ergebnis mit
   zwanzig Seiten Quelltext ist es nicht. Regel: große Ergebnisse als
   Artefakt in Postgres, im Kontext nur die ID plus Zusammenfassung. Die
   `artefakt`-Activity muss das von sich aus tun.

5. **Determinismus.** Der Interpreter läuft in Temporals deterministischer
   Sandbox. Jeder neue Knotentyp muss reine Logik im Workflow und alles
   andere in Activities halten. `unterprozess` ist sauber (Child-Workflow),
   `rueckfrage` ist ein Signal, `http` und `artefakt` sind Activities. Die
   Versuchung wird sein, „mal eben" im Workflow etwas nachzuladen. Nicht.

6. **Budget und Portfolio fehlen bewusst.** Du hattest sie in der ersten
   Beschreibung, jetzt nicht mehr. Ich lasse sie draußen, bis es
   Historiendaten gibt; ohne die sind Schätzungen aus dem Modell plausibel
   klingender Unsinn. Wenn sie zurückkommen sollen, sind sie ein weiterer
   Prozess mit einer Vorlage, die nur aus Rate-Cards und Vergleichsfällen
   rechnen darf. Die Plattform ändert sich dafür nicht.

7. **Datenschutz.** Screenshots aus Teams, Kundenmails, Backlog-Inhalte gehen
   an die Anthropic-API. Vor der ersten echten Nutzung im Job klären, ob das
   unter den Standardbedingungen (30 Tage Aufbewahrung) erlaubt ist, oder ob
   Enterprise/Zero-Data-Retention oder Bedrock/Vertex in der EU nötig sind.
   Zusätzlich: das Repo ist öffentlich. Produktakten und Vorlagen mit
   Firmenwissen dürfen dann nicht hinein; die gehören in eine private
   Konfiguration oder das Repo wird privat.

8. **Sprachmemos** bleiben ein Feature mit hohem Aufwand und Prüfpflicht des
   Transkripts. Paket 9, nicht früher.

9. **Public Repo, Prompt-Injection.** Sobald Mail-Eingang läuft, verarbeitet
   der Router Text, den Fremde geschrieben haben. Ein Agent, der Jira
   schreiben darf, braucht die Rückfrage davor als harte Schranke; keine
   Vollautomatik für Prozesse mit Nebenwirkungen, egal wie hoch die
   Konfidenz ist. Das ist eine Graph-Regel, die in der Validierung
   erzwungen werden sollte: `http` ohne vorherige `rueckfrage` im selben
   Prozess ist ein Fehler, keine Warnung.

## 10. Offene Fragen

1. **Jira:** echtes Jira (Cloud oder Server), oder vorerst nur der Stub? Gibt
   es ein Story-Template und eine Definition of Ready, die in die Produktakte
   gehören?
2. **Produkte:** eines oder mehrere? Bestimmt, ob die Produktakte eine Datei
   oder eine Tabelle ist.
3. **Nutzerkreis:** nur du? Sobald ein Kollege Rückfragen beantworten soll,
   braucht es mindestens einen Namen am Vorgang und Rechte am Designer.
4. **Repo:** generalisieren wie in Abschnitt 8, ja oder nein? Umbenennen?
   Privat stellen?
5. **Unfalldemo:** behalten als Prozess (Empfehlung) oder rauswerfen?
6. **Datenschutz:** Antwort auf 9.7, bevor echte Inhalte durchlaufen.
7. **Erster Fachprozess:** Story-Schnitt (Vorschlag) oder Recherche?

---

*Nächster Schritt nach deiner Rückmeldung: Paket 1 und 2 (Vorgang, mehrere
Prozesse) als Branch mit Tests, danach Paket 4 (die drei Knoten), weil daran
alles Weitere hängt.*
