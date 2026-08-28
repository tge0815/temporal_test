/* Dashboard-Logik: Formular abschicken, Fälle im Sekundentakt abholen,
   und bei wartenden Fällen die nötige Aktion anbieten. */

const SCHRITTE = ["Eingang", "Gutachten", "MdE", "Entgeltmeldung", "JAV", "Rente", "Ergebnis"];

const STATUS_PILL = {
  EINGEGANGEN:              ["pill-blau",  "Eingegangen"],
  IN_BEARBEITUNG:           ["pill-blau",  "In Bearbeitung"],
  WARTE_AUF_GUTACHTEN:      ["pill-lila",  "Wartet auf Gutachten"],
  MDE_ERMITTLUNG:           ["pill-gelb",  "MdE wird ermittelt"],
  WARTE_AUF_ENTGELTMELDUNG: ["pill-lila",  "Wartet auf Entgeltmeldung"],
  JAV_ERMITTLUNG:           ["pill-gelb",  "JAV wird ermittelt"],
  RENTENBERECHNUNG:         ["pill-gelb",  "Rente wird berechnet"],
  ABGESCHLOSSEN:            ["pill-gruen", "Abgeschlossen"],
  FEHLER:                   ["pill-rot",   "Fehler"],
};

/* Index der Stufe, die gerade läuft bzw. auf die gewartet wird */
const FORTSCHRITT = {
  EINGEGANGEN: 1, IN_BEARBEITUNG: 1, WARTE_AUF_GUTACHTEN: 1,
  MDE_ERMITTLUNG: 2, WARTE_AUF_ENTGELTMELDUNG: 3, JAV_ERMITTLUNG: 4,
  RENTENBERECHNUNG: 5, ABGESCHLOSSEN: 7,
};

/* Bei diesen Status wartet der Workflow auf uns */
const WARTET = ["WARTE_AUF_GUTACHTEN", "WARTE_AUF_ENTGELTMELDUNG"];

let offenerFall = null;      // ID des im Dialog geöffneten Falls
let listenSignatur = "";     // verhindert unnötiges Neuzeichnen
let dialogSignatur = "";
let stammdaten = {};

const eur = (n) => n == null ? "–" :
  Number(n).toLocaleString("de-DE", { minimumFractionDigits: 2, maximumFractionDigits: 2 });
const uhr = (iso) => iso ? new Date(iso).toLocaleTimeString("de-DE", { hour12: false }) : "";
const esc = (s) => String(s ?? "").replace(/[&<>"']/g,
  (c) => ({ "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;" }[c]));

/* ---------------- Start ---------------- */
async function start() {
  document.querySelector('input[name="unfall_datum"]').valueAsDate = new Date();

  stammdaten = await (await fetch("/api/stammdaten")).json();
  fuelleSelect("sel-beruf", stammdaten.berufe, "Bauarbeiter");
  fuelleSelect("sel-koerperteil", stammdaten.koerperteile, "Bein");
  fuelleSelect("sel-schwere", stammdaten.schweregrade, "mittel");
  document.getElementById("topic-name").textContent = stammdaten.kafka_topic;
  document.getElementById("temporal-link").href = stammdaten.temporal_ui
    || `${location.protocol}//${location.hostname}:${stammdaten.temporal_ui_port}`;
  zeigeExtraktionsHinweis();

  document.getElementById("unfall-form").addEventListener("submit", absenden);
  document.getElementById("dialog-zu").addEventListener("click", dialogSchliessen);
  document.getElementById("overlay").addEventListener("click", (e) => {
    if (e.target.id === "overlay") dialogSchliessen();
  });
  document.addEventListener("keydown", (e) => { if (e.key === "Escape") dialogSchliessen(); });

  tick();
  setInterval(tick, 1000);
}

function fuelleSelect(id, werte, vorauswahl) {
  const el = document.getElementById(id);
  el.innerHTML = werte.map((w) => `<option${w === vorauswahl ? " selected" : ""}>${esc(w)}</option>`).join("");
}

function zeigeExtraktionsHinweis() {
  const el = document.getElementById("extraktion-hinweis");
  if (!el) return;
  el.innerHTML = stammdaten.claude_aktiv
    ? `Der <b>mde-agent</b> liest das Gutachten mit <code>${esc(stammdaten.claude_modell)}</code>.`
    : `Der <b>mde-agent</b> liest das Gutachten per Textsuche.
       Für eine inhaltliche Auswertung durch Claude einen
       <code>ANTHROPIC_API_KEY</code> in der <code>.env</code> hinterlegen.`;
}

/* ---------------- Unfall melden ---------------- */
async function absenden(ereignis) {
  ereignis.preventDefault();
  const knopf = document.getElementById("absenden");
  const meldung = document.getElementById("form-meldung");
  const daten = Object.fromEntries(new FormData(ereignis.target).entries());
  daten.geburtsjahr = Number(daten.geburtsjahr);

  knopf.disabled = true;
  meldung.className = "meldung";
  meldung.textContent = "Sende Event an Kafka …";
  try {
    const antwort = await fetch("/api/unfaelle", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(daten),
    });
    if (!antwort.ok) throw new Error(await antwort.text());
    const e = await antwort.json();
    meldung.className = "meldung ok";
    meldung.textContent = `✓ ${e.fall_nummer} – Kafka-Event geschrieben `
      + `(Topic ${e.kafka.topic}, Partition ${e.kafka.partition}, Offset ${e.kafka.offset})`;
    tick();
  } catch (fehler) {
    meldung.className = "meldung fehler";
    meldung.textContent = "Fehler: " + fehler.message;
  } finally {
    knopf.disabled = false;
  }
}

/* ---------------- Polling ---------------- */
async function tick() {
  try {
    const [faelle, health] = await Promise.all([
      fetch("/api/faelle").then((r) => r.ok ? r.json() : []),
      fetch("/api/health").then((r) => r.json()).catch(() => null),
    ]);
    zeigeHealth(health);
    zeichneFaelle(faelle);
    if (offenerFall) zeichneDialog(offenerFall);
  } catch (e) {
    zeigeHealth(null);
  }
}

function zeigeHealth(h) {
  const el = document.getElementById("health");
  if (!h) { el.className = "pill pill-rot"; el.textContent = "API nicht erreichbar"; return; }
  const teile = [["DB", h.datenbank], ["Kafka", h.kafka], ["Temporal", h.temporal]];
  const ok = teile.every(([, bereit]) => bereit);
  el.className = "pill " + (ok ? "pill-gruen" : "pill-gelb");
  el.textContent = teile.map(([name, bereit]) => `${name} ${bereit ? "✓" : "…"}`).join("  ");
  el.title = ok ? "Alle Dienste verbunden"
                : "… bedeutet: dieser Dienst startet noch oder ist nicht erreichbar";
}

function zeichneFaelle(faelle) {
  document.getElementById("leer-hinweis").hidden = faelle.length > 0;
  // Nur neu zeichnen, wenn sich wirklich etwas geändert hat – sonst würde
  // ein gerade getipptes Eingabefeld jede Sekunde geleert.
  const signatur = JSON.stringify(faelle);
  if (signatur === listenSignatur) return;
  listenSignatur = signatur;

  document.getElementById("faelle-liste").innerHTML = faelle.map(fallKarte).join("");
  document.querySelectorAll(".fall").forEach((el) => {
    el.onclick = (e) => { if (!e.target.closest(".aktion")) dialogOeffnen(el.dataset.id); };
  });
  faelle.filter((f) => WARTET.includes(f.status)).forEach(aktionVerdrahten);
}

function fallKarte(f) {
  const [klasse, text] = STATUS_PILL[f.status] || ["pill-grau", f.status];
  const bis = FORTSCHRITT[f.status] ?? 0;
  const laeuft = f.status !== "ABGESCHLOSSEN";

  const pipeline = SCHRITTE.map((name, i) => {
    let zustand = "";
    if (i < bis) zustand = "fertig";
    else if (i === bis && laeuft) zustand = WARTET.includes(f.status) ? "wartet" : "aktiv";
    return `<div class="stufe ${zustand}"><div class="balken"></div><span class="name">${name}</span></div>`;
  }).join("");

  return `
  <article class="fall" data-id="${f.id}">
    <div class="fall-kopf">
      <div>
        <div class="fall-titel">${esc(f.fall_nummer)} · ${esc(f.versicherter)}</div>
        <div class="fall-meta">${esc(f.beruf)} · ${esc(f.koerperteil)} · ${esc(f.schwere)}
          · gemeldet ${uhr(f.erstellt_am)}</div>
      </div>
      <span class="pill ${klasse}">${text}</span>
    </div>
    <div class="pipeline">${pipeline}</div>
    ${aktionsBereich(f)}
    <div class="werte">
      <div class="wert">MdE<b>${f.mde_prozent != null ? f.mde_prozent + " %" : "…"}</b></div>
      <div class="wert">JAV<b>${f.jav_euro != null ? eur(f.jav_euro) + " €" : "…"}</b></div>
      <div class="wert ergebnis">Rente / Monat<b>${f.rente_monat != null ? eur(f.rente_monat) + " €" : "…"}</b></div>
    </div>
  </article>`;
}

/* ---------------- Aktionen bei wartendem Workflow ---------------- */
function aktionsBereich(f) {
  if (f.status === "WARTE_AUF_GUTACHTEN") {
    return `
    <div class="aktion" data-id="${f.id}" data-art="gutachten">
      <div class="aktion-kopf">⏳ Der Workflow wartet auf das ärztliche Gutachten</div>
      <p class="aktion-text">Temporal hält den Fall an dieser Stelle fest – beliebig lange.
        Laden Sie das Gutachten hoch (PDF oder Textdatei), dann läuft er weiter.</p>
      <div class="aktion-zeile">
        <input type="file" class="gutachten-datei" accept=".pdf,.txt,.md,application/pdf,text/plain">
        <button type="button" class="klein gutachten-senden">Gutachten einreichen</button>
      </div>
      <a class="aktion-link" href="/api/faelle/${f.id}/beispiel-gutachten"
         download>Kein Gutachten zur Hand? Beispiel-Gutachten als PDF herunterladen ↓</a>
      <div class="aktion-meldung"></div>
    </div>`;
  }
  if (f.status === "WARTE_AUF_ENTGELTMELDUNG") {
    return `
    <div class="aktion" data-id="${f.id}" data-art="entgelt">
      <div class="aktion-kopf">⏳ Der Workflow wartet auf die Entgeltmeldung</div>
      <p class="aktion-text">Unternehmer und Versicherte Person wurden angeschrieben.
        Tragen Sie das gemeldete Jahresbrutto ein – oder lassen Sie die Antwort simulieren.</p>
      <div class="aktion-zeile">
        <input type="number" class="jav-wert" placeholder="Jahresbrutto in €" min="1" step="100">
        <button type="button" class="klein jav-senden">Meldung erfassen</button>
        <button type="button" class="klein sekundaer jav-simulieren">Antwort simulieren</button>
      </div>
      <div class="aktion-meldung"></div>
    </div>`;
  }
  return "";
}

function aktionVerdrahten(f) {
  const box = document.querySelector(`.aktion[data-id="${f.id}"]`);
  if (!box) return;
  const meldung = box.querySelector(".aktion-meldung");

  const melde = (text, klasse) => {
    meldung.className = "aktion-meldung " + (klasse || "");
    meldung.textContent = text;
  };

  const senden = async (fn) => {
    box.querySelectorAll("button").forEach((b) => (b.disabled = true));
    try {
      const antwort = await fn();
      if (!antwort.ok) {
        const fehler = await antwort.json().catch(() => ({}));
        throw new Error(fehler.detail || `HTTP ${antwort.status}`);
      }
      melde("✓ Eingegangen – der Workflow läuft weiter.", "ok");
      listenSignatur = "";   // Neuzeichnen im nächsten Takt erzwingen
      tick();
    } catch (e) {
      melde("Fehler: " + e.message, "fehler");
      box.querySelectorAll("button").forEach((b) => (b.disabled = false));
    }
  };

  const knopfGutachten = box.querySelector(".gutachten-senden");
  if (knopfGutachten) {
    knopfGutachten.onclick = () => {
      const feld = box.querySelector(".gutachten-datei");
      if (!feld.files.length) { melde("Bitte zuerst eine Datei auswählen.", "fehler"); return; }
      const formular = new FormData();
      formular.append("datei", feld.files[0]);
      melde("Lade hoch …");
      senden(() => fetch(`/api/faelle/${f.id}/gutachten`, { method: "POST", body: formular }));
    };
  }

  const knopfJav = box.querySelector(".jav-senden");
  if (knopfJav) {
    knopfJav.onclick = () => {
      const wert = Number(box.querySelector(".jav-wert").value);
      if (!wert || wert <= 0) { melde("Bitte ein Jahresbrutto eintragen.", "fehler"); return; }
      melde("Sende Meldung …");
      senden(() => fetch(`/api/faelle/${f.id}/entgeltmeldung`, {
        method: "POST", headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ jav_euro: wert, quelle: "Entgeltmeldung des Unternehmers" }),
      }));
    };
    box.querySelector(".jav-simulieren").onclick = () => {
      melde("Simuliere Antwort …");
      senden(() => fetch(`/api/faelle/${f.id}/entgeltmeldung/simulieren`, { method: "POST" }));
    };
  }
}

/* ---------------- Detaildialog ---------------- */
function dialogOeffnen(id) {
  offenerFall = id;
  dialogSignatur = "";
  document.getElementById("overlay").hidden = false;
  zeichneDialog(id);
}
function dialogSchliessen() {
  offenerFall = null;
  document.getElementById("overlay").hidden = true;
}

async function zeichneDialog(id) {
  const [f, wf] = await Promise.all([
    fetch(`/api/faelle/${id}`).then((r) => r.json()),
    fetch(`/api/faelle/${id}/workflow`).then((r) => r.json()).catch(() => null),
  ]);
  if (offenerFall !== id) return;   // zwischenzeitlich geschlossen

  // Nicht jede Sekunde neu zeichnen – sonst klappen geöffnete Details zu.
  const signatur = JSON.stringify([f, wf]);
  if (signatur === dialogSignatur) return;
  dialogSignatur = signatur;

  const [klasse, text] = STATUS_PILL[f.status] || ["pill-grau", f.status];

  document.getElementById("dialog-inhalt").innerHTML = `
    <h2>${esc(f.fall_nummer)} <span class="pill ${klasse}">${text}</span></h2>

    <h3>Stammdaten</h3>
    <div class="stammdaten">
      <div><span>Versicherte Person</span>${esc(f.versicherter)}</div>
      <div><span>Geburtsjahr</span>${esc(f.geburtsjahr)}</div>
      <div><span>Beruf</span>${esc(f.beruf)}</div>
      <div><span>Unfalldatum</span>${esc(f.unfall_datum)}</div>
      <div><span>Körperteil</span>${esc(f.koerperteil)}</div>
      <div><span>Schweregrad</span>${esc(f.schwere)}</div>
    </div>
    ${f.hergang ? `<p class="hilfe" style="margin-top:12px">„${esc(f.hergang)}“</p>` : ""}

    ${dokumenteBlock(f)}

    <h3>Ergebnis</h3>
    <div class="stammdaten">
      <div><span>MdE</span>${f.mde_prozent != null ? f.mde_prozent + " %" : "noch offen"}</div>
      <div><span>JAV</span>${f.jav_euro != null ? eur(f.jav_euro) + " €" : "noch offen"}</div>
      <div><span>Rente / Jahr</span>${f.rente_jahr != null ? eur(f.rente_jahr) + " €" : "noch offen"}</div>
      <div><span>Rente / Monat</span>${f.rente_monat != null ? eur(f.rente_monat) + " €" : "noch offen"}</div>
    </div>
    ${f.mde_quelle ? `<p class="hilfe" style="margin-top:10px">
       <b>MdE-Quelle:</b> ${esc(f.mde_quelle)}</p>` : ""}
    ${f.jav_gemeldet ? `<p class="hilfe">
       <b>Gemeldet waren:</b> ${eur(f.jav_gemeldet)} € (${esc(f.jav_quelle)})</p>` : ""}
    ${f.rente_formel ? `<h3>Rechenweg (deterministisch)</h3>
      <div class="formelbox">${esc(f.rente_formel)}</div>` : ""}

    <h3>Temporal-Workflow</h3>
    ${workflowBlock(wf)}

    <h3>Verarbeitungsverlauf</h3>
    <div class="zeitstrahl">${(f.verlauf || []).map(verlaufEintrag).join("")}</div>`;
}

function dokumenteBlock(f) {
  if (!f.dokumente || !f.dokumente.length) return "";
  return `<h3>Eingegangene Dokumente</h3>
    <div class="dokumente">${f.dokumente.map((d) => `
      <a class="dokument" href="/api/faelle/${f.id}/dokumente/${d.id}" target="_blank" rel="noopener">
        <span class="dok-symbol">📄</span>
        <span><b>${esc(d.dateiname)}</b><br>
          <span class="dok-meta">${esc(d.art)} · ${Math.round((d.groesse || 0) / 1024)} KB
            · ${uhr(d.hochgeladen_am)}</span></span>
      </a>`).join("")}</div>`;
}

function workflowBlock(wf) {
  if (!wf || !wf.verfuegbar) {
    return `<p class="hilfe">${esc(wf?.grund || "Workflow noch nicht gestartet")}</p>`;
  }
  const z = wf.zustand || {};
  const erinnerungen = z.erinnerungen || {};
  const anzahl = (erinnerungen.gutachten || 0) + (erinnerungen.entgeltmeldung || 0);
  return `<div class="stammdaten">
      <div><span>Workflow-ID</span>${esc(wf.workflow_id)}</div>
      <div><span>Typ</span>${esc(wf.workflow_typ)}</div>
      <div><span>Task-Queue</span>${esc(wf.task_queue)}</div>
      <div><span>Status</span>${esc(wf.status)}</div>
      <div><span>Aktueller Schritt</span>${esc(z.aktueller_schritt || "–")}</div>
      <div><span>Erledigte Schritte</span>${esc((z.erledigte_schritte || []).length)} / 5</div>
      ${z.wartet_auf ? `<div><span>Wartet auf</span>${esc(z.wartet_auf)}</div>` : ""}
      <div><span>Erinnerungen</span>${anzahl}</div>
    </div>
    <p class="hilfe" style="margin-top:10px">Diese Angaben stammen aus einer echten
      Temporal-Query gegen den laufenden Workflow.</p>`;
}

function verlaufEintrag(e) {
  const d = e.details || {};
  const konf = d.konfidenz;
  return `
  <div class="eintrag ${esc((e.status || "").toLowerCase())}">
    <div class="eintrag-kopf">
      <span class="eintrag-schritt">${esc(e.schritt)}</span>
      <span class="eintrag-komponente">${esc(e.komponente)}</span>
      <span class="eintrag-zeit">${uhr(e.zeitpunkt)}</span>
    </div>
    <div class="eintrag-text">${esc(e.nachricht)}</div>
    ${konf != null ? `<div class="konfidenz">Konfidenz
        <span class="leiste"><span class="fuellung" style="width:${Math.round(konf * 100)}%"></span></span>
        ${Math.round(konf * 100)} %</div>` : ""}
    ${d.begruendung ? `<div class="eintrag-text" style="color:var(--gedimmt)">
        Begründung: ${esc(d.begruendung)}</div>` : ""}
    ${d.fundstelle ? `<div class="fundstelle">Fundstelle im Gutachten:
        „${esc(d.fundstelle)}“</div>` : ""}
    ${Object.keys(d).length ? `<details class="eintrag-details">
        <summary>Rohdaten anzeigen</summary>
        <pre>${esc(JSON.stringify(d, null, 2))}</pre></details>` : ""}
  </div>`;
}

start();
