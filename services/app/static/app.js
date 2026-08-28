/* Dashboard-Logik: Formular abschicken + Fälle im Sekundentakt abholen. */

const SCHRITTE = ["Eingang", "MdE-Ermittlung", "JAV-Ermittlung", "Rentenberechnung", "Ergebnis"];

const STATUS_PILL = {
  EINGEGANGEN:      ["pill-blau",  "Eingegangen"],
  IN_BEARBEITUNG:   ["pill-blau",  "In Bearbeitung"],
  MDE_ERMITTLUNG:   ["pill-gelb",  "MdE wird ermittelt"],
  JAV_ERMITTLUNG:   ["pill-gelb",  "JAV wird ermittelt"],
  RENTENBERECHNUNG: ["pill-gelb",  "Rente wird berechnet"],
  ABGESCHLOSSEN:    ["pill-gruen", "Abgeschlossen"],
  FEHLER:           ["pill-rot",   "Fehler"],
};

/* wie weit ist der Fall? -> Index des gerade laufenden Schritts */
const FORTSCHRITT = {
  EINGEGANGEN: 1, IN_BEARBEITUNG: 1,
  MDE_ERMITTLUNG: 1, JAV_ERMITTLUNG: 2, RENTENBERECHNUNG: 3, ABGESCHLOSSEN: 5,
};

let offenerFall = null;   // ID des im Dialog geöffneten Falls

const eur = (n) => n == null ? "–" :
  Number(n).toLocaleString("de-DE", { minimumFractionDigits: 2, maximumFractionDigits: 2 });
const uhr = (iso) => iso ? new Date(iso).toLocaleTimeString("de-DE", { hour12: false }) : "";
const esc = (s) => String(s ?? "").replace(/[&<>"']/g,
  (c) => ({ "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;" }[c]));

/* ---------------- Start ---------------- */
async function start() {
  document.querySelector('input[name="unfall_datum"]').valueAsDate = new Date();

  const stamm = await (await fetch("/api/stammdaten")).json();
  fuelleSelect("sel-beruf", stamm.berufe, "Bauarbeiter");
  fuelleSelect("sel-koerperteil", stamm.koerperteile, "Bein");
  fuelleSelect("sel-schwere", stamm.schweregrade, "mittel");
  document.getElementById("topic-name").textContent = stamm.kafka_topic;
  // Den Link zur Temporal-Oberflaeche aus der Adresse bauen, unter der dieses
  // Dashboard gerade laeuft. So stimmt er auch beim Zugriff ueber die VM-IP.
  document.getElementById("temporal-link").href = stamm.temporal_ui
    || `${location.protocol}//${location.hostname}:${stamm.temporal_ui_port}`;

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

/* ---------------- Formular ---------------- */
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
      fetch("/api/faelle").then((r) => r.json()),
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
  // Beim Hochfahren einzeln anzeigen, worauf noch gewartet wird.
  const teile = [["DB", h.datenbank], ["Kafka", h.kafka], ["Temporal", h.temporal]];
  const ok = teile.every(([, bereit]) => bereit);
  el.className = "pill " + (ok ? "pill-gruen" : "pill-gelb");
  el.textContent = teile.map(([name, bereit]) => `${name} ${bereit ? "✓" : "…"}`).join("  ");
  el.title = ok ? "Alle Dienste verbunden"
                : "… bedeutet: dieser Dienst startet noch oder ist nicht erreichbar";
}

function zeichneFaelle(faelle) {
  document.getElementById("leer-hinweis").hidden = faelle.length > 0;
  document.getElementById("faelle-liste").innerHTML = faelle.map(fallKarte).join("");
  document.querySelectorAll(".fall").forEach((el) => {
    el.onclick = () => dialogOeffnen(el.dataset.id);
  });
}

function fallKarte(f) {
  const [klasse, text] = STATUS_PILL[f.status] || ["pill-grau", f.status];
  const bis = FORTSCHRITT[f.status] ?? 0;
  const laeuft = f.status !== "ABGESCHLOSSEN";

  const pipeline = SCHRITTE.map((name, i) => {
    let zustand = "";
    if (i < bis) zustand = "fertig";
    else if (i === bis && laeuft) zustand = "aktiv";
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
    <div class="werte">
      <div class="wert">MdE<b>${f.mde_prozent != null ? f.mde_prozent + " %" : "…"}</b></div>
      <div class="wert">JAV<b>${f.jav_euro != null ? eur(f.jav_euro) + " €" : "…"}</b></div>
      <div class="wert ergebnis">Rente / Monat<b>${f.rente_monat != null ? eur(f.rente_monat) + " €" : "…"}</b></div>
    </div>
  </article>`;
}

/* ---------------- Detaildialog ---------------- */
function dialogOeffnen(id) {
  offenerFall = id;
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
  if (offenerFall !== id) return;   // Dialog wurde zwischenzeitlich geschlossen
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

    <h3>Ergebnis</h3>
    <div class="stammdaten">
      <div><span>MdE</span>${f.mde_prozent != null ? f.mde_prozent + " %" : "noch offen"}</div>
      <div><span>JAV</span>${f.jav_euro != null ? eur(f.jav_euro) + " €" : "noch offen"}</div>
      <div><span>Rente / Jahr</span>${f.rente_jahr != null ? eur(f.rente_jahr) + " €" : "noch offen"}</div>
      <div><span>Rente / Monat</span>${f.rente_monat != null ? eur(f.rente_monat) + " €" : "noch offen"}</div>
    </div>
    ${f.rente_formel ? `<h3>Rechenweg (deterministisch)</h3>
      <div class="formelbox">${esc(f.rente_formel)}</div>` : ""}

    <h3>Temporal-Workflow</h3>
    ${workflowBlock(wf, f)}

    <h3>Verarbeitungsverlauf</h3>
    <div class="zeitstrahl">${(f.verlauf || []).map(verlaufEintrag).join("")}</div>`;
}

function workflowBlock(wf, f) {
  if (!wf || !wf.verfuegbar) {
    return `<p class="hilfe">${esc(wf?.grund || "Workflow noch nicht gestartet")}</p>`;
  }
  const z = wf.zustand || {};
  return `<div class="stammdaten">
      <div><span>Workflow-ID</span>${esc(wf.workflow_id)}</div>
      <div><span>Typ</span>${esc(wf.workflow_typ)}</div>
      <div><span>Task-Queue</span>${esc(wf.task_queue)}</div>
      <div><span>Status</span>${esc(wf.status)}</div>
      <div><span>Aktueller Schritt</span>${esc(z.aktueller_schritt || "–")}</div>
      <div><span>Erledigte Schritte</span>${esc((z.erledigte_schritte || []).length)} / 3</div>
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
    ${Object.keys(d).length ? `<details class="eintrag-details">
        <summary>Rohdaten anzeigen</summary>
        <pre>${esc(JSON.stringify(d, null, 2))}</pre></details>` : ""}
  </div>`;
}

start();
