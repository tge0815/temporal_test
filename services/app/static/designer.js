/* Prozessdesigner: Palette, Zeichenfläche, Eigenschaften, Versionen. */

const PROZESS = "Unfallsachbearbeitung";

/* Pfade, die es im Kontext geben kann – als Vorschlagsliste für Bedingungen,
   Eingaben und Prompts. Der Kontext ist ein einfaches Dict: "fall" liegt von
   Anfang an drin, alles andere legen Knoten unter ihrem "ergebnis"-Namen ab. */
const BEKANNTE_PFADE = [
  "fall.id", "fall.beruf", "fall.schwere", "fall.koerperteil", "fall.geburtsjahr",
  "gutachten.dokument_id", "gutachten.dateiname",
  "mde.mde_prozent", "mde.konfidenz", "mde.begruendung",
  "entgeltmeldung.jav_euro", "entgeltmeldung.quelle",
  "jav.jav_euro", "jav.konfidenz",
  "rente.rente_monat_euro", "rente.rente_jahr_euro",
  "agent.antwort", "agent.einschaetzung", "agent.konfidenz",
];

const esc = (s) => String(s ?? "").replace(/[&<>"']/g,
  (c) => ({ "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;" }[c]));

let katalog = null;
let canvas = null;
let versionen = [];
let geladeneVersion = null;   // Nummer der Version, die gerade auf der Fläche liegt
let ungespeichert = false;

/* ---------------- Start ---------------- */
async function start() {
  katalog = await (await fetch("/api/katalog")).json();
  canvas = new ProzessCanvas(document.getElementById("canvas"), {
    editierbar: true,
    katalog,
    onAuswahl: zeichneEigenschaften,
    onAenderung: () => { ungespeichert = true; zeigeTitel(); },
  });
  zeichnePalette();

  document.getElementById("laden").onclick = () => versionLaden(Number(versionWahl().value));
  document.getElementById("aktivieren").onclick = aktivieren;
  document.getElementById("pruefen").onclick = () => pruefen(true);
  document.getElementById("speichern").onclick = speichern;
  document.getElementById("json-zeigen").onclick = jsonZeigen;
  document.getElementById("dialog-zu").onclick = () => (document.getElementById("overlay").hidden = true);
  document.getElementById("overlay").onclick = (e) => { if (e.target.id === "overlay") e.target.hidden = true; };
  window.addEventListener("beforeunload", (e) => { if (ungespeichert) { e.preventDefault(); e.returnValue = ""; } });

  await versionenLaden();
  const aktiv = versionen.find((v) => v.aktiv) || versionen[versionen.length - 1];
  if (aktiv) await versionLaden(aktiv.version);
  health();
  setInterval(health, 3000);
}

const versionWahl = () => document.getElementById("version-wahl");

async function versionenLaden() {
  versionen = (await (await fetch("/api/prozesse")).json()).filter((v) => v.name === PROZESS);
  versionWahl().innerHTML = versionen.map((v) =>
    `<option value="${v.version}">v${v.version}${v.aktiv ? " · aktiv" : ""}${v.kommentar ? " – " + esc(v.kommentar) : ""}</option>`
  ).join("") || `<option value="">(keine Version)</option>`;
  if (geladeneVersion) versionWahl().value = geladeneVersion;
}

async function versionLaden(nummer) {
  if (!nummer) return;
  if (ungespeichert && !confirm("Ungespeicherte Änderungen verwerfen?")) return;
  const d = await (await fetch(`/api/prozesse/${encodeURIComponent(PROZESS)}/versionen/${nummer}`)).json();
  canvas.setGraph(d.graph);
  geladeneVersion = d.version;
  ungespeichert = false;
  versionWahl().value = nummer;
  zeigeTitel();
  melde(`Version ${d.version} geladen (${d.graph.knoten.length} Knoten)${d.aktiv ? " – das ist die aktive Version" : ""}.`);
  document.getElementById("pruef-ergebnis").innerHTML = "";
}

function zeigeTitel() {
  document.title = `${ungespeichert ? "● " : ""}Prozessdesigner – v${geladeneVersion ?? "?"}`;
}

/* ---------------- Palette ---------------- */
function zeichnePalette() {
  const eintraege = [];
  for (const t of katalog.knotentypen) {
    if (t.typ === "aktivitaet") {
      for (const a of katalog.aktivitaeten) {
        eintraege.push({ typ: "aktivitaet", titel: a.titel, beschreibung: `${a.beschreibung} (Dienst: ${a.dienst})`,
          knoten: { typ: "aktivitaet", titel: a.titel.replace(/ \(.*\)$/, ""), aktivitaet: a.name } });
      }
    } else if (t.typ === "signal") {
      for (const s of katalog.signale) {
        eintraege.push({ typ: "signal", titel: s.titel, beschreibung: `Signal ${s.name}, Erinnerung nach ${s.frist} s`,
          knoten: { typ: "signal", titel: s.titel, signal: s.name } });
      }
    } else if (t.typ === "bedingung") {
      eintraege.push({ ...t, knoten: { typ: "bedingung", titel: "Bedingung",
        bedingung: { links: "mde.mde_prozent", op: ">=", rechts: 20 } } });
    } else if (t.typ === "agent") {
      eintraege.push({ ...t, beschreibung: t.beschreibung + (katalog.claude_aktiv ? "" : " Zurzeit ohne Schlüssel – antwortet 'unklar'."),
        knoten: { typ: "agent", titel: "Agent fragen", ergebnis: "agent",
          prompt: "Versicherte Person: {{fall.beruf}}, Verletzung: {{fall.koerperteil}} ({{fall.schwere}}), MdE {{mde.mde_prozent}} %. Ist eine Nachuntersuchung in zwei Jahren sinnvoll?" } });
    } else if (t.typ === "notiz") {
      eintraege.push({ ...t, knoten: { typ: "notiz", titel: "Notiz", text: "Hinweis für die Sachbearbeitung: MdE {{mde.mde_prozent}} %" } });
    } else {
      eintraege.push({ ...t, knoten: { typ: t.typ, titel: t.titel } });
    }
  }
  document.getElementById("palette-liste").innerHTML = eintraege.map((e, i) => `
    <button type="button" class="palette-eintrag typ-${e.typ}" data-i="${i}" title="${esc(e.beschreibung)}">
      <span class="pg-symbol">${{ start: "▶", aktivitaet: "⚙", signal: "⏳", bedingung: "?", agent: "✦", notiz: "✎", ende: "■" }[e.typ]}</span>
      <span><b>${esc(e.titel)}</b><br><small>${esc(e.beschreibung)}</small></span>
    </button>`).join("");
  document.querySelectorAll(".palette-eintrag").forEach((el) => {
    el.onclick = () => canvas.knotenHinzufuegen(JSON.parse(JSON.stringify(eintraege[el.dataset.i].knoten)));
  });
}

/* ---------------- Eigenschaften ---------------- */
function zeichneEigenschaften(auswahl) {
  const box = document.getElementById("eigenschaften-inhalt");
  if (!auswahl) { box.innerHTML = `<p class="hilfe">Knoten auswählen, um ihn zu bearbeiten.</p>`; return; }
  if (auswahl.art === "kante") {
    const k = canvas.graph.kanten[auswahl.index];
    box.innerHTML = `<p class="hilfe">Kante <b>${esc(canvas.knoten(k.von)?.titel)}</b> → <b>${esc(canvas.knoten(k.nach)?.titel)}</b>${k.port ? ` (Ausgang „${k.port}“)` : ""}</p>
      <button type="button" class="klein sekundaer" id="kante-loeschen">Kante löschen</button>`;
    document.getElementById("kante-loeschen").onclick = () => canvas.kanteEntfernen(auswahl.index);
    return;
  }
  const k = canvas.knoten(auswahl.id);
  if (!k) return;
  const datalist = `<datalist id="pfade">${BEKANNTE_PFADE.map((p) => `<option value="${p}">`).join("")}</datalist>`;
  let felder = `<label>Titel<input data-feld="titel" value="${esc(k.titel || "")}"></label>`;

  if (k.typ === "aktivitaet") {
    const a = katalog.aktivitaeten.find((x) => x.name === k.aktivitaet) || {};
    const eingaben = k.eingaben || (a.eingaben || []).map((e) => ({ pfad: e.pfad }));
    felder += `<label>Aktivität (Temporal-Activity)
        <select data-feld="aktivitaet">${katalog.aktivitaeten.map((x) =>
          `<option value="${x.name}"${x.name === k.aktivitaet ? " selected" : ""}>${esc(x.titel)}</option>`).join("")}</select></label>
      <div class="info-zeile">Dienst <code>${esc(a.dienst || "?")}</code> · Queue <code>${esc(a.queue || "?")}</code></div>
      <div class="feldgruppe"><span>Eingaben (Pfad im Kontext)</span>
        ${(a.eingaben || []).map((e, i) => `<label class="klein-label">${esc(e.name)}
          <input list="pfade" data-eingabe="${i}" value="${esc(eingaben[i]?.pfad ?? "")}"></label>`).join("")}</div>
      <label>Ergebnis ablegen als<input data-feld="ergebnis" value="${esc(k.ergebnis ?? a.ergebnis ?? "")}" placeholder="(kein Ergebnis)"></label>
      <label>Fallstatus während des Schritts<input data-feld="status" value="${esc(k.status ?? a.status ?? "")}"></label>`;
  } else if (k.typ === "signal") {
    const s = katalog.signale.find((x) => x.name === k.signal) || {};
    felder += `<label>Ereignis
        <select data-feld="signal">${katalog.signale.map((x) =>
          `<option value="${x.name}"${x.name === k.signal ? " selected" : ""}>${esc(x.titel)}</option>`).join("")}</select></label>
      <div class="info-zeile">Setzt Status <code>${esc(s.status)}</code>; das Dashboard bietet dann die passende Eingabe an. Ergebnis liegt unter <code>${esc(s.ergebnis)}</code>.</div>
      <label>Erinnerung nach (Sekunden)<input type="number" min="5" data-feld="frist" value="${esc(k.frist ?? s.frist ?? 30)}"></label>
      <label>Empfänger der Erinnerung<input data-feld="empfaenger" value="${esc(k.empfaenger ?? s.empfaenger ?? "")}"></label>`;
  } else if (k.typ === "bedingung") {
    const b = k.bedingung || {};
    felder += `<div class="feldgruppe"><span>Bedingung</span>
        <label class="klein-label">Wert aus dem Kontext<input list="pfade" data-bedingung="links" value="${esc(b.links || "")}"></label>
        <label class="klein-label">Vergleich<select data-bedingung="op">${katalog.vergleiche.map((v) =>
          `<option${v === b.op ? " selected" : ""}>${v}</option>`).join("")}</select></label>
        <label class="klein-label">Vergleichswert<input data-bedingung="rechts" value="${esc(b.rechts ?? "")}"></label></div>
      <div class="info-zeile">Ausgang <b>ja</b> links unten, <b>nein</b> rechts unten. Zahlen werden numerisch verglichen, sonst als Text.</div>`;
  } else if (k.typ === "agent") {
    felder += `<label>Frage an Claude (Platzhalter: {{pfad}})<textarea rows="6" data-feld="prompt">${esc(k.prompt || "")}</textarea></label>
      <label>Ergebnis ablegen als<input data-feld="ergebnis" value="${esc(k.ergebnis || "agent")}"></label>
      <div class="info-zeile">Antwort enthält <code>antwort</code>, <code>einschaetzung</code> (ja/nein/unklar) und <code>konfidenz</code>. Eine Bedingung dahinter kann darauf verzweigen.</div>`;
  } else if (k.typ === "notiz") {
    felder += `<label>Text für den Verlauf (Platzhalter: {{pfad}})<textarea rows="4" data-feld="text">${esc(k.text || "")}</textarea></label>`;
  }

  box.innerHTML = `${datalist}<div class="eigenschaften-kopf"><span class="pill pill-grau">${esc(k.typ)}</span> <code>${esc(k.id)}</code></div>
    ${felder}
    <button type="button" class="klein sekundaer" id="knoten-loeschen" style="margin-top:12px">Knoten löschen</button>`;

  box.querySelectorAll("[data-feld]").forEach((el) => {
    el.onchange = () => {
      let wert = el.value;
      if (el.dataset.feld === "frist") wert = Number(wert);
      if (el.dataset.feld === "ergebnis" && wert === "") wert = null;
      const felderNeu = { [el.dataset.feld]: wert };
      // Beim Wechsel der Aktivität: Eingaben und Ergebnis aus dem Katalog neu setzen
      if (el.dataset.feld === "aktivitaet") {
        const a = katalog.aktivitaeten.find((x) => x.name === wert);
        felderNeu.eingaben = a.eingaben.map((e) => ({ pfad: e.pfad }));
        felderNeu.ergebnis = a.ergebnis; felderNeu.status = a.status;
        if (!k.titel || katalog.aktivitaeten.some((x) => x.titel.startsWith(k.titel))) felderNeu.titel = a.titel.replace(/ \(.*\)$/, "");
      }
      if (el.dataset.feld === "signal") {
        const s = katalog.signale.find((x) => x.name === wert);
        felderNeu.frist = s.frist; felderNeu.empfaenger = s.empfaenger; felderNeu.titel = s.titel;
        delete k.status; delete k.ergebnis; delete k.vorgang; delete k.wartet_auf;
      }
      canvas.knotenAktualisieren(k.id, felderNeu);
      zeichneEigenschaften(auswahl);
    };
  });
  box.querySelectorAll("[data-eingabe]").forEach((el) => {
    el.onchange = () => {
      const a = katalog.aktivitaeten.find((x) => x.name === k.aktivitaet);
      const eingaben = k.eingaben || a.eingaben.map((e) => ({ pfad: e.pfad }));
      eingaben[Number(el.dataset.eingabe)] = { pfad: el.value };
      canvas.knotenAktualisieren(k.id, { eingaben });
    };
  });
  box.querySelectorAll("[data-bedingung]").forEach((el) => {
    el.onchange = () => {
      const b = { ...(k.bedingung || {}) };
      let wert = el.value;
      if (el.dataset.bedingung === "rechts" && wert !== "" && !isNaN(Number(wert))) wert = Number(wert);
      b[el.dataset.bedingung] = wert;
      canvas.knotenAktualisieren(k.id, { bedingung: b });
    };
  });
  document.getElementById("knoten-loeschen").onclick = () => canvas.knotenEntfernen(k.id);
}

/* ---------------- Prüfen / Speichern / Aktivieren ---------------- */
async function pruefen(anzeigen) {
  const antwort = await fetch("/api/prozesse/pruefen", {
    method: "POST", headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ graph: canvas.getGraph() }),
  });
  const e = await antwort.json();
  if (anzeigen) zeigePruefung(e);
  return e;
}

function zeigePruefung(e) {
  const box = document.getElementById("pruef-ergebnis");
  if (!e.fehler.length && !e.warnungen.length) {
    box.innerHTML = `<div class="pruef ok">✓ Graph ist in Ordnung.</div>`; return;
  }
  box.innerHTML = `
    ${e.fehler.map((f) => `<div class="pruef fehler">✕ ${esc(f)}</div>`).join("")}
    ${e.warnungen.map((w) => `<div class="pruef warnung">⚠ ${esc(w)}</div>`).join("")}`;
}

async function speichern() {
  const p = await pruefen(true);
  if (p.fehler.length) { melde("Nicht gespeichert – erst die Fehler beheben.", "fehler"); return; }
  const antwort = await fetch(`/api/prozesse/${encodeURIComponent(PROZESS)}/versionen`, {
    method: "POST", headers: { "Content-Type": "application/json" },
    body: JSON.stringify({
      graph: canvas.getGraph(),
      kommentar: document.getElementById("kommentar").value.trim(),
      aktivieren: document.getElementById("gleich-aktivieren").checked,
    }),
  });
  if (!antwort.ok) {
    const f = await antwort.json().catch(() => ({}));
    melde("Fehler: " + (f.detail?.fehler?.join("; ") || f.detail || antwort.status), "fehler");
    return;
  }
  const neu = await antwort.json();
  ungespeichert = false;
  geladeneVersion = neu.version;
  document.getElementById("kommentar").value = "";
  await versionenLaden();
  versionWahl().value = neu.version;
  zeigeTitel();
  melde(`✓ Version ${neu.version} gespeichert${neu.aktiv ? " und aktiviert – neue Fälle laufen ab jetzt damit" : ""}. Laufende Fälle bleiben bei ihrer Version.`, "ok");
}

async function aktivieren() {
  const nummer = Number(versionWahl().value);
  if (!nummer) return;
  const antwort = await fetch(`/api/prozesse/${encodeURIComponent(PROZESS)}/versionen/${nummer}/aktivieren`, { method: "POST" });
  if (!antwort.ok) { melde("Aktivieren fehlgeschlagen", "fehler"); return; }
  await versionenLaden();
  versionWahl().value = nummer;
  melde(`✓ Version ${nummer} ist jetzt aktiv. Neue Unfallmeldungen laufen damit.`, "ok");
}

function jsonZeigen() {
  document.getElementById("json-inhalt").textContent = JSON.stringify(canvas.getGraph(), null, 2);
  document.getElementById("overlay").hidden = false;
}

function melde(text, klasse) {
  const el = document.getElementById("leiste-meldung");
  el.className = "meldung " + (klasse || "");
  el.textContent = text;
}

async function health() {
  const el = document.getElementById("health");
  try {
    const h = await (await fetch("/api/health")).json();
    const ok = h.datenbank && h.temporal;
    el.className = "pill " + (ok ? "pill-gruen" : "pill-gelb");
    el.textContent = `DB ${h.datenbank ? "✓" : "…"}  Temporal ${h.temporal ? "✓" : "…"}`;
  } catch { el.className = "pill pill-rot"; el.textContent = "API nicht erreichbar"; }
}

start();
