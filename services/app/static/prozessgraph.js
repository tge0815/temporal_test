/* Prozessgraph-Canvas – ohne Framework, ohne Build-Schritt.

   Wird zweimal benutzt:
     * im Designer (editierbar: Knoten ziehen, verbinden, löschen)
     * im Dashboard-Dialog (nur lesen: zeigt live, wo der Fall gerade steht)

   Knoten sind absolut positionierte <div>s, Kanten ein <svg> darunter.
   Das ist bewusst die einfachste Variante, die funktioniert – genau das ist
   der Punkt der Demo. */

const PG_BREITE = 220;

const PG_TYP_INFO = {
  start:      { symbol: "▶", farbe: "gruen",  name: "Start" },
  aktivitaet: { symbol: "⚙", farbe: "blau",   name: "Aktivität" },
  signal:     { symbol: "⏳", farbe: "lila",   name: "Warten" },
  bedingung:  { symbol: "?", farbe: "gelb",   name: "Bedingung" },
  agent:      { symbol: "✦", farbe: "tuerkis", name: "Agent" },
  notiz:      { symbol: "✎", farbe: "grau",   name: "Notiz" },
  ende:       { symbol: "■", farbe: "gruen",  name: "Ende" },
};

const pgEsc = (s) => String(s ?? "").replace(/[&<>"']/g,
  (c) => ({ "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;" }[c]));

class ProzessCanvas {
  constructor(container, optionen = {}) {
    this.container = container;
    this.editierbar = !!optionen.editierbar;
    this.onAuswahl = optionen.onAuswahl || (() => {});
    this.onAenderung = optionen.onAenderung || (() => {});
    this.katalog = optionen.katalog || null;
    this.graph = { knoten: [], kanten: [] };
    this.status = { knoten_status: {}, entscheidungen: {}, pfad: [], aktueller_knoten: null };
    this.auswahl = null;        // { art: "knoten"|"kante", id | index }
    this.zieh = null;           // laufende Drag-Operation

    container.classList.add("pg-container");
    container.innerHTML = `<div class="pg-flaeche">
        <svg class="pg-kanten" xmlns="http://www.w3.org/2000/svg"></svg>
        <div class="pg-knoten-ebene"></div>
      </div>`;
    this.flaeche = container.querySelector(".pg-flaeche");
    this.svg = container.querySelector(".pg-kanten");
    this.ebene = container.querySelector(".pg-knoten-ebene");

    if (this.editierbar) this._editierenVerdrahten();
  }

  /* ---------- Daten rein / raus ---------- */
  setGraph(graph) {
    this.graph = JSON.parse(JSON.stringify(graph || { knoten: [], kanten: [] }));
    this.graph.knoten ||= [];
    this.graph.kanten ||= [];
    this.auswahl = null;
    this.zeichnen();
  }

  getGraph() { return JSON.parse(JSON.stringify(this.graph)); }

  setStatus(status) {
    this.status = { knoten_status: {}, entscheidungen: {}, pfad: [], ...(status || {}) };
    this.zeichnen();
  }

  knoten(id) { return this.graph.knoten.find((k) => k.id === id); }

  knotenHinzufuegen(k) {
    const maxY = Math.max(0, ...this.graph.knoten.map((n) => (n.y || 0) + 90));
    const neu = { x: 60, y: maxY + 40, ...k };
    neu.id ||= "k" + Math.random().toString(36).slice(2, 8);
    this.graph.knoten.push(neu);
    this.auswaehlen({ art: "knoten", id: neu.id });
    this.zeichnen();
    this.onAenderung();
    return neu;
  }

  knotenAktualisieren(id, felder) {
    const k = this.knoten(id);
    if (!k) return;
    Object.assign(k, felder);
    this.zeichnen();
    this.onAenderung();
  }

  knotenEntfernen(id) {
    this.graph.knoten = this.graph.knoten.filter((k) => k.id !== id);
    this.graph.kanten = this.graph.kanten.filter((k) => k.von !== id && k.nach !== id);
    this.auswaehlen(null);
    this.zeichnen();
    this.onAenderung();
  }

  kanteSetzen(von, port, nach) {
    if (von === nach) return;
    this.graph.kanten = this.graph.kanten.filter((k) => !(k.von === von && (k.port || "weiter") === port));
    const kante = { von, nach };
    if (port !== "weiter") kante.port = port;
    this.graph.kanten.push(kante);
    this.zeichnen();
    this.onAenderung();
  }

  kanteEntfernen(index) {
    this.graph.kanten.splice(index, 1);
    this.auswaehlen(null);
    this.zeichnen();
    this.onAenderung();
  }

  auswaehlen(auswahl) {
    this.auswahl = auswahl;
    this.onAuswahl(auswahl);
  }

  /* ---------- Zeichnen ---------- */
  zeichnen() {
    const st = this.status;
    // Größe der Fläche nach den Knoten richten
    const maxX = Math.max(600, ...this.graph.knoten.map((k) => (k.x || 0) + PG_BREITE + 120));
    const maxY = Math.max(400, ...this.graph.knoten.map((k) => (k.y || 0) + 200));
    this.flaeche.style.width = maxX + "px";
    this.flaeche.style.height = maxY + "px";
    this.svg.setAttribute("width", maxX);
    this.svg.setAttribute("height", maxY);

    this.ebene.innerHTML = this.graph.knoten.map((k) => this._knotenHtml(k, st)).join("");

    // Kanten erst nach dem Einfügen der Knoten – wir brauchen deren Höhe.
    const pfad = st.pfad || [];
    const gelaufen = new Set();
    for (let i = 0; i + 1 < pfad.length; i++) gelaufen.add(pfad[i] + "→" + pfad[i + 1]);

    this.svg.innerHTML = `<defs>
        <marker id="pg-pfeil" viewBox="0 0 10 10" refX="9" refY="5" markerWidth="7" markerHeight="7" orient="auto-start-reverse">
          <path d="M0,0 L10,5 L0,10 z" fill="var(--pg-kante)"/></marker>
        <marker id="pg-pfeil-aktiv" viewBox="0 0 10 10" refX="9" refY="5" markerWidth="7" markerHeight="7" orient="auto-start-reverse">
          <path d="M0,0 L10,5 L0,10 z" fill="var(--gruen)"/></marker>
      </defs>` + this.graph.kanten.map((kante, i) => {
      const p = this._kantenPfad(kante);
      if (!p) return "";
      const port = kante.port || "weiter";
      const aktiv = gelaufen.has(kante.von + "→" + kante.nach);
      const gewaehlt = this.auswahl?.art === "kante" && this.auswahl.index === i;
      const label = port === "weiter" ? "" :
        `<text class="pg-kante-label ${port}" x="${p.lx}" y="${p.ly}">${port}</text>`;
      return `<g class="pg-kante ${aktiv ? "gelaufen" : ""} ${gewaehlt ? "gewaehlt" : ""}" data-index="${i}">
          <path class="pg-kante-treffer" d="${p.d}"/>
          <path class="pg-kante-linie" d="${p.d}" marker-end="url(#${aktiv ? "pg-pfeil-aktiv" : "pg-pfeil"})"/>
          ${label}</g>`;
    }).join("");
  }

  _knotenHtml(k, st) {
    const info = PG_TYP_INFO[k.typ] || PG_TYP_INFO.notiz;
    const zustand = st.knoten_status?.[k.id] || "";
    const gewaehlt = this.auswahl?.art === "knoten" && this.auswahl.id === k.id ? "gewaehlt" : "";
    const entscheidung = st.entscheidungen?.[k.id];
    const ports = k.typ === "ende" ? "" : k.typ === "bedingung"
      ? `<span class="pg-port aus ja" data-port="ja" title="Ausgang: ja">ja</span>
         <span class="pg-port aus nein" data-port="nein" title="Ausgang: nein">nein</span>`
      : `<span class="pg-port aus" data-port="weiter" title="Ausgang"></span>`;
    const eingang = k.typ === "start" ? "" : `<span class="pg-port ein"></span>`;
    return `<div class="pg-knoten typ-${k.typ} farbe-${info.farbe} ${zustand} ${gewaehlt}"
                 data-id="${pgEsc(k.id)}" style="left:${k.x || 0}px;top:${k.y || 0}px;width:${PG_BREITE}px">
        ${eingang}
        <div class="pg-kopf"><span class="pg-symbol">${info.symbol}</span>
          <span class="pg-typ">${info.name}</span>
          <span class="pg-zustand">${this._zustandText(zustand)}</span></div>
        <div class="pg-titel">${pgEsc(k.titel || k.id)}</div>
        <div class="pg-detail">${this._detailText(k)}</div>
        ${entscheidung ? `<div class="pg-entscheidung">${pgEsc(entscheidung)}</div>` : ""}
        ${ports}
      </div>`;
  }

  _zustandText(z) {
    return { fertig: "✓ erledigt", aktiv: "● läuft", wartet: "⏳ wartet", fehler: "✕ Fehler" }[z] || "";
  }

  _detailText(k) {
    switch (k.typ) {
      case "aktivitaet": {
        const a = this.katalog?.aktivitaeten?.find((x) => x.name === k.aktivitaet);
        return `<code>${pgEsc(k.aktivitaet || "?")}</code>` +
          (a ? ` <span class="pg-dienst">→ ${pgEsc(a.dienst)}</span>` : "");
      }
      case "signal": {
        const s = this.katalog?.signale?.find((x) => x.name === k.signal);
        const frist = k.frist ?? s?.frist;
        return `Signal <code>${pgEsc(k.signal || "?")}</code>` +
          (frist ? `<br>Erinnerung nach ${pgEsc(frist)} s` : "");
      }
      case "bedingung": {
        const b = k.bedingung || {};
        return `<code>${pgEsc(b.links || "?")} ${pgEsc(b.op || "?")} ${pgEsc(b.rechts ?? "?")}</code>`;
      }
      case "agent":
        return pgEsc((k.prompt || "").slice(0, 90)) + ((k.prompt || "").length > 90 ? " …" : "");
      case "notiz":
        return pgEsc((k.text || "").slice(0, 90));
      case "start":
        return "Kontext: <code>fall</code>";
      default:
        return "";
    }
  }

  _knotenBox(id) {
    const el = this.ebene.querySelector(`.pg-knoten[data-id="${CSS.escape(id)}"]`);
    if (!el) return null;
    return { x: el.offsetLeft, y: el.offsetTop, w: el.offsetWidth, h: el.offsetHeight, el };
  }

  _portPosition(id, port) {
    const b = this._knotenBox(id);
    if (!b) return null;
    if (port === "ja") return { x: b.x + b.w * 0.28, y: b.y + b.h };
    if (port === "nein") return { x: b.x + b.w * 0.72, y: b.y + b.h };
    return { x: b.x + b.w / 2, y: b.y + b.h };
  }

  _kantenPfad(kante) {
    const von = this._portPosition(kante.von, kante.port || "weiter");
    const zielBox = this._knotenBox(kante.nach);
    if (!von || !zielBox) return null;
    const nach = { x: zielBox.x + zielBox.w / 2, y: zielBox.y };
    return this._bezier(von, nach);
  }

  _bezier(von, nach) {
    const dy = Math.max(40, Math.abs(nach.y - von.y) / 2);
    const d = `M${von.x},${von.y} C${von.x},${von.y + dy} ${nach.x},${nach.y - dy} ${nach.x},${nach.y}`;
    return { d, lx: von.x + (nach.x - von.x) * 0.25, ly: von.y + (nach.y - von.y) * 0.25 - 6 };
  }

  /* ---------- Bearbeiten ---------- */
  _editierenVerdrahten() {
    const pos = (e) => {
      const r = this.flaeche.getBoundingClientRect();
      return { x: e.clientX - r.left, y: e.clientY - r.top };
    };

    this.container.addEventListener("mousedown", (e) => {
      const port = e.target.closest(".pg-port.aus");
      const knotenEl = e.target.closest(".pg-knoten");
      if (port && knotenEl) {
        // Neue Kante ziehen
        e.preventDefault();
        this.zieh = { art: "kante", von: knotenEl.dataset.id, port: port.dataset.port };
        this._tempKante(pos(e));
        return;
      }
      if (knotenEl) {
        const k = this.knoten(knotenEl.dataset.id);
        const p = pos(e);
        this.zieh = { art: "knoten", id: k.id, dx: p.x - (k.x || 0), dy: p.y - (k.y || 0), bewegt: false };
        this.auswaehlen({ art: "knoten", id: k.id });
        this.zeichnen();
        e.preventDefault();
        return;
      }
      const kanteEl = e.target.closest(".pg-kante");
      if (kanteEl) {
        this.auswaehlen({ art: "kante", index: Number(kanteEl.dataset.index) });
        this.zeichnen();
        return;
      }
      if (e.target === this.flaeche || e.target === this.svg || e.target === this.ebene) {
        this.auswaehlen(null);
        this.zeichnen();
      }
    });

    window.addEventListener("mousemove", (e) => {
      if (!this.zieh) return;
      const p = pos(e);
      if (this.zieh.art === "knoten") {
        const k = this.knoten(this.zieh.id);
        k.x = Math.max(0, Math.round((p.x - this.zieh.dx) / 10) * 10);
        k.y = Math.max(0, Math.round((p.y - this.zieh.dy) / 10) * 10);
        this.zieh.bewegt = true;
        this.zeichnen();
      } else {
        this._tempKante(p);
        this.ebene.querySelectorAll(".pg-knoten").forEach((el) => el.classList.remove("ziel"));
        const ziel = document.elementFromPoint(e.clientX, e.clientY)?.closest?.(".pg-knoten");
        if (ziel && ziel.dataset.id !== this.zieh.von) ziel.classList.add("ziel");
      }
    });

    window.addEventListener("mouseup", (e) => {
      if (!this.zieh) return;
      const z = this.zieh;
      this.zieh = null;
      if (z.art === "knoten") {
        if (z.bewegt) this.onAenderung();
        return;
      }
      this.svg.querySelector(".pg-temp")?.remove();
      const ziel = document.elementFromPoint(e.clientX, e.clientY)?.closest?.(".pg-knoten");
      if (ziel && ziel.dataset.id !== z.von && this.knoten(ziel.dataset.id).typ !== "start") {
        this.kanteSetzen(z.von, z.port, ziel.dataset.id);
      } else {
        this.zeichnen();
      }
    });

    document.addEventListener("keydown", (e) => {
      if (e.key !== "Delete" && e.key !== "Backspace") return;
      if (["INPUT", "TEXTAREA", "SELECT"].includes(document.activeElement?.tagName)) return;
      if (!this.auswahl) return;
      e.preventDefault();
      if (this.auswahl.art === "knoten") this.knotenEntfernen(this.auswahl.id);
      else this.kanteEntfernen(this.auswahl.index);
    });
  }

  _tempKante(p) {
    const von = this._portPosition(this.zieh.von, this.zieh.port);
    if (!von) return;
    let el = this.svg.querySelector(".pg-temp");
    if (!el) {
      el = document.createElementNS("http://www.w3.org/2000/svg", "path");
      el.setAttribute("class", "pg-temp");
      this.svg.appendChild(el);
    }
    el.setAttribute("d", this._bezier(von, p).d);
  }
}

window.ProzessCanvas = ProzessCanvas;
