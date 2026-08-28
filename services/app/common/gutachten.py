"""Alles rund um das ärztliche Gutachten.

Drei Aufgaben:
  1. Ein realistisch aussehendes Beispiel-Gutachten als PDF erzeugen
     (damit man die Demo ohne eigene Datei durchspielen kann).
  2. Aus einem hochgeladenen Dokument den Text herausholen (PDF oder Textdatei).
  3. Aus diesem Text die MdE in Prozent ermitteln – mit Claude, falls ein
     API-Schlüssel hinterlegt ist, sonst über ein einfaches Regelwerk.
"""
from __future__ import annotations

import io
import logging
import random
import re
from datetime import date

from pydantic import BaseModel, Field

from . import config

log = logging.getLogger(__name__)


# ===========================================================================
#  1. Beispiel-Gutachten erzeugen
# ===========================================================================

BEFUNDE = {
    "Hand": "Bewegungseinschränkung der Finger II–V, Kraftminderung beim Faustschluss",
    "Arm": "Eingeschränkte Beweglichkeit im Ellenbogengelenk, muskuläre Atrophie",
    "Bein": "Belastungsschmerz, hinkendes Gangbild, Umfangsminderung des Oberschenkels",
    "Fuß": "Fehlstellung nach knöcherner Konsolidierung, Abrollstörung",
    "Auge": "Herabgesetzte Sehschärfe, eingeschränktes Gesichtsfeld",
    "Wirbelsäule": "Bewegungseinschränkung der Lendenwirbelsäule, belastungsabhängige Schmerzen",
    "Kopf": "Konzentrationsstörungen und Kopfschmerzen nach Schädel-Hirn-Trauma",
    "Schulter": "Abduktion nur bis 90° möglich, Kraftminderung bei Überkopfarbeit",
    "Knie": "Streckdefizit von 10°, Instabilität bei seitlicher Belastung",
    "Sonstiges": "Funktionsbeeinträchtigung mit belastungsabhängiger Schmerzsymptomatik",
}

BASIS_KOERPERTEIL = {
    "Hand": 20, "Arm": 30, "Bein": 30, "Fuß": 20, "Auge": 25,
    "Wirbelsäule": 30, "Kopf": 35, "Schulter": 20, "Knie": 20, "Sonstiges": 15,
}
FAKTOR_SCHWERE = {"leicht": 0.4, "mittel": 0.8, "schwer": 1.3, "sehr schwer": 1.8}


def plausibler_mde_wert(koerperteil: str, schwere: str) -> int:
    """Leitet einen plausiblen MdE-Wert aus Körperteil und Schweregrad ab."""
    basis = BASIS_KOERPERTEIL.get(koerperteil, 15)
    faktor = FAKTOR_SCHWERE.get((schwere or "mittel").lower(), 0.8)
    roh = basis * faktor * random.uniform(0.85, 1.15)
    return int(min(100, max(10, round(roh / 5) * 5)))


def beispiel_gutachten_text(fall: dict) -> tuple[str, int]:
    """Baut den Text eines Beispiel-Gutachtens. Gibt Text und MdE-Wert zurück."""
    koerperteil = fall.get("koerperteil") or "Sonstiges"
    schwere = fall.get("schwere") or "mittel"
    mde = plausibler_mde_wert(koerperteil, schwere)
    befund = BEFUNDE.get(koerperteil, BEFUNDE["Sonstiges"])
    heute = date.today().strftime("%d.%m.%Y")

    text = f"""FACHÄRZTLICHES GUTACHTEN
zur Feststellung der Minderung der Erwerbsfähigkeit (MdE)

Gutachterin: Dr. med. Sabine Hoffmann, Fachärztin für Unfallchirurgie
Praxis am Klinikum, Beispielstraße 12, 12345 Musterstadt
Gutachten-Nr.: GA-{fall.get('fall_nummer', 'UV-0000-000000')}
Datum der Untersuchung: {heute}

1. AUFTRAG
Im Auftrag der Berufsgenossenschaft wird zum Vorgang
{fall.get('fall_nummer', '')} die unfallbedingte Minderung der
Erwerbsfähigkeit begutachtet.

2. ANGABEN ZUR VERSICHERTEN PERSON
Name:            {fall.get('versicherter', '')}
Geburtsjahr:     {fall.get('geburtsjahr', '')}
Beruf:           {fall.get('beruf', '')}
Unfalltag:       {fall.get('unfall_datum', '')}

3. UNFALLHERGANG (nach Aktenlage)
{fall.get('hergang') or 'Kein näherer Hergang aktenkundig.'}

4. BEFUND
Betroffene Körperregion: {koerperteil}
Schweregrad der Verletzung: {schwere}
Klinischer Befund: {befund}.
Die Beschwerden bestehen fort; eine wesentliche Besserung ist nach
Abschluss der Heilbehandlung nicht mehr zu erwarten.

5. BEURTEILUNG
Die unfallbedingten Funktionsstörungen sind gegenüber dem Vorzustand
eindeutig abgrenzbar. Unter Berücksichtigung der einschlägigen
Erfahrungswerte und des erhobenen Befundes ist die verbliebene
Beeinträchtigung im allgemeinen Erwerbsleben wie folgt zu bewerten:

   Die Minderung der Erwerbsfähigkeit (MdE) beträgt {mde} v. H.

Diese Einschätzung gilt ab dem Tag nach Abschluss der Heilbehandlung
auf unbestimmte Zeit. Eine Nachuntersuchung wird in 24 Monaten empfohlen.

Musterstadt, den {heute}

Dr. med. Sabine Hoffmann
"""
    return text, mde


# Die in fpdf eingebauten Schriften beherrschen nur latin-1. Typografische
# Zeichen müssen deshalb ersetzt werden, sonst bricht die PDF-Erzeugung ab.
TYPOGRAFIE = {
    "–": "-", "—": "-", "„": '"', "“": '"', "”": '"', "«": '"', "»": '"',
    "‘": "'", "’": "'", "‚": "'", "…": "...", "•": "-", "→": "->", "≈": "~",
    "\u00a0": " ", "\u202f": " ",
}


def _latin1_tauglich(text: str) -> str:
    """Macht beliebigen Text für die eingebauten PDF-Schriften darstellbar.

    Der Unfallhergang ist ein freies Textfeld – da kann alles drinstehen,
    bis hin zu Emojis. Was sich nicht abbilden lässt, wird zu '?'.
    """
    for zeichen, ersatz in TYPOGRAFIE.items():
        text = text.replace(zeichen, ersatz)
    return text.encode("latin-1", "replace").decode("latin-1")


def beispiel_gutachten_pdf(fall: dict) -> tuple[bytes, int]:
    """Erzeugt das Beispiel-Gutachten als PDF. Gibt PDF-Bytes und MdE zurück."""
    from fpdf import FPDF
    from fpdf.enums import XPos, YPos

    text, mde = beispiel_gutachten_text(fall)

    pdf = FPDF(format="A4")
    # Ränder VOR add_page setzen - sonst rechnet fpdf mit der falschen Breite.
    pdf.set_margins(20, 20, 20)
    pdf.set_auto_page_break(auto=True, margin=20)
    pdf.add_page()

    for absatz in map(_latin1_tauglich, text.split("\n")):
        if not absatz.strip():
            pdf.ln(4)
            continue
        if absatz.isupper() or re.match(r"^\d\. [A-ZÄÖÜ]", absatz):
            pdf.set_font("Helvetica", "B", 11)
        elif "Minderung der Erwerbsfähigkeit (MdE) beträgt" in absatz:
            pdf.set_font("Helvetica", "B", 12)
        else:
            pdf.set_font("Helvetica", "", 10)
        # new_x=LMARGIN ist wichtig: sonst bleibt der Cursor am rechten
        # Rand stehen und der naechste Absatz hat keine Breite mehr.
        pdf.multi_cell(0, 5.5, absatz, new_x=XPos.LMARGIN, new_y=YPos.NEXT)

    ausgabe = pdf.output()
    return bytes(ausgabe), mde


# ===========================================================================
#  2. Text aus einem hochgeladenen Dokument holen
# ===========================================================================

def text_aus_dokument(inhalt: bytes, medientyp: str | None,
                      dateiname: str = "") -> str:
    """Holt den Text aus PDF, Text- oder Markdown-Datei."""
    ist_pdf = (medientyp or "").endswith("pdf") or dateiname.lower().endswith(".pdf") \
        or inhalt[:5] == b"%PDF-"
    if ist_pdf:
        try:
            from pypdf import PdfReader
            leser = PdfReader(io.BytesIO(inhalt))
            return "\n".join((seite.extract_text() or "") for seite in leser.pages)
        except Exception:  # noqa: BLE001
            log.exception("PDF konnte nicht gelesen werden")
            return ""
    for kodierung in ("utf-8", "latin-1"):
        try:
            return inhalt.decode(kodierung)
        except UnicodeDecodeError:
            continue
    return ""


# ===========================================================================
#  3. MdE aus dem Gutachten ermitteln
# ===========================================================================

class GutachtenBefund(BaseModel):
    """Was der mde-agent aus dem Gutachten herauslesen soll."""

    mde_prozent: int = Field(
        ge=0, le=100,
        description="Die im Gutachten festgestellte Minderung der "
                    "Erwerbsfähigkeit in Prozent (v. H.).")
    konfidenz: float = Field(
        ge=0, le=1,
        description="Wie eindeutig steht der Wert im Text? 1.0 = wörtlich "
                    "benannt, niedriger bei Unschärfe oder Widersprüchen.")
    begruendung: str = Field(
        description="Ein bis zwei Sätze auf Deutsch: worauf sich der Wert "
                    "stützt, mit Bezug auf Befund und Fundstelle im Text.")
    fundstelle: str = Field(
        default="",
        description="Der Satz aus dem Gutachten, in dem die MdE genannt wird.")


SYSTEM_PROMPT = (
    "Du unterstützt die Sachbearbeitung einer gesetzlichen Unfallversicherung. "
    "Du liest ein fachärztliches Gutachten und entnimmst ihm die festgestellte "
    "Minderung der Erwerbsfähigkeit (MdE) in Prozent. "
    "Die MdE steht üblicherweise als Prozentzahl oder als 'v. H.' im Abschnitt "
    "Beurteilung. Übernimm ausschließlich den im Gutachten genannten Wert – "
    "schätze nicht und rechne nichts um. Findest du keinen Wert, setze "
    "mde_prozent auf 0 und die Konfidenz auf 0."
)


async def mde_aus_gutachten(text: str) -> dict:
    """Ermittelt die MdE aus dem Gutachtentext.

    Mit hinterlegtem API-Schlüssel liest Claude den Wert aus, sonst greift das
    Regelwerk unten. In beiden Fällen ist das Ergebnis gleich aufgebaut.
    """
    if config.ANTHROPIC_API_KEY:
        try:
            return await _mit_claude(text)
        except Exception:  # noqa: BLE001
            log.exception("Claude-Extraktion fehlgeschlagen – nutze Regelwerk")
    return _mit_regelwerk(text)


async def _mit_claude(text: str) -> dict:
    from anthropic import AsyncAnthropic

    client = AsyncAnthropic(api_key=config.ANTHROPIC_API_KEY)
    antwort = await client.messages.parse(
        model=config.CLAUDE_MODELL,
        max_tokens=16000,
        system=SYSTEM_PROMPT,
        messages=[{
            "role": "user",
            "content": "Hier ist das Gutachten:\n\n<gutachten>\n"
                       f"{text}\n</gutachten>\n\n"
                       "Entnimm ihm die festgestellte MdE.",
        }],
        output_format=GutachtenBefund,
    )
    # Bei einer Ablehnung gibt es kein verwertbares Ergebnis -> Regelwerk.
    if getattr(antwort, "stop_reason", None) == "refusal":
        raise RuntimeError("Anfrage wurde abgelehnt")

    befund: GutachtenBefund = antwort.parsed_output
    return {
        "mde_prozent": befund.mde_prozent,
        "konfidenz": round(befund.konfidenz, 2),
        "begruendung": befund.begruendung,
        "fundstelle": befund.fundstelle,
        "quelle": f"Gutachten, ausgelesen von Claude ({config.CLAUDE_MODELL})",
        "verfahren": "claude",
    }


# Sucht Formulierungen wie "MdE beträgt 30 v. H." oder "MdE: 30 %".
MUSTER = [
    re.compile(r"MdE[^.\n]{0,60}?(\d{1,3})\s*(?:v\.?\s*H\.?|%|vom Hundert)",
               re.IGNORECASE),
    re.compile(r"(\d{1,3})\s*(?:v\.?\s*H\.?|%)[^.\n]{0,40}?MdE", re.IGNORECASE),
    re.compile(r"Minderung der Erwerbsf[äa]higkeit[^.\n]{0,60}?(\d{1,3})\s*"
               r"(?:v\.?\s*H\.?|%)", re.IGNORECASE),
]


def _mit_regelwerk(text: str) -> dict:
    """Ohne API-Schlüssel: Textsuche nach der MdE-Angabe."""
    for muster in MUSTER:
        treffer = muster.search(text or "")
        if not treffer:
            continue
        wert = int(treffer.group(1))
        if not 0 <= wert <= 100:
            continue
        satz = _satz_um(text, treffer.start())
        return {
            "mde_prozent": wert,
            "konfidenz": 0.75,
            "begruendung": f"Im Gutachten wörtlich genannter Wert von {wert} % "
                           f"wurde per Textsuche übernommen. Für eine inhaltliche "
                           f"Würdigung des Befundes wird ein API-Schlüssel für "
                           f"Claude benötigt.",
            "fundstelle": satz,
            "quelle": "Gutachten, ausgelesen per Textsuche (kein API-Schlüssel)",
            "verfahren": "regelwerk",
        }
    return {
        "mde_prozent": 0,
        "konfidenz": 0.0,
        "begruendung": "Im hochgeladenen Dokument war keine MdE-Angabe zu finden.",
        "fundstelle": "",
        "quelle": "Gutachten, ausgelesen per Textsuche (kein API-Schlüssel)",
        "verfahren": "regelwerk",
    }


def _satz_um(text: str, position: int) -> str:
    anfang = max(text.rfind(".", 0, position), text.rfind("\n", 0, position)) + 1
    ende = min([x for x in (text.find(".", position), text.find("\n", position))
                if x != -1] or [len(text)])
    return " ".join(text[anfang:ende + 1].split())[:300]
