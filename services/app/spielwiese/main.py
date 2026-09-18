"""Spielwiese: Mail rein, Jira-Ticket raus – alles lokal, nichts echt.

Drei Dinge auf einer Seite, damit man einen n8n-Flow "Mail kommt → Agent →
Jira-Ticket" ohne Firmenzugänge vorführen kann:

  * ein Formular, das eine Mail per SMTP an den lokalen Mailserver (GreenMail)
    schickt – das ist das Ereignis, auf das n8n per IMAP wartet,
  * der Posteingang dieses Mailservers (per IMAP gelesen),
  * ein Jira-Stub: nimmt POST /jira/rest/api/2/issue an wie ein echtes Jira,
    vergibt Ticketnummern und zeigt die Tickets an.

Keine Datenbank, alles im Speicher. Nach einem Neustart ist die Liste leer.
"""
import email
import imaplib
import logging
import os
import smtplib
from datetime import datetime, timezone
from email.header import decode_header, make_header
from email.message import EmailMessage
from pathlib import Path

from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import FileResponse

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("spielwiese")

MAIL_HOST = os.getenv("MAIL_HOST", "greenmail")
SMTP_PORT = int(os.getenv("MAIL_SMTP_PORT", "3025"))
IMAP_PORT = int(os.getenv("MAIL_IMAP_PORT", "3143"))
MAIL_USER = os.getenv("MAIL_USER", "demo")
MAIL_PASS = os.getenv("MAIL_PASS", "demo")
MAIL_ADRESSE = os.getenv("MAIL_ADRESSE", "demo@spielwiese.local")

STATIC = Path(__file__).resolve().parent / "static"

app = FastAPI(title="Spielwiese")

tickets: list[dict] = []
webhooks: list[dict] = []


# --- Mail ------------------------------------------------------------------

@app.post("/api/mail", status_code=202)
async def mail_senden(daten: dict) -> dict:
    """Schickt eine Mail an den lokalen Posteingang – das Ereignis für n8n."""
    msg = EmailMessage()
    msg["From"] = daten.get("von") or "kunde@example.com"
    msg["To"] = MAIL_ADRESSE
    msg["Subject"] = daten.get("betreff") or "(kein Betreff)"
    msg.set_content(daten.get("text") or "")
    with smtplib.SMTP(MAIL_HOST, SMTP_PORT, timeout=10) as smtp:
        smtp.send_message(msg)
    log.info("Mail an %s: %s", MAIL_ADRESSE, msg["Subject"])
    return {"hinweis": f"Mail liegt im Posteingang {MAIL_ADRESSE}. "
                       "n8n holt sie beim nächsten IMAP-Poll ab."}


@app.get("/api/posteingang")
async def posteingang() -> list[dict]:
    """Liest den Posteingang per IMAP – dieselbe Schnittstelle, die n8n nutzt."""
    try:
        imap = imaplib.IMAP4(MAIL_HOST, IMAP_PORT, timeout=10)
        imap.login(MAIL_USER, MAIL_PASS)
        imap.select("INBOX", readonly=True)
        _, daten = imap.search(None, "ALL")
        nummern = daten[0].split()[-30:]
        mails = []
        for n in reversed(nummern):
            _, teile = imap.fetch(n, "(FLAGS BODY.PEEK[])")
            roh = next(t[1] for t in teile if isinstance(t, tuple))
            flags = teile[0][0].decode(errors="ignore") if teile and teile[0] else ""
            m = email.message_from_bytes(roh)
            text = ""
            if m.is_multipart():
                for teil in m.walk():
                    if teil.get_content_type() == "text/plain":
                        text = teil.get_payload(decode=True).decode(errors="ignore")
                        break
            else:
                text = m.get_payload(decode=True).decode(errors="ignore")
            mails.append({
                "von": _dekodiert(m.get("From")),
                "betreff": _dekodiert(m.get("Subject")),
                "datum": _dekodiert(m.get("Date")),
                "text": text.strip()[:500],
                "gelesen": "\\Seen" in flags,
            })
        imap.logout()
        return mails
    except Exception as e:  # noqa: BLE001
        raise HTTPException(503, f"Mailserver nicht erreichbar: {e}")


def _dekodiert(wert: str | None) -> str:
    if not wert:
        return ""
    try:
        return str(make_header(decode_header(wert)))
    except Exception:  # noqa: BLE001
        return wert


# --- Jira-Stub -------------------------------------------------------------
# Nimmt genau die Anfrage an, die ein echtes Jira (REST API v2) bekommen wuerde:
#   POST /jira/rest/api/2/issue   {"fields": {"summary": ..., "description": ...}}
# und antwortet wie Jira: {"id": "10001", "key": "DEMO-1", "self": ...}

@app.post("/jira/rest/api/2/issue", status_code=201)
async def jira_issue(anfrage: Request) -> dict:
    daten = await anfrage.json()
    felder = daten.get("fields") or daten
    nummer = len(tickets) + 1
    ticket = {
        "id": str(10000 + nummer),
        "key": f"DEMO-{nummer}",
        "summary": felder.get("summary") or "(ohne Titel)",
        "description": felder.get("description") or "",
        "priority": (felder.get("priority") or {}).get("name") if isinstance(felder.get("priority"), dict) else felder.get("priority"),
        "labels": felder.get("labels") or [],
        "erstellt_am": datetime.now(timezone.utc).isoformat(),
        "roh": daten,
    }
    tickets.append(ticket)
    log.info("Jira-Ticket %s: %s", ticket["key"], ticket["summary"])
    return {"id": ticket["id"], "key": ticket["key"],
            "self": f"{anfrage.base_url}jira/rest/api/2/issue/{ticket['id']}"}


@app.get("/jira/rest/api/2/issue/{schluessel}")
async def jira_issue_lesen(schluessel: str) -> dict:
    for t in tickets:
        if schluessel in (t["key"], t["id"]):
            return {"id": t["id"], "key": t["key"], "fields": {
                "summary": t["summary"], "description": t["description"]}}
    raise HTTPException(404, "Issue Does Not Exist")


@app.get("/api/tickets")
async def tickets_lesen() -> list[dict]:
    return list(reversed(tickets))


# --- Beliebiger Webhook-Empfaenger -----------------------------------------
# Fuer alles, was kein Jira ist: n8n kann hierher posten, die Seite zeigt es an.

@app.post("/webhook/{name}", status_code=202)
async def webhook(name: str, anfrage: Request) -> dict:
    try:
        nutzlast = await anfrage.json()
    except Exception:  # noqa: BLE001
        nutzlast = (await anfrage.body()).decode(errors="ignore")
    webhooks.insert(0, {"name": name, "nutzlast": nutzlast,
                        "zeitpunkt": datetime.now(timezone.utc).isoformat()})
    del webhooks[50:]
    log.info("Webhook '%s' empfangen", name)
    return {"ok": True}


@app.get("/api/webhooks")
async def webhooks_lesen() -> list[dict]:
    return webhooks


@app.get("/api/info")
async def info() -> dict:
    return {"mail_adresse": MAIL_ADRESSE, "imap": f"{MAIL_HOST}:{IMAP_PORT}",
            "smtp": f"{MAIL_HOST}:{SMTP_PORT}", "benutzer": MAIL_USER,
            "n8n_port": os.getenv("N8N_PORT", "25678")}


@app.get("/")
async def index() -> FileResponse:
    return FileResponse(str(STATIC / "index.html"))
