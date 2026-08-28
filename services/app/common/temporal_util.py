"""Gemeinsame Hilfsfunktion: robust zu Temporal verbinden."""
import asyncio
import logging

from temporalio.client import Client

from . import config

log = logging.getLogger(__name__)


async def verbinde(versuche: int = 90) -> Client:
    for i in range(versuche):
        try:
            c = await Client.connect(config.TEMPORAL_HOST,
                                     namespace=config.TEMPORAL_NAMESPACE)
            log.info("Temporal verbunden (%s)", config.TEMPORAL_HOST)
            return c
        except Exception as e:  # noqa: BLE001
            log.warning("Warte auf Temporal (%s/%s): %s", i + 1, versuche, e)
            await asyncio.sleep(2)
    raise RuntimeError("Temporal nicht erreichbar")
