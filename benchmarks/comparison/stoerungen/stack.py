"""Eigener Compose-Stack des Garantie-Vergleichs und Ziel jeder Störung (Plan 2026-10-24-1118, U9).

RabbitMQ, ein Valkey mit und eines ohne Persistenz, davor Toxiproxy. Alle
Clients, Erzeuger wie Worker, sprechen nur über Toxiproxy mit den Diensten,
damit die Netztrennung jede Verbindung trifft.
"""

from __future__ import annotations

import asyncio
from pathlib import Path

from garantieprofile import BROKER

#: Compose-Projekt des Stacks; nur seine Container startet, stört und entfernt der Vergleich.
PROJEKT = "swarmq-vergleich-stoerung"
COMPOSE = Path(__file__).with_name("compose.yml")

SIGKILL = "sigkill"
BROKER_NEUSTART = "broker_neustart"
NETZTRENNUNG = "netztrennung"
VALKEY_PERSISTENT = "valkey_neustart_persistent"
VALKEY_FLUECHTIG = "valkey_neustart_fluechtig"

#: Host-Port des Toxiproxy-Proxys je Compose-Dienst (``compose.yml``,
#: ``toxiproxy.json``). Bewusst keiner der Standard-, Test-, Chaos- oder
#: Vergleichsports (5672/6379, 5699/6399, 5698/6398, 5682/6389).
PORTS = {"rabbitmq": 5686, "valkey": 6386, "valkey-fluechtig": 6387}
#: Host-Port der Toxiproxy-API (Chaos-Tests nutzen 8474).
ADMIN_PORT = 18476
ADMIN_URL = f"http://localhost:{ADMIN_PORT}"
#: Proxys in ``toxiproxy.json``, je einer je Dienst.
PROXYS = list(PORTS)


def _valkey(stoerung: str) -> str:
    """Valkey der Kombination: das flüchtige nur in der Störung, die es neu startet."""
    return "valkey-fluechtig" if stoerung == VALKEY_FLUECHTIG else "valkey"


def dienst(stoerung: str, system: str) -> str:
    """Compose-Dienst, den ``stoerung`` für ``system`` neu startet.

    Der Broker-Neustart trifft den Broker des Systems; das ist bei arq Valkey.
    """
    if stoerung == BROKER_NEUSTART:
        return BROKER[system]
    return _valkey(stoerung)


def umgebung(stoerung: str) -> dict[str, str]:
    """Adressen, über die Adapter und Worker den Stack in dieser Störung erreichen (``systems/base.py``)."""
    return {
        "VERGLEICH_RABBITMQ_URL": f"amqp://guest:guest@localhost:{PORTS['rabbitmq']}/",
        "VERGLEICH_VALKEY_HOST": f"localhost:{PORTS[_valkey(stoerung)]}",
    }


async def compose(*argumente: str) -> None:
    """``docker compose`` für den eigenen Stack; ein Fehler bricht ab."""
    prozess = await asyncio.create_subprocess_exec("docker", "compose", "-p", PROJEKT, "-f", str(COMPOSE), *argumente)
    if await prozess.wait() != 0:
        raise RuntimeError(f"docker compose {' '.join(argumente)} scheiterte")
