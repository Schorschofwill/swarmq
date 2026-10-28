# ruff: noqa: INP001 - eigenständiges Skript für celsius, kein Paket (Verzeichnis mit Bindestrich)
"""Runner-Supervisor für celsius (Plan 2026-10-28-1308, U6, KTD4/KTD6).

Nur Standardbibliothek: Auf celsius sind weder pip noch jq vorausgesetzt.
"""

from __future__ import annotations

import argparse
import contextlib
import fcntl
import functools
import json
import os
import shutil
import signal
import subprocess
import sys
import time
import urllib.error
import urllib.request
import uuid
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import TYPE_CHECKING, Any, Protocol

if TYPE_CHECKING:
    from collections.abc import Callable, Generator, Iterable, Mapping

_PAUSE_POLL = 5.0
_RUN_POLL = 5.0
_BACKOFF_START = 5.0
_BACKOFF_MAX = 300.0
_STOP_GRACE = 30.0
_WAIT_POLL = 2.0
_DELETED = 204
_BUSY = 422
_PAT_WARN = timedelta(days=14)


def _say(message: str) -> None:
    # stdout landet unter systemd im Journal.
    sys.stdout.write(message + "\n")
    sys.stdout.flush()


class ApiError(Exception):
    """Fehlgeschlagener GitHub-Aufruf; die Meldung enthält nie das PAT."""


@dataclass(frozen=True)
class Config:
    """Einstellungen aus der Umgebung (systemd: ``EnvironmentFile``, siehe ``github-runner.env.example``).

    Distribution und Docker-Socket brauchen nur ``slot`` und ``status``;
    ``pause`` und ``resume`` kommen ohne sie aus.
    """

    state_dir: Path
    token_file: Path
    owner: str
    user: str
    home: Path
    dist_dir: Path | None
    docker_host: str | None

    @classmethod
    def from_env(cls, env: Mapping[str, str]) -> Config:
        def get(name: str) -> str | None:
            # ``NAME=`` in der Umgebungsdatei heißt „Vorgabe“, nicht „leerer Pfad“.
            return env.get(f"GH_RUNNER_{name}") or None

        user = get("USER") or "ghrunner"
        dist = get("DIST")
        return cls(
            state_dir=Path(get("STATE_DIR") or "/var/lib/github-runner"),
            token_file=Path(get("TOKEN_FILE") or "/etc/github-runner/token"),
            owner=get("OWNER") or "Schorschofwill",
            user=user,
            home=Path(get("HOME") or f"/home/{user}"),
            dist_dir=Path(dist) if dist else None,
            docker_host=get("DOCKER_HOST"),
        )

    @property
    def runner_version(self) -> str | None:
        return None if self.dist_dir is None else self.dist_dir.name


class Clock(Protocol):
    def sleep(self, seconds: float) -> None: ...
    def monotonic(self) -> float: ...


class SystemClock:
    def sleep(self, seconds: float) -> None:
        time.sleep(seconds)

    def monotonic(self) -> float:
        return time.monotonic()


@dataclass(frozen=True)
class JitConfig:
    runner_id: int
    encoded: str


@dataclass(frozen=True)
class Release:
    version: str
    # Header ``github-authentication-token-expiration``, fehlt bei PATs ohne Ablauf.
    token_expiration: str | None


@dataclass(frozen=True)
class Response:
    status: int
    headers: Mapping[str, str]
    body: bytes


class Http(Protocol):
    def __call__(self, method: str, url: str, headers: dict[str, str], body: bytes | None) -> Response: ...


class UrllibHttp:
    def __init__(self, opener: Callable[..., Any] = urllib.request.urlopen, timeout: float = 30.0) -> None:
        self.opener = opener
        self.timeout = timeout

    def __call__(self, method: str, url: str, headers: dict[str, str], body: bytes | None) -> Response:
        request = urllib.request.Request(url, data=body, headers=headers, method=method)  # noqa: S310 - nur https://api.github.com
        try:
            with self.opener(request, timeout=self.timeout) as answer:
                return Response(status=answer.status, headers=_lower(answer.headers.items()), body=answer.read())
        except urllib.error.HTTPError as error:
            # 4xx/5xx sind Antworten: 422 heißt beim Löschen „beschäftigt“.
            with error:
                return Response(status=error.code, headers=_lower(error.headers.items()), body=error.read())
        except OSError as error:
            # URLError und Timeouts: Netz weg, DNS, TLS. Die Schleife wartet und versucht es neu.
            raise ApiError(f"{method} {url}: {error}") from error


def _lower(headers: Iterable[tuple[str, str]]) -> dict[str, str]:
    return {key.lower(): value for key, value in headers}


class GitHubApi:
    """Die wenigen REST-Aufrufe des Supervisors; das PAT verlässt diese Klasse nur im Header."""

    def __init__(self, http: Http, token: str, owner: str, base: str = "https://api.github.com") -> None:
        self._http = http
        self._token = token
        self.owner = owner
        self.base = base

    def _call(self, method: str, path: str, payload: object = None) -> Response:
        headers = {
            "Authorization": f"Bearer {self._token}",
            "Accept": "application/vnd.github+json",
            "X-GitHub-Api-Version": "2022-11-28",
        }
        body = None if payload is None else json.dumps(payload).encode()
        if body is not None:
            headers["Content-Type"] = "application/json"
        return self._http(method, self.base + path, headers, body)

    def _expect(self, response: Response, *ok: int) -> Response:
        if response.status not in ok:
            message = response.body.decode(errors="replace")[:200]
            raise ApiError(f"HTTP {response.status}: {message}")
        return response

    def jit_config(self, repo: str, name: str, labels: list[str], work_folder: str) -> JitConfig:
        response = self._expect(
            self._call(
                "POST",
                f"/repos/{self.owner}/{repo}/actions/runners/generate-jitconfig",
                {"name": name, "runner_group_id": 1, "labels": labels, "work_folder": work_folder},
            ),
            201,
        )
        data = json.loads(response.body)
        return JitConfig(runner_id=int(data["runner"]["id"]), encoded=str(data["encoded_jit_config"]))

    def latest_runner_release(self) -> Release:
        # Authentifiziert, damit die Antwort auch das Ablaufdatum des PAT trägt.
        response = self._expect(self._call("GET", "/repos/actions/runner/releases/latest"), 200)
        tag = str(json.loads(response.body)["tag_name"])
        return Release(
            version=tag.removeprefix("v"),
            token_expiration=response.headers.get("github-authentication-token-expiration"),
        )

    def delete_runner(self, repo: str, runner_id: int) -> bool:
        """True: gelöscht (204). False: beschäftigt, die API lehnt ab (422)."""
        response = self._expect(
            self._call("DELETE", f"/repos/{self.owner}/{repo}/actions/runners/{runner_id}"), _DELETED, _BUSY
        )
        return response.status == _DELETED


# Zustandswörter in slots/<slot>.state. scripts/vergleich-entfernt.sh liest
# dieselben Dateien und wartet auf PAUSIERT; die Wörter sind also Vertrag.
HOLT = "holt"
LAEUFT = "laeuft"
PAUSIERT = "pausiert"
FEHLER = "fehler"


class Switch:
    """Schalter ``pause``, Sperre ``lock`` und ``slots/<slot>.state`` unter dem Zustandsverzeichnis.

    Dieselben Dateien setzt und liest ``scripts/vergleich-entfernt.sh`` über
    einen Helfer-Container; Namen und Wörter sind deshalb Vertrag.
    """

    def __init__(self, state_dir: Path) -> None:
        self.state_dir = state_dir
        self.slots_dir = state_dir / "slots"
        self._pause_file = state_dir / "pause"

    def ensure(self) -> None:
        self.slots_dir.mkdir(parents=True, exist_ok=True)

    def paused(self) -> bool:
        return self._pause_file.exists()

    def engage(self) -> None:
        self._pause_file.touch()

    def release(self) -> None:
        self._pause_file.unlink()

    @contextlib.contextmanager
    def lock(self) -> Generator[None]:
        with (self.state_dir / "lock").open("a") as handle:
            fcntl.flock(handle, fcntl.LOCK_EX)
            try:
                yield
            finally:
                fcntl.flock(handle, fcntl.LOCK_UN)

    def write_state(self, slot: str, word: str) -> None:
        (self.slots_dir / f"{slot}.state").write_text(f"{word}\n")

    def states(self) -> dict[str, str]:
        return {
            path.name.removesuffix(".state"): path.read_text().strip()
            for path in sorted(self.slots_dir.glob("*.state"))
        }


class GitHub(Protocol):
    def jit_config(self, repo: str, name: str, labels: list[str], work_folder: str) -> JitConfig: ...
    def delete_runner(self, repo: str, runner_id: int) -> bool: ...


class RunnerHandle(Protocol):
    def poll(self) -> int | None: ...
    def stop(self) -> None: ...


class Launcher(Protocol):
    def start(self, slot_dir: Path, jitconfig: str) -> RunnerHandle: ...


def _chown_tree(root: Path, user: str) -> None:
    shutil.chown(root, user=user, group=user)
    for parent, dirs, files in os.walk(root):
        for name in dirs + files:
            shutil.chown(Path(parent) / name, user=user, group=user)


class HostLauncher:
    """Startet den Runner als Host-Prozess unter ``ghrunner`` aus einer frischen Kopie.

    Kein Runner-Container: Bei ``container:``-Jobs bindet der Runner seine
    externals über den Docker-Daemon in den Job-Container ein. Nur als
    Host-Prozess sieht der Daemon dieselben Pfade wie der Runner.
    """

    def __init__(
        self,
        dist_dir: Path,
        user: str,
        home: Path,
        docker_host: str,
        popen: Callable[..., Any] = subprocess.Popen,
        chown: Callable[[Path], None] | None = None,
        killpg: Callable[[int, int], None] = os.killpg,
    ) -> None:
        self.dist_dir = dist_dir
        self.user = user
        self.home = home
        self.docker_host = docker_host
        self.popen = popen
        self.chown = chown if chown is not None else functools.partial(_chown_tree, user=user)
        self.killpg = killpg

    def start(self, slot_dir: Path, jitconfig: str) -> RunnerHandle:
        copy = slot_dir / "runner"
        # Reste eines abgebrochenen Laufs (Neustart der Unit mitten im Job).
        shutil.rmtree(slot_dir, ignore_errors=True)
        shutil.copytree(self.dist_dir, copy, symlinks=True)
        # Der Runner schreibt in seine Kopie (.runner, _diag, _work).
        self.chown(copy)
        # Eigene, leere Umgebung: Nichts vom Supervisor (PAT-Pfad, Variablen
        # von systemd) erreicht den Runner, nur die JIT-Konfiguration.
        env = {
            "DOCKER_HOST": self.docker_host,
            "HOME": str(self.home),
            "USER": self.user,
            "PATH": "/usr/local/bin:/usr/bin:/bin",
        }
        process = self.popen(
            [str(copy / "run.sh"), "--jitconfig", jitconfig],
            cwd=copy,
            env=env,
            user=self.user,
            group=self.user,
            extra_groups=[],
            start_new_session=True,
        )
        return _HostRunner(process, self.killpg)


class _HostRunner:
    def __init__(self, process: Any, killpg: Callable[[int, int], None]) -> None:
        self.process = process
        self.killpg = killpg

    def poll(self) -> int | None:
        result: int | None = self.process.poll()
        return result

    def stop(self) -> None:
        # Die ganze Gruppe: run.sh startet den Listener als Kindprozess.
        self.killpg(self.process.pid, signal.SIGTERM)
        try:
            self.process.wait(timeout=_STOP_GRACE)
        except subprocess.TimeoutExpired:
            self.killpg(self.process.pid, signal.SIGKILL)
            self.process.wait()


class Slot:
    def __init__(
        self, repo: str, number: int, switch: Switch, github: GitHub, launcher: Launcher, clock: Clock
    ) -> None:
        self.repo = repo
        self.name = f"{repo}-{number}"
        self.switch = switch
        self.github = github
        self.launcher = launcher
        self.clock = clock
        self._backoff = _BACKOFF_START

    def run_once(self) -> None:
        self.switch.ensure()
        if self.switch.paused():
            self.switch.write_state(self.name, PAUSIERT)
            self.clock.sleep(_PAUSE_POLL)
            return
        self.switch.write_state(self.name, HOLT)
        try:
            jit = self.github.jit_config(
                self.repo, self._runner_name(), ["self-hosted", f"celsius-{self.repo}"], "_work"
            )
        except ApiError as error:
            _say(f"{self.name}: JIT-Konfiguration fehlgeschlagen ({error}), nächster Versuch in {self._backoff:.0f} s")
            self.switch.write_state(self.name, FEHLER)
            self.clock.sleep(self._backoff)
            self._backoff = min(self._backoff * 2, _BACKOFF_MAX)
            return
        self._backoff = _BACKOFF_START
        slot_dir = self.switch.slots_dir / self.name
        # Unter der Sperre: ``pause`` setzt den Schalter unter derselben Sperre,
        # zwischen Nachprüfen und Start kann er also nicht erscheinen.
        with self.switch.lock():
            if self.switch.paused():
                # Pause kam während des Holens: Registrierung zurückgeben, nichts starten.
                try:
                    self.github.delete_runner(self.repo, jit.runner_id)
                except ApiError as error:
                    # Nie gestartet, also nie online: GitHub räumt die Registrierung selbst ab.
                    _say(f"{self.name}: verworfene Registrierung nicht gelöscht ({error})")
                self.switch.write_state(self.name, PAUSIERT)
                return
            runner = self.launcher.start(slot_dir, jit.encoded)
            self.switch.write_state(self.name, LAEUFT)
        busy = False
        while runner.poll() is None:
            # Erst die Registrierung löschen, dann stoppen: Die API lehnt das
            # Löschen eines beschäftigten Runners ab, so wird kein Job abgebrochen.
            if not busy and self.switch.paused():
                try:
                    deleted = self.github.delete_runner(self.repo, jit.runner_id)
                except ApiError as error:
                    _say(f"{self.name}: Registrierung nicht gelöscht ({error}), neuer Versuch")
                else:
                    if deleted:
                        runner.stop()
                        break
                    busy = True
            self.clock.sleep(_RUN_POLL)
        # Ephemer: kein Rest eines Jobs erreicht den nächsten.
        shutil.rmtree(slot_dir, ignore_errors=True)

    def _runner_name(self) -> str:
        return f"celsius-{self.name}-{uuid.uuid4().hex[:8]}"


def pause(switch: Switch, clock: Clock, timeout: float | None = None) -> int:
    """Schalter setzen und warten, bis kein Slot mehr einen Job haben kann.

    Nach einem Timeout bleibt der Schalter gesetzt; ``resume`` hebt ihn auf.
    """
    switch.ensure()
    with switch.lock():
        if switch.paused():
            # Trotzdem warten: Wer pausiert hat, wartet vielleicht selbst noch auf einen Slot.
            _say("schon pausiert")
        else:
            switch.engage()
    deadline = None if timeout is None else clock.monotonic() + timeout
    while busy := [slot for slot, word in switch.states().items() if word != PAUSIERT]:
        if deadline is not None and clock.monotonic() + _WAIT_POLL > deadline:
            _say(f"Timeout: noch nicht pausiert: {', '.join(busy)}")
            return 1
        clock.sleep(_WAIT_POLL)
    _say("pausiert: alle Slots ruhen")
    return 0


def resume(switch: Switch) -> int:
    switch.ensure()
    with switch.lock():
        if not switch.paused():
            _say("nicht pausiert, nichts zu tun")
            return 0
        switch.release()
    _say("fortgesetzt")
    return 0


class StatusSource(Protocol):
    def latest_runner_release(self) -> Release: ...


def status(switch: Switch, github: StatusSource, pinned_version: str, now: datetime) -> int:
    switch.ensure()
    _say(f"Schalter: {'gesetzt (pausiert)' if switch.paused() else 'aus'}")
    for slot, word in switch.states().items():
        _say(f"  {slot}: {word}")
    try:
        release = github.latest_runner_release()
    except ApiError as error:
        # 401 heißt meist: PAT abgelaufen oder widerrufen.
        _say(f"WARNUNG: GitHub nicht erreichbar ({error})")
        return 1
    _say(f"Runner: {pinned_version} (neuestes Release {release.version})")
    if _version(release.version) > _version(pinned_version):
        # Veraltete Runner nimmt GitHub irgendwann nicht mehr an (README: monatlich anheben).
        _say(f"WARNUNG: neueres Runner-Release {release.version}, Distribution anheben")
    if release.token_expiration is None:
        # Der Plan verlangt ein PAT mit Ablaufdatum; ohne Header ist es ein anderes.
        _say("WARNUNG: kein Ablaufdatum des PAT gemeldet")
        return 0
    _say(f"PAT läuft ab: {release.token_expiration}")
    expires = datetime.strptime(release.token_expiration.replace("UTC", "+0000"), "%Y-%m-%d %H:%M:%S %z")
    if expires - now < _PAT_WARN:
        _say("WARNUNG: PAT läuft in weniger als 14 Tagen ab, neues PAT anlegen (README)")
    return 0


def _version(text: str) -> tuple[int, ...]:
    return tuple(int(part) for part in text.removeprefix("v").split("."))


class ConfigError(Exception):
    pass


def _require(value: str | Path | None, name: str) -> str:
    if value is None:
        raise ConfigError(f"{name} ist nicht gesetzt (siehe github-runner.env.example)")
    return str(value)


def _read_token(path: Path) -> str:
    try:
        return path.read_text().strip()
    except OSError as error:
        # Nur Pfad und Fehlerart, nie Inhalt.
        raise ConfigError(f"PAT-Datei {path} nicht lesbar ({error.strerror})") from error


def _github(config: Config) -> GitHubApi:
    return GitHubApi(http=UrllibHttp(), token=_read_token(config.token_file), owner=config.owner)


def _run_slot(config: Config, repo: str, number: int) -> int:
    dist = Path(_require(config.dist_dir, "GH_RUNNER_DIST"))
    docker_host = _require(config.docker_host, "GH_RUNNER_DOCKER_HOST")
    slot = Slot(
        repo=repo,
        number=number,
        switch=Switch(config.state_dir),
        github=_github(config),
        launcher=HostLauncher(dist_dir=dist, user=config.user, home=config.home, docker_host=docker_host),
        clock=SystemClock(),
    )
    _say(f"{slot.name}: Slot startet (Runner {dist.name}, {docker_host})")
    while True:
        slot.run_once()


def _run_status(config: Config) -> int:
    version = _require(config.runner_version, "GH_RUNNER_DIST")
    return status(Switch(config.state_dir), _github(config), pinned_version=version, now=datetime.now(UTC))


def main(argv: list[str] | None = None, env: Mapping[str, str] = os.environ) -> int:
    parser = argparse.ArgumentParser(prog="github_runner.py", description=__doc__)
    commands = parser.add_subparsers(dest="command", required=True)
    slot_cmd = commands.add_parser("slot", help="Slot-Schleife für systemd (endet nicht)")
    slot_cmd.add_argument("repo", help="swarmq oder swarmv")
    slot_cmd.add_argument("number", type=int)
    pause_cmd = commands.add_parser("pause", help="Schalter setzen, warten bis alle Slots ruhen")
    pause_cmd.add_argument("--timeout", type=float, default=None, help="höchstens so viele Sekunden warten")
    commands.add_parser("resume", help="Schalter entfernen")
    commands.add_parser("status", help="Schalter, Slots, Runner-Version und PAT-Ablauf")
    args = parser.parse_args(argv)
    config = Config.from_env(env)
    switch = Switch(config.state_dir)
    try:
        if args.command == "slot":
            return _run_slot(config, args.repo, args.number)
        if args.command == "status":
            return _run_status(config)
    except ConfigError as error:
        sys.stderr.write(f"{error}\n")
        return 2
    if args.command == "pause":
        return pause(switch, SystemClock(), timeout=args.timeout)
    return resume(switch)


if __name__ == "__main__":
    sys.exit(main())
