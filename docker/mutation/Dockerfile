# Mutmut-Abbild für Mutation-Testing auf dem Lastserver (Plan
# 2026-10-17-0941, U2/KTD4). Gebaut und gestartet über den Docker-Kontext
# (`uv run just mutation-starten a|b`); auf dem Server liegt kein Checkout.
# Build-Kontext ist das Repo-Root, `.dockerignore` hält venv, .git und
# Caches draußen.
#
# Basis wie die Job-Container in .github/workflows/ci.yml (nicht slim: git für die
# Unit-Tests, die Repos in tmp_path anlegen).
#
# Basis-Images per Digest gepinnt (Scorecard Pinned-Dependencies); Dependabot
# hebt Tag und Digest gemeinsam an (.github/dependabot.yml).
FROM ghcr.io/astral-sh/uv:0.11.6-python3.14-trixie@sha256:f3b1d933a49112e15fb02a24f9bfc8fef9986351afc601fcaba4c01a9b6a0eac

ENV UV_FROZEN=1 \
    UV_LINK_MODE=copy \
    UV_PYTHON_DOWNLOADS=never \
    UV_COMPILE_BYTECODE=0 \
    PYTHONUNBUFFERED=1

WORKDIR /app

# Abhängigkeiten zuerst (eigene Schicht), dann das Projekt.
COPY pyproject.toml uv.lock README.md ./
RUN uv sync --frozen --extra test --extra reload --no-install-project

COPY . .
RUN uv sync --frozen --extra test --extra reload \
    && uv run --no-sync mutmut --version

COPY docker/mutation/entrypoint.sh /usr/local/bin/mutmut-entrypoint
RUN chmod 0755 /usr/local/bin/mutmut-entrypoint

ENTRYPOINT ["/usr/local/bin/mutmut-entrypoint"]
