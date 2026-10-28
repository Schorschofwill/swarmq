# Vergleichs-Image für einen entfernten Messhost (scripts/vergleich-entfernt.sh).
#
# Enthält das Repo eines Commits samt Git-Historie (für die A/B-Messung), die
# fünf Vergleichsumgebungen aus einrichten.sh und die Docker-CLI mit Compose,
# mit der der Lauf seine Dienste über den Socket des Hosts startet. Auf dem
# Host liegt nichts außer Image und Container.
#
# Python und uv wie der CI-Job unit-tests (.github/workflows/ci.yml); Docker-CLI wie der
# Daemon auf celsius.
FROM docker:29.9.2-cli AS docker

FROM ghcr.io/astral-sh/uv:0.11.6-python3.14-trixie
COPY --from=docker /usr/local/bin/docker /usr/local/bin/docker
COPY --from=docker /usr/local/libexec/docker/cli-plugins/docker-compose /usr/local/libexec/docker/cli-plugins/docker-compose

ENV UV_LINK_MODE=copy UV_FROZEN=1
WORKDIR /repo
COPY . /repo
RUN benchmarks/comparison/einrichten.sh \
    && uv sync --no-default-groups --group messung

ENTRYPOINT ["benchmarks/comparison/im-container.sh"]
