# Vergleichs-Image für einen entfernten Messhost (scripts/vergleich-entfernt.sh).
#
# Enthält das Repo eines Commits samt Git-Historie (für die A/B-Messung), die
# fünf Vergleichsumgebungen aus einrichten.sh und die Docker-CLI mit Compose,
# mit der der Lauf seine Dienste über den Socket des Hosts startet. Auf dem
# Host liegt nichts außer Image und Container.
#
# Python und uv wie der CI-Job unit-tests (.github/workflows/ci.yml); Docker-CLI wie der
# Daemon auf celsius.
#
# Basis-Images per Digest gepinnt (Scorecard Pinned-Dependencies); Dependabot
# hebt Tag und Digest gemeinsam an (.github/dependabot.yml).
FROM docker:29.9.2-cli@sha256:5e6baf67325a79f64ed3bed6863954d5e05d0f3d4e57cbdb14809346814bbfdb AS docker

FROM ghcr.io/astral-sh/uv:0.11.6-python3.14-trixie@sha256:f3b1d933a49112e15fb02a24f9bfc8fef9986351afc601fcaba4c01a9b6a0eac
COPY --from=docker /usr/local/bin/docker /usr/local/bin/docker
COPY --from=docker /usr/local/libexec/docker/cli-plugins/docker-compose /usr/local/libexec/docker/cli-plugins/docker-compose

ENV UV_LINK_MODE=copy UV_FROZEN=1
WORKDIR /repo
COPY . /repo
RUN benchmarks/comparison/einrichten.sh \
    && uv sync --no-default-groups --group messung

ENTRYPOINT ["benchmarks/comparison/im-container.sh"]
