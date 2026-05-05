FROM python:3.10-slim-bookworm

ARG SALT_VERSION=3006.9

ENV DEBIAN_FRONTEND=noninteractive \
    PIP_NO_CACHE_DIR=1

RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        bash \
        ca-certificates \
        curl \
        gcc \
        iproute2 \
        libffi-dev \
        libssl-dev \
        libzmq3-dev \
        make \
        procps \
        python3-dev \
        sudo \
        tini \
    && python -m pip install --upgrade pip setuptools wheel \
    && python -m pip install \
        "salt==${SALT_VERSION}" \
        cherrypy \
        cryptography \
        distro \
        jinja2 \
        looseversion \
        msgpack \
        psutil \
        pyOpenSSL \
        pyzmq \
        PyYAML \
        python-gnupg \
        requests \
        setproctitle \
        tornado \
    && useradd --create-home --shell /bin/bash saltapi \
    && echo 'saltapi:saltapi' | chpasswd \
    && mkdir -p /etc/salt/master.d /etc/salt/minion.d /srv/salt /srv/pillar /var/cache/salt /var/log/salt /var/run/salt \
    && apt-get purge -y --auto-remove gcc make python3-dev libffi-dev libssl-dev \
    && rm -rf /var/lib/apt/lists/*

ENTRYPOINT ["/usr/bin/tini", "--"]
