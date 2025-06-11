FROM python:3.12

USER root

LABEL org.opencontainers.image.description "A configurable router for Azure Service Bus."

# hadolint ignore=DL3008
RUN apt-get update \
  && apt-get install --no-install-recommends --yes bind9-dnsutils ncat \
  && apt-get clean \
  && rm -rf /var/lib/apt/lists/* \
  && addgroup --gid 1000 appuser \
  && adduser \
    --uid 1000 \
    --gid 1000 \
    --comment 'Application User' \
    --shell /usr/sbin/nologin appuser

USER appuser
WORKDIR /home/appuser
ENV ROUTER_PROMETHEUS_PORT=8000
ENV PYTHONPATH=/home/appuser
HEALTHCHECK --interval=30s --timeout=30s --start-period=10s --retries=3 CMD [ "/bin/sh", "-c", "curl --fail localhost:${ROUTER_PROMETHEUS_PORT}" ]

COPY --chown=appuser:appuser requirements.txt /home/appuser/requirements.txt
RUN pip install --no-cache-dir -r requirements.txt --user
COPY --chown=appuser:appuser --chmod=0644 rule-schema.json /home/appuser/rule-schema.json
COPY --chown=appuser:appuser --chmod=0755 router.py /home/appuser/router.py
COPY --chown=appuser:appuser --chmod=0755 nukedlq.py /home/appuser/nukedlq.py

ENTRYPOINT [ "/home/appuser/router.py" ]
