FROM python:3.12

USER root

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

COPY --chown=appuser:appuser requirements.txt /home/appuser/requirements.txt
RUN pip install --no-cache-dir -r requirements.txt --user
COPY --chown=appuser:appuser --chmod=0644 rule-schema.json /home/appuser/rule-schema.json
COPY --chown=appuser:appuser --chmod=0755 router.py /home/appuser/router.py

ENTRYPOINT [ "/home/appuser/router.py" ]
