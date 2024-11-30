FROM python:3.12

USER root

RUN addgroup --gid 1000 appuser \
  && adduser \
    --uid 1000 \
    --gid 1000 \
    --comment 'Application User' \
    --shell /usr/sbin/nologin appuser

USER appuser
WORKDIR /home/appuser

COPY --chown=appuser:appuser requirements.txt /home/appuser/requirements.txt
COPY --chown=appuser:appuser router.py /home/appuser/router.py

RUN pip install --no-cache-dir -r requirements.txt --user

ENTRYPOINT [ "/home/appuser/router.py" ]
