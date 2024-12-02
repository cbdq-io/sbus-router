FROM python:3.12

RUN pip install python-qpid-proton
COPY is_sbus_emulator_alive.py /usr/local/bin/is_sbus_emulator_alive.py

ENTRYPOINT [ "/usr/local/bin/is_sbus_emulator_alive.py" ]
