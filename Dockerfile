FROM python:3.10-slim

WORKDIR /app
ADD requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

ADD server.py /app/server.py

ENTRYPOINT [ "python", "server.py" ]