FROM python:3.11-slim

RUN apt-get update && apt-get install -y --no-install-recommends \
    default-jre-headless \
    curl \
    && rm -rf /var/lib/apt/lists/*

RUN pip install --no-cache-dir jaydebeapi JPype1

WORKDIR /app

# Bake the JDBC driver into the image so it never needs to download at runtime
RUN curl -fsSL -o /app/db2jcc4.jar \
    https://repo1.maven.org/maven2/com/ibm/db2/jcc/11.5.9.0/jcc-11.5.9.0.jar

COPY generate.py ./

CMD ["python", "generate.py"]
