FROM mysql:8.0.36-debian
RUN apt-get update 
RUN apt-get install -y \
    python3-full \
    python3-pip 
RUN pip install --break-system-packages --upgrade  pip
COPY server.py deploy.sh requirements.txt           /docker-entrypoint-initdb.d/
RUN pip install --break-system-packages -r  /docker-entrypoint-initdb.d/requirements.txt
ENV MYSQL_ROOT_PASSWORD="rootFFF"
ENV MYSQL_USER="Kayden"
ENV MYSQL_PASSWORD="ds@1234"
ENV MYSQL_DATABASE="StudentDB"