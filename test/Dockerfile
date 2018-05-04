# docker build zt .
# docker run $PWD/..:/z zt
FROM ubuntu

ENV DEBIAN_FRONTEND=noninteractive

RUN apt-get update -y # what
RUN apt-get install -q -y postgresql mysql-server

EXPOSE 3306
EXPOSE 5432

RUN apt-get install sudo
RUN apt-get install -y vim curl
RUN apt-get install -y mysql-client

RUN echo "host all all 127.0.0.1/32 trust" > /etc/postgresql/9.5/main/pg_hba.conf
RUN echo "local all all trust" >> /etc/postgresql/9.5/main/pg_hba.conf
RUN sed -e "s/.*listen_addresses\s*=\s*.*/listen_addresses = */" /etc/postgresql/9.5/main/postgresql.conf

RUN service postgresql start && createdb -U postgres alfa

RUN curl -sL https://deb.nodesource.com/setup_8.x | sudo -E bash -
RUN apt-get install -y nodejs
RUN apt-get install -y sqlite3

# ENTRYPOINT /z/test
ENV PGUSER=postgres
ENV ZAZLER_HOME=/z/app.js
RUN npm i express  cookie-parser express-form-data md5 mocha parsimmon wildcard sqlite pg mysql
RUN npm i request
RUN npm i request-promise
VOLUME /z
ADD pg.sql pg.sql
ENV PGDATABASE=alfa
RUN service postgresql start && psql -1f pg.sql
CMD service postgresql start && cd /z && npm test

