# docker build zt .
# docker run -v $PWD/..:/z zt
FROM ubuntu

ENV DEBIAN_FRONTEND=noninteractive


RUN apt-get update -y # what
RUN apt-get install -q -y postgresql mysql-client mysql-server sqlite3 sudo vim curl

RUN apt-get install -q -y gnupg

RUN curl -sL https://deb.nodesource.com/setup_8.x | sudo -E bash -
RUN apt-get install -y nodejs

EXPOSE 3306
EXPOSE 5432

RUN echo "host all all 127.0.0.1/32 trust\nlocal all all trust" > /etc/postgresql/10/main/pg_hba.conf
RUN sed -e "s/.*listen_addresses\s*=\s*.*/listen_addresses = */"     /etc/postgresql/10/main/postgresql.conf

RUN service postgresql start && createdb -U postgres alfa

# ENTRYPOINT /z/test
ENV PGUSER=postgres
ENV ZAZLER_HOME=/z/app.js

RUN npm i express cookie-parser express-form-data md5 mocha parsimmon wildcard sqlite pg mysql request request-promise mocha
VOLUME /z
ADD pg.sql pg.sql
ENV PGDATABASE=alfa
RUN service postgresql start && psql -1f pg.sql
CMD service postgresql start && cd /z && npm test

