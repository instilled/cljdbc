# Docker Database Setup

For oracle cd into `docker-oracle` then type

    docker build . -t cljdbc-oracle
    docker run --name cljdbc-oracle -d -p 49161:1521 cljdbc-oracle

You'll now be able to connect to it with

    user: cljdbc
    password: cljdbc
    host: localhost:49161

Things are similar for mysql. To get a mysql image running cd into
`docker-mysql` and type

    docker build . -t cljdbc-mysql
    docker run --name cljdbc-mysql -e MYSQL_ROOT_PASSWORD=cljdbc-root -d -p 3306:3306 cljdbc-mysql

Enjoy!
