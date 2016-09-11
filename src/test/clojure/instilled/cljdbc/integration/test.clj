(ns instilled.cljdbc.integration.test
  (:require
    [instilled.cljdbc                  :as jdbc]
    [instilled.cljdbc.integration.base :refer :all]
    [clojure.java.jdbc                 :as clojure.jdbc]
    [clojure.test                      :refer :all]))

(defn prepare-and-run
  [jdbc-url ddl]
  (let [ds (jdbc/make-datasource jdbc-url {:hikari {}})]
    (try
      (jdbc/with-connection [conn ds]
        (clojure.jdbc/db-do-commands
          {:connection (jdbc/lift-connection conn)}
          ddl))
      (basic-crud-tests ds)
      (statement-option-test ds)
      (transaction-tests ds)
      (finally
        (jdbc/close ds)))))

(deftest ^:integration ^:mysql test-mysql
  (prepare-and-run
    (format "jdbc:mysql://%s:%s/%s?user=%s&password=%s&useSSL=false"
      (or (env :db.mysql.host) "localhost")
      (or (env :db.mysql.port) "3306")
      (or (env :db.mysql.name) "cljdbc")
      (or (env :db.mysql.user) "cljdbc")
      (or (env :db.mysql.pass) "cljdbc"))
    ["DROP TABLE IF EXISTS planet"
     "CREATE TABLE planet (
       id     BIGINT NOT NULL AUTO_INCREMENT,
       system VARCHAR(30) NOT NULL,
       name   VARCHAR(30) NOT NULL,
       mass   DECIMAL(65,30) NOT NULL,
       PRIMARY KEY (id))"]))

(deftest ^:integration ^:oracle test-oracle
  (prepare-and-run
    (format "jdbc:oracle:thin:%s/%s@%s:%s:xe"
      (or (env :db.oracle.user) "cljdbc")
      (or (env :db.oracle.pass) "cljdbc")
      (or (env :db.oracle.host) "localhost")
      (or (env :db.oracle.port) "49161"))
    ["BEGIN
       EXECUTE IMMEDIATE 'DROP TABLE planet CASCADE CONSTRAINTS PURGE';
       EXECUTE IMMEDIATE 'DROP SEQUENCE pk_planet_seq';
      EXCEPTION
         WHEN OTHERS THEN
            IF SQLCODE = -1 THEN
               RAISE;
            END IF;
      END;"
     "CREATE TABLE planet (
       id     NUMBER(5) PRIMARY KEY,
       system VARCHAR2(30) NOT NULL,
       name   VARCHAR2(30) NOT NULL,
       mass   DECIMAL(38,30) NOT NULL)"
     "CREATE SEQUENCE pk_planet_seq"
     "CREATE OR REPLACE TRIGGER trig_planet_pk_seq
         BEFORE INSERT ON planet
         FOR EACH ROW

         BEGIN
           SELECT pk_planet_seq.NEXTVAL
           INTO   :new.id
           FROM   dual;
       END;"]))

(deftest ^:integration ^:postgres test-postgres
  (prepare-and-run
    (format "jdbc:postgresql://%s:%s/%s?user=%s&password=%s"
      (or (env :db.postgres.host) "localhost")
      (or (env :db.postgres.port) "5432")
      (or (env :db.postgres.name) "cljdbc")
      (or (env :db.postgres.user) "cljdbc")
      (or (env :db.postgres.pass) "cljdbc"))
    ["DROP TABLE IF EXISTS planet;"
     "CREATE TABLE planet (
      id     SERIAL         PRIMARY KEY,
      system VARCHAR(30)    NOT NULL,
      name   VARCHAR(30)    NOT NULL,
      mass   NUMERIC(38,30) NOT NULL);"]))
