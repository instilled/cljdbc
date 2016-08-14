(ns instilled.cljdbc.integration.oracle
  (:require
    [instilled.cljdbc                  :refer :all]
    [instilled.cljdbc.integration.base :refer :all]
    [clojure.java.jdbc                 :as    jdbc]
    [clojure.test                      :refer :all]))

(deftest ^:integration ^:oracle test-oracle-crud
  (let [;; running in docker
        conn (make-connection
               (format "jdbc:oracle:thin:%s/%s@%s:%s:xe"
                 (or (env :db.oracle.user) "cljdbc")
                 (or (env :db.oracle.pass) "cljdbc")
                 (or (env :db.oracle.host) "localhost")
                 (or (env :db.oracle.port) "49161"))
               {:hikari {}})]
    (jdbc/db-do-commands
      {:connection (get-connection conn nil)}
      [;;"delete planet"
       "BEGIN
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
               END;"])
    (apply basic-crud conn (crud-queries))))
