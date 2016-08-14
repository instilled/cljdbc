(ns instilled.cljdbc.integration.mysql
  (:require
    [instilled.cljdbc                  :refer :all]
    [instilled.cljdbc.integration.base :refer :all]
    [clojure.java.jdbc                 :as    jdbc]
    [clojure.test                      :refer :all]))

(deftest ^:integration ^:mysql test-mysql-crud
  (let [;; running in docker
        conn (make-connection
               (format "jdbc:mysql://%s:%s/%s?user=%s&password=%s"
                 (or (env :db.mysql.host) "localhost")
                 (or (env :db.mysql.port) "3306")
                 (or (env :db.mysql.name) "cljdbc")
                 (or (env :db.mysql.user) "cljdbc")
                 (or (env :db.mysql.pass) "cljdbc"))
               {:hikari {}})]
    (jdbc/db-do-commands
      {:connection (get-connection conn nil)}
      ["DROP TABLE IF EXISTS planet"
       "CREATE TABLE planet (
         id     BIGINT NOT NULL AUTO_INCREMENT,
         system VARCHAR(30) NOT NULL,
         name   VARCHAR(30) NOT NULL,
         mass   DECIMAL(65,30) NOT NULL,
         PRIMARY KEY (id))"])
    (apply basic-crud conn (crud-queries))))
