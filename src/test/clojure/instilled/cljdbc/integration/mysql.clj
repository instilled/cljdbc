(ns instilled.cljdbc.integration.mysql
  (:require
    [instilled.cljdbc                  :as jdbc]
    [instilled.cljdbc.integration.base :refer :all]
    [clojure.java.jdbc                 :as clojure.jdbc]
    [clojure.test                      :refer :all]))

(deftest ^:integration ^:mysql test-mysql-crud
  (let [;; running in docker
        ds (jdbc/make-datasource
             (format "jdbc:mysql://%s:%s/%s?user=%s&password=%s"
               (or (env :db.mysql.host) "localhost")
               (or (env :db.mysql.port) "3306")
               (or (env :db.mysql.name) "cljdbc")
               (or (env :db.mysql.user) "cljdbc")
               (or (env :db.mysql.pass) "cljdbc"))
             {:hikari {}})]
    (try
      (jdbc/with-connection [conn ds]
        (clojure.jdbc/db-do-commands
          {:connection (jdbc/lift-connection conn)}
          ["DROP TABLE IF EXISTS planet"
           "CREATE TABLE planet (
           id     BIGINT NOT NULL AUTO_INCREMENT,
           system VARCHAR(30) NOT NULL,
           name   VARCHAR(30) NOT NULL,
           mass   DECIMAL(65,30) NOT NULL,
           PRIMARY KEY (id))"]))
      (apply basic-crud-tests ds (crud-queries))
      (statement-option-test ds)
      (transaction-tests ds)
      (finally
        (jdbc/close ds)))))
