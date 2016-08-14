(ns instilled.cljdbc-integration-test
  (:require
    [instilled.cljdbc  :refer :all]
    [clojure.string    :as    str]
    [clojure.set       :refer [rename-keys]]
    [clojure.java.jdbc :as    jdbc]
    [clojure.test      :refer :all]))

(extend-protocol jdbc/IResultSetReadColumn
  java.math.BigDecimal
  (result-set-read-column [^java.math.BigDecimal x# _2 _3]
    (if (= (.getScale _2 _3)) (long x#) x#)))

(defn is-success
  [expected actual]
  (->> (map #(or (= %1 %2) (not (= java.sql.Statement/EXECUTE_FAILED %2))) expected actual)
       (reduce (fn [acc v] (and acc v)) true)))

(defn env
  [k]
  (let [^String kstr (name k)]
    (or (System/getProperty kstr)
        (System/getenv (-> kstr (.replaceAll "(\\.|-)" "_") (.toUpperCase))))))

(defn ^:private crud-queries
  [ & [return-keys-naming-strategy]]
  (let [return-keys-naming-strategy
        (or return-keys-naming-strategy
            (fn [_] :id))]
    [["insert into planet (system, name, mass) values (:system,:name,:mass)"
      {:return-keys-naming-strategy return-keys-naming-strategy}]
     ["select * from planet where system = :system"]
     ["update planet set name = :to-name where system = :system and name = :name"]
     ["delete from planet where name = :name"]]))

(defn basic-crud
  [conn [c co] [r ro] [u uo] [d do]]
  (testing "query"
    (testing "cursors"
      ;; populate with some data
      (insert!
        conn
        (parse-statement c co)
        (mapv (fn [i]
                {:system (str "System " i)
                 :name   "Planet"
                 :mass   1})
          (range 0 100)))
      (let [qs (parse-statement "select * from planet")
            rs (query conn qs)]
        (is (= 100
               (count rs)))
        (is (= "System 0"
               (-> rs first :system)))
        (is (= "System 99"
               (-> rs last :system)))))))

#_(defn extended-crud
    [conn [c co] [r ro] [u uo] [d do]]
    (testing "create"
      (let [qs (parse-statement c co)]
        (testing "returning keys"
          ;; single
          (is (= [{:id 1}]
                 (insert!
                   conn
                   qs
                   {:system "Sun System"
                    :name "Earth"
                    :mass 5.972}
                   {:return-keys ["id"]})))

          ;; multi
          (is (= [{:id 2} {:id 3}]
                 (insert!
                   conn
                   qs
                   [{:system "Sun System"
                     :name "Mars"
                     :mass 6.39}
                    {:system "Sun System"
                     :name "Venus"
                     :mass 4.867}]
                   {:return-keys ["id"]}))))

        (testing "returning counts"
          ;; single
          (is (= 1
                 (insert!
                   conn
                   qs
                   {:system "PSR 1257+12"
                    :name "A"
                    :mass -1})))
          ;; multi
          (is-success
            [1 1]
            (insert!
              conn
              qs
              [{:system "PSR 1257+12"
                :name "B"
                :mass -1}
               {:system "PSR 1257+12"
                :name "C"
                :mass -1}])))))

    (testing "read"
      (let [qs (parse-statement r ro)]
        (is (< 0
              (count (query conn qs {:system "Sun System"}))))))

    (testing "update"
      (let [qs (parse-statement u uo)]
        (is (= 1
               (update!
                 conn
                 qs
                 {:system "PSR 1257+12"
                  :name "A"
                  :to-name "X"})))
        (is-success
          [1 2]
          (update!
            conn
            qs
            [{:system "PSR 1257+12"
              :name "B"
              :to-name "X"}
             {:system "PSR 1257+12"
              :name "X"
              :to-name "Z"}]))
        (is (= 2
               (update!
                 conn
                 qs
                 {:system "PSR 1257+12"
                  :name "Z"
                  :to-name "XYZ"})))))

    (testing "delete"
      (let [qs (parse-statement d do)]
        (is (= 2
               (delete!
                 conn
                 qs
                 {:name "XYZ"})))
        (is-success
          [1 0]
          (delete!
            conn
            qs
            [{:name "Earth"}
             {:name "???"}])))))

;;s.getConnection().getMetaData().getDriverMajorVersion()
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