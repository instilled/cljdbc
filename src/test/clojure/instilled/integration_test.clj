(ns instilled.integration-test
  (:require
    [instilled.cljdbc  :refer :all]
    [clojure.java.jdbc :as    jdbc]
    [clojure.test      :refer :all]))

(def conn "jdbc:mysql://localhost:3306/cljdbc_test?user=root")

(defn prepare-db
  [conn]
  (jdbc/db-do-commands
    conn
    ["DROP TABLE IF EXISTS planet"
     "CREATE TABLE planet (
     id BIGINT NOT NULL AUTO_INCREMENT,
     system VARCHAR(30) NOT NULL,
     name VARCHAR(30) NOT NULL,
     mass DECIMAL(65,30) NOT NULL,
     PRIMARY KEY (id))"]))

(defn test-crud
  [conn c r u d]
  (testing "create"
    (let [qs (parse-statement c)]
      (testing "returning keys"
        ;; single
        (is (= [{:generated_key 1}]
               (insert!
                 conn
                 qs
                 {:system "Sun System"
                  :name "Earth"
                  :mass 5.972E24}
                 {:return-keys true})))

        ;; multi
        (is (= [{:generated_key 2} {:generated_key 3}]
               (insert!
                 conn
                 qs
                 [{:system "Sun System"
                   :name "Mars"
                   :mass 6.39E23}
                  {:system "Sun System"
                   :name "Venus"
                   :mass 4.867E24}]
                 {:return-keys true}))))

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
        (is (= [1 1]
               (insert!
                 conn
                 qs
                 [{:system "PSR 1257+12"
                   :name "B"
                   :mass -1}
                  {:system "PSR 1257+12"
                   :name "C"
                   :mass -1}]))))))

  (testing "read"
    (let [qs (parse-statement r)]
      (is (< 0
            (count (query conn qs {:system "Sun System"}))))))

  (testing "update"
    (let [qs (parse-statement u)]
      (is (= 1
             (update!
               conn
               qs
               {:system "PSR 1257+12"
                :name "A"
                :to-name "X"})))
      (is (= [1 2]
             (update!
               conn
               qs
               [{:system "PSR 1257+12"
                 :name "B"
                 :to-name "X"}
                {:system "PSR 1257+12"
                 :name "X"
                 :to-name "Z"}])))))

  (testing "delete"
    (let [qs (parse-statement d)]
      (is (= 2
             (delete!
               conn
               qs
               {:name "Z"})))
      (is (= [1 0]
             (delete!
               conn
               qs
               [{:name "Earth"}
                {:name "???"}]))))))

(deftest ^:integration ^:mysql test-mysql-crud
  (prepare-db conn)
  (test-crud conn
    "insert into planet (system, name, mass) values (:system,:name,:mass)"
    "select * from planet where system = :system"
    "update planet set name = :to-name where system = :system and name = :name"
    "delete from planet where name = :name"))
