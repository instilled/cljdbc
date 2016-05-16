(ns instilled.integration-test
  (:require
    [instilled.cljdbc           :refer :all]
    [clojure.test               :refer :all]))

(def conn "jdbc:mysql://localhost:3306/cljdbc_test?user=root")

;; mysql test table:
;;CREATE TABLE planet (
;;     id BIGINT NOT NULL AUTO_INCREMENT,
;;     system VARCHAR(30) NOT NULL,
;;     name VARCHAR(30) NOT NULL,
;;     mass DECIMAL NOT NULL,
;;     PRIMARY KEY (id)
;;);

(deftest ^:integration ^:mysql test-mysql-crud
  (testing "create"
    (let [qs (parse-statement "insert into planet (system, name, mass) values (:system,:name,:mass)")]
      ;; single
      #_(is (= {:generated_key 1}
               (insert!
                 conn
                 qs
                 {:system "Sun System"
                  :name "Earth"
                  ;;:mass 5.972E24
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
               {:return-keys ["id"]})))))

  (testing "read"
    (let [qs (parse-statement "select * from planet where system = :system")]
      (is (< 0
            (count (query conn qs {:system "Sun System"})))))))
