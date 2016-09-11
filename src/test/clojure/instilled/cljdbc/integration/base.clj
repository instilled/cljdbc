(ns instilled.cljdbc.integration.base
  (:require
    [instilled.cljdbc  :as jdbc]
    [clojure.test      :refer :all]))

(def ^:dynamic *global-conn*
  nil)

(defn basic-crud-tests
  [ds]
  (testing "binding"
    (jdbc/with-connection-binding [*global-conn* ds]
      (let [rs (jdbc/query *global-conn*
                 (jdbc/parse-statement "select * from planet"))]
        (is (empty? rs)))))
  (jdbc/with-connection [conn ds]
    (testing "insert"
      (let [rs (jdbc/insert!
                 conn
                 (jdbc/parse-statement
                   "insert into planet (system,name,mass) values (:system,:name,:mass)"
                   {:returning [:id :system]
                    :batched? true})
                 (mapv (fn [i]
                         {:system (str "System " i)
                          :name   "Planet"
                          :mass   1})
                   (range 0 100)))]
        (dotimes [n 100]
          (is (= (+ 1 n)
                 (let [r (nth rs n)]
                   (long (:id r))))))))

    (testing "query"
      (let [qs (jdbc/parse-statement "select * from planet")
            rs (jdbc/query conn qs)]
        (is (= 100
               (count rs)))
        (is (= "System 0"
               (-> rs first :system)))
        (is (= "System 99"
               (-> rs last :system)))))

    (testing "update"
      (let [rs (jdbc/update!
                 conn
                 (jdbc/parse-statement "update planet set mass = :mass")
                 {:mass 10})
            rs2 (jdbc/query
                  conn
                  (jdbc/parse-statement "select * from planet where mass = :mass")
                  {:mass 10})]
        ;; TODO: better test
        (is rs)
        (is (< 9.99 (-> rs2 first :mass)))
        (is (= 100 (count rs2)))))

    (testing "delete"
      (let [rs (jdbc/delete!
                 conn
                 (jdbc/parse-statement "delete from planet"))]
        (is (empty?
              (jdbc/query
                conn
                (jdbc/parse-statement "select * from planet"))))))

    (testing "insert - classic params"
      (let [rs (jdbc/insert!
                 conn
                 (jdbc/parse-statement
                   "insert into planet (system,name,mass) values (?,?,?)"
                   {:batched? true})
                 (mapv (fn [i]
                         [(str "System " i)
                          "Planet"
                          1])
                   (range 0 10)))]
        (is rs)))))

(defn statement-option-test
  [ds]
  (testing "illegal option valuese throw"
    (jdbc/with-connection [conn ds]
      (is (thrown?
            IllegalArgumentException
            (jdbc/query
              conn
              (jdbc/parse-statement "select * from planet")
              {}
              {:result-type -100
               :concurrency :read-only})))
      (is (thrown?
            IllegalArgumentException
            (jdbc/query
              conn
              (jdbc/parse-statement "select * from planet")
              {}
              {:result-type :forward-only
               :concurrency -100})))
      (is (thrown?
            IllegalArgumentException
            (jdbc/query
              conn
              (jdbc/parse-statement "select * from planet")
              {}
              {:result-type :forward-only
               :concurrency :read-only
               :cursors -100})))
      (is (thrown?
            IllegalArgumentException
            (jdbc/query
              conn
              (jdbc/parse-statement "select * from planet")
              {}
              {:fetch-size -100})))
      (is (thrown?
            IllegalArgumentException
            (jdbc/query
              conn
              (jdbc/parse-statement "delete from planet")
              {}
              {:max-rows -100})))
      (is (thrown?
            IllegalArgumentException
            (jdbc/query
              conn
              (jdbc/parse-statement "delete from planet")
              {}
              {:timeout -100}))))))

(defn transaction-tests
  [ds]
  (letfn [(transaction-state
            [conn]
            (-> (jdbc/get-transaction conn)
                :ctx (deref)))
          (savepoint-count
            [conn]
            (-> (jdbc/get-transaction conn)
                :ctx
                (deref)
                :savepoints
                (count)))]
    (testing "transaction without options"
      (jdbc/with-connection [conn ds]
        (= {:read-only? false
            :auto-commit true
            :savepoints (list)
            :depth 0}
           (-> (transaction-state conn)
               (select-keys [:read-only? :auto-commit
                             :savepoints :depth])))))

    (testing "transaction with options"
      (jdbc/with-connection [conn ds] {:read-only? true}
        (= {:auto-commit true
            :savepoints (list)
            :depth 0}
           (-> (transaction-state conn)
               (select-keys [:auto-commit :savepoints :depth])))
        (is (= true
               (-> (jdbc/lift-connection conn)
                   (.isReadOnly))))))

    (testing "transaction with illegal option values throw"
      (is (thrown?
            IllegalArgumentException
            (jdbc/with-connection [conn ds] {:isolation :wrong}))))

    (testing "nested transaction"
      (jdbc/with-connection [conn ds]
        (is (= {:auto-commit true
                :savepoints (list)
                :depth 0}
               (-> (transaction-state conn)
                   (select-keys [:auto-commit :savepoints :depth]))))
        (jdbc/with-transaction [conn conn]
          (is (= {:auto-commit true
                  :depth 1}
                 (-> (transaction-state conn)
                     (select-keys [:auto-commit :depth]))))
          (is (= 1
                 (savepoint-count conn)))
          (jdbc/with-transaction [conn conn]
            (is (= {:depth 2}
                   (-> (transaction-state conn)
                       (select-keys [:depth]))))
            (is (= 2
                   (savepoint-count conn))))
          (is (= 1
                 (savepoint-count conn))))
        (is (= {:auto-commit true
                :savepoints (list)
                :depth 0}
               (-> (transaction-state conn)
                   (select-keys [:auto-commit :savepoints :depth]))))))

    (testing "queries"
      (jdbc/with-connection [conn ds]
        (jdbc/delete! conn
          (jdbc/parse-statement "delete from planet"))
        (jdbc/insert! conn
          (jdbc/parse-statement
            "insert into planet (system,name,mass) values (:system,:name,:mass)"
            {:batched? true})
          (mapv (fn [i]
                  {:system (str "System " i)
                   :name   "Planet"
                   :mass   1})
            (range 0 100)))
        (is (= 100
               (->> (jdbc/parse-statement "select * from planet")
                    (jdbc/query conn)
                    (count)))))

      (jdbc/with-connection [conn ds]
        (try
          (jdbc/delete! conn
            (jdbc/parse-statement "delete from planet"))
          (jdbc/insert! conn
            (jdbc/parse-statement
              "insert into planet (system,name,mass) values (:system,:name,:mass)")
            {:system "Virgo" :name "Draugr" :mass -1})
          (jdbc/with-transaction [conn conn]
            (jdbc/insert! conn
              (jdbc/parse-statement
                "insert into planet (system,name,mass) values (:system,:name,:mass)"
                {:batched? true})
              (mapv (fn [i]
                      {:system (str "System " i)
                       :name   "Planet"
                       :mass   1})
                (range 0 100)))
            ;; This should rollback only the inner transaction (insert)
            (throw (IllegalStateException. "expected")))
          (catch Throwable t
            (is (= 1
                   (->> (jdbc/parse-statement "select * from planet")
                        (jdbc/query conn)
                        (count))))))))))
