(ns instilled.cljdbc.integration.base
  (:require
    [instilled.cljdbc  :as jdbc]
    [clojure.test      :refer :all]))

(defn env
  [k]
  (let [^String kstr (name k)]
    (or (System/getProperty kstr)
        (System/getenv (-> kstr (.replaceAll "(\\.|-)" "_") (.toUpperCase))))))

(defn crud-queries
  [ & [return-keys-naming-strategy]]
  (let [return-keys-naming-strategy
        (or return-keys-naming-strategy
            (fn [_] :id))]
    [["insert into planet (system, name, mass) values (:system,:name,:mass)"
      {:return-keys ["id"]
       :return-keys-naming-strategy return-keys-naming-strategy}]
     ["select * from planet where system = :system"]
     ["update planet set name = :to-name where system = :system and name = :name"]
     ["delete from planet where name = :name"]]))

(defn basic-crud-tests
  [ds [c co] [r ro] [u uo] [d do]]
  (jdbc/with-connection [conn ds]
    (testing "insert"
      (let [rs (jdbc/insert!
                 conn
                 (jdbc/parse-statement c co)
                 (mapv (fn [i]
                         {:system (str "System " i)
                          :name   "Planet"
                          :mass   1})
                   (range 0 100)))]
        (dotimes [n 100]
          (is (= (+ 1 n)
                 ;; TODO: naming of pk should be same for all
                 ;; dialects
                 (let [r (nth rs n)]
                   (long (or (:id r) (:generated_key r)))))))))

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
                (jdbc/parse-statement "select * from planet"))))))))

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
    (testing "transaction"
      (jdbc/with-connection [conn ds]
        (testing "without options"
          (jdbc/transactionally [conn conn]
            (= {:read-only? false
                :auto-commit true
                :savepoints (list)
                :depth 0}
               (-> (transaction-state conn)
                   (select-keys [:read-only? :auto-commit
                                 :savepoints :depth])))))
        (testing "with options"
          (jdbc/transactionally [conn conn] {:read-only? true}
            (= {:auto-commit true
                :savepoints (list)
                :depth 0}
               (-> (transaction-state conn)
                   (select-keys [:auto-commit :savepoints :depth])))
            (is (= true
                   (-> (jdbc/lift-connection conn)
                       (.isReadOnly))))))
        (testing "illegal option values throw"
          (is (thrown?
                IllegalArgumentException
                (jdbc/transactionally [conn conn] {:isolation :wrong}))))))

    (testing "nested transaction"
      (jdbc/with-connection [conn ds]
        (jdbc/transactionally [conn conn]
          (is (= {:auto-commit true
                  :savepoints (list)
                  :depth 0}
                 (-> (transaction-state conn)
                     (select-keys [:auto-commit :savepoints :depth]))))
          (jdbc/transactionally [conn conn]
            (is (= {:auto-commit true
                    :depth 1}
                   (-> (transaction-state conn)
                       (select-keys [:auto-commit :depth]))))
            (is (= 1
                   (savepoint-count conn)))
            (jdbc/transactionally [conn conn]
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
                     (select-keys [:auto-commit :savepoints :depth])))))))))
