(ns instilled.cljdbc.integration.base
  (:require
    [instilled.cljdbc  :refer :all]
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

(defn basic-crud
  [conn [c co] [r ro] [u uo] [d do]]
  (testing "insert"
    (let [rs (insert!
               conn
               (parse-statement c co)
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
    (let [qs (parse-statement "select * from planet")
          rs (query conn qs)]
      (is (= 100
             (count rs)))
      (is (= "System 0"
             (-> rs first :system)))
      (is (= "System 99"
             (-> rs last :system)))))
  (testing "update"
    (let [rs (update!
               conn
               (parse-statement "update planet set mass = :mass")
               {:mass 10})
          rs2 (query
                conn
                (parse-statement "select * from planet where mass = :mass")
                {:mass 10})]
      ;; TODO: better test
      (is rs)
      (is (< 9.99 (-> rs2 first :mass)))
      (is (= 100 (count rs2)))))
  (testing "delete"
    (let [rs (delete!
               conn
               (parse-statement "delete from planet"))]
      (is (empty?
            (query
              conn
              (parse-statement "select * from planet")))))))
