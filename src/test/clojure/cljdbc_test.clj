(ns cljdbc-test
  (:require
    [cljdbc       :refer :all]
    [clojure.test :refer :all]))

(deftest test-parse-statement
  (testing "nothing to replace"
    (let [qs (parse-statement "select * from table1")]
      (is (nil? (named-or-positional? qs)))
      (is (= "select * from table1" (:sql qs)))
      (is (= {:op :select :table "table1"} (:meta qs)))))

  (testing "named params-idx"
    (let [qs (parse-statement "select * from table1 where col1 = :col1-val and col2 = :col2-val and col3 = :col1-val")]
      (is (= {:col1-val [1 3]
              :col2-val [2]}
             (:params-idx qs)))
      (is (= "select * from table1 where col1 = ? and col2 = ? and col3 = ?"
             (:sql qs)))
      (is (= {:op :select :table "table1"} (:meta qs))) ))

  (testing "positional params-idx"
    (let [qs (parse-statement "update table1 set col1 = :? and col2 = :?")]
      (is (= {:? [1 2]}
             (:params-idx qs)))
      (is (= "update table1 set col1 = ? and col2 = ?"
             (:sql qs)))
      (is (= {:op :update :table "table1"} (:meta qs)))))

  (testing "mixed named & positional params-idx"
    (let [qs (parse-statement "insert into table1 (col1, col2, col3) values (:?, :col2-val, :col3-val)")]
      (is (= {:? [1]
              :col2-val [2]
              :col3-val [3]}
             (:params-idx qs)))
      (is (= "insert into table1 (col1, col2, col3) values (?, ?, ?)"
             (:sql qs)))
      (is (= {:op :insert :table "table1"} (:meta qs)))))

  (testing "delete"
    (let [qs (parse-statement "delete from table1")]
      (is (nil? (named-or-positional? qs)))
      (is (= "delete from table1" (:sql qs)))
      (is (= {:op :delete :table "table1"} (:meta qs)))))

  (testing "classic params (positional but with ? only)"
    (let [qs (parse-statement "delete from table1")]
      (is (nil? (named-or-positional? qs)))
      (is (= "delete from table1" (:sql qs)))
      (is (= {:op :delete :table "table1"} (:meta qs))))))
