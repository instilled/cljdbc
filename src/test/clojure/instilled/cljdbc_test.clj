(ns instilled.cljdbc-test
  (:require
    [instilled.cljdbc :refer :all]
    [clojure.test     :refer :all]))

(deftest test-parse-statement
  (testing "nothing to replace"
    (let [qs (parse-statement "select * from table1")]
      (is (nil? (:params-idx qs)))
      (is (= "select * from table1" (:sql qs)))))

  (testing "named params-idx"
    (let [qs (parse-statement "select * from table1 where col1 = :col1-val and col2 = :col2-val and col3 = :col1-val")]
      (is (= {:col1-val [1 3]
              :col2-val [2]}
             (:params-idx qs)))
      (is (= "select * from table1 where col1 = ? and col2 = ? and col3 = ?"
             (:sql qs)))))

  (testing "positional params-idx"
    (let [qs (parse-statement "select * from table1 where col1 = :? and col2 = :?")]
      (is (= {:? [1 2]}
             (:params-idx qs)))
      (is (= "select * from table1 where col1 = ? and col2 = ?"
             (:sql qs)))))

  (testing "mixed named & positional params-idx"
    (let [qs (parse-statement "select * from table1 where col1 = :? and col2 = :col2-val and col3 = :col3-val")]
      (is (= {:? [1]
              :col2-val [2]
              :col3-val [3]}
             (:params-idx qs)))
      (is (= "select * from table1 where col1 = ? and col2 = ? and col3 = ?"
             (:sql qs))))))
