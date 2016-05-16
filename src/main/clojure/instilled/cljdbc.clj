(ns instilled.cljdbc
  (:require
    [clojure.java.jdbc     :as jdbc])
  (:import
    [java.sql
     PreparedStatement]))

(defrecord QuerySpec [sql params-idx])

(defn parse-statement
  "Parse a templateized sql-string possibly with named params into `QuerySpec`. Both, named and sequential params are supported. They must all be prefixed with `:`, e.g. `:?` for positional and `:named-param` for named params."
  [sql-str]
  (let [m (re-matcher #":(\w+-\w+|\w+|\?)" sql-str)]
    (loop [i 1 params-idx nil]
      (if-let [f (and (.find m) (.group m 1))]
        (recur
          (inc i)
          (update
            params-idx
            (keyword f)
            #(if (vector? %1) (conj %1 %2) [%2])
            i))
        (QuerySpec. (.replaceAll m "?") params-idx)))))

(defn set-prepared-statement-params!
  "Set the `stmnt`'s `params` based on `query-spec`."
  [^PreparedStatement stmnt query-spec params]
  (doseq [[k v] params i (get-in query-spec [:params-idx k])]
    (jdbc/set-parameter v stmnt i))
  stmnt)

(defn create-prepared-statement
  [conn query-spec options]
  #_(.prepareStatement
      (jdbc/db-find-connection conn)
      (:sql query-spec)
      (int-array 1 [0]))
  (jdbc/prepare-statement
    (jdbc/db-find-connection conn)
    (:sql query-spec)
    (or options {})))

(defn execute-prepared-statement
  [exec-fn ^PreparedStatement stmnt query-spec & [params {:keys [return-keys] :as options}]]
  (let [;; may not be supported by all vendors
        ret-ks  (^{:once true} fn* [stmnt alt-ret]
                 (try
                   (let [rs (.getGeneratedKeys stmnt)]
                     (doall (jdbc/result-set-seq rs)))
                   (catch Exception _ alt-ret)))
        ret-cnt (^{:once true} fn* [stmnt alt-ret] alt-ret)
        ret-fn  (if return-keys ret-ks ret-cnt)]
    (ret-fn stmnt (exec-fn stmnt))))


;; #######################################
;; Public API

(defn query
  "Query the database given `query-spec`."
  [conn query-spec & [params options]]
  (jdbc/with-db-connection [conn conn]
    (let [stmnt (-> (create-prepared-statement conn query-spec options)
                    (set-prepared-statement-params! query-spec params))]
      (jdbc/query conn [stmnt] (or options {})))))

(defn insert!
  "Insert a single record into the database, returing the inserted key (if supported)."
  [conn query-spec & [params {:keys [return-keys] :as options}]]
  (jdbc/with-db-connection [conn conn]
    (let [stmnt (create-prepared-statement conn query-spec options)]
      (try
        (let [exec-batch (^{:once true} fn* [stmnt] (.executeBatch stmnt))
              exec-single (^{:once true} fn* [stmnt] (.executeUpdate stmnt)) ]
          (if (vector? params)
            (do
              (doseq [params params]
                (set-prepared-statement-params! stmnt query-spec params)
                (.addBatch stmnt))
              (execute-prepared-statement exec-batch stmnt query-spec params options))
            (do
              (set-prepared-statement-params! stmnt query-spec params)
              (execute-prepared-statement exec-single stmnt query-spec params options))))
        (catch Exception _
          (.close stmnt))))))

(comment
  ;; example usage
  (def my-query-spec (parse-statement
                       "SELECT id, system, name, mass FROM planet WHERE system = :system"))
  (def conn "jdbc:mysql://localhost:3306/cljdbc_test?user=root")
  (query conn my-query-spec {:system "Sun System"}))
