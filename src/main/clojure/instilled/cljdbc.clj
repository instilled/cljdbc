(ns instilled.cljdbc
  (:require
    [clojure.string        :as str]
    [clojure.java.jdbc     :as jdbc])
  (:import
    [java.sql
     ResultSet
     PreparedStatement
     Connection
     DriverManager]))

;; TODO: remove dependency to clojure.java.jdbc

(defprotocol IConnectionAware
  (get-connection
    [this]
    "Retrieve the connection. Based on the exending type this may result
     in new connections each time the function is being invoked. It is
     strongly recommended to pass a `javax.sql.DataSource` object to all
     functions taking a `conn`."))

(extend-type String
  IConnectionAware
  (get-connection
    [this]
    (DriverManager/getConnection this)))

(extend-type javax.sql.DataSource
  IConnectionAware
  (get-connection
    [this]
    (.getConnection ^javax.sql.DataSource this)))

(defprotocol IExecutionAspect
  (pre [conn query-spec params])
  (post [conn query-spec rs]))

(defrecord QuerySpec [sql params-idx options meta])

(defn execute?
  [query-spec]
  (case (get-in query-spec [:meta :op])
    :update true
    :insert true
    :execute true
    false))

(defn parse-statement
  "Parse a possibly parametrized sql-string into `QuerySpec`.
   Both, named and sequential params, are supported. Params
   must be prefixed with `:`, e.g. `:?` (positional)
   `:named-param` (named)."
  ([^String sql-str]
   (parse-statement sql-str nil))
  ([^String sql-str options ]
   (letfn [(extract-table-name
             [op sql-str]
             (case op
               :update (second (re-find #"(?i)update\s+(.*?)(\s+.*)?$" sql-str))
               :insert (second (re-find #"(?i)into\s+(.*?)(\s+.*)?$" sql-str))
               :delete (second (re-find #"(?i)from\s+(.*?)(\s+.*)?$" sql-str))
               :select (second (re-find #"(?i)from\s+(.*?)(\s+.*)?$" sql-str))
               (throw
                 (IllegalStateException.
                   (format "Failed to extract table name from sql: %s"
                     sql-str)))))]
     (let [^java.util.regex.Matcher m (re-matcher #":(\w+(-|_)\w+|\w+|\?)" sql-str)
           op (if-let [type (first (re-find #"(?mi)^(update|delete|insert|select)" sql-str))]
                (-> type .toLowerCase keyword)
                (throw
                  (IllegalStateException.
                    (format "Unsupported sql query type! Expecting either select, insert, update, or delete. SQL: %s"
                      sql-str))))
           table (extract-table-name op sql-str)
           meta  {:op op :table table}]
       (loop [i 1 params-idx nil]
         (if-let [f (and (.find m) (.group m 1))]
           (recur
             (inc i)
             (update
               params-idx
               (keyword f)
               #(if (vector? %1) (conj %1 %2) [%2])
               i))
           (QuerySpec. (.replaceAll m "?") params-idx options meta)))))))


;; ########################################
;; ops

(defmacro with-connection
  "Evaluates body in the context of an active connection to the database.
   (with-connection [con-db db-spec]
     ... con-db ...)"
  [binding & body]
  (let [conn-var (first binding)]
    `(let [spec# ~(second binding)]
       (with-open [~conn-var (get-connection spec#)]
         ~@body))))

(def ^{:const true :private true :from "clojure.java.jdbc"}
  result-set-concurrency
  {:read-only ResultSet/CONCUR_READ_ONLY
   :updatable ResultSet/CONCUR_UPDATABLE})

(def ^{:const true :private true :from "clojure.java.jdbc"}
  result-set-holdability
  {:hold ResultSet/HOLD_CURSORS_OVER_COMMIT
   :close ResultSet/CLOSE_CURSORS_AT_COMMIT})

(def ^{:const true :private true :from "clojure.java.jdbc"}
  result-set-type
  {:forward-only ResultSet/TYPE_FORWARD_ONLY
   :scroll-insensitive ResultSet/TYPE_SCROLL_INSENSITIVE
   :scroll-sensitive ResultSet/TYPE_SCROLL_SENSITIVE})

(defn ^{:from "clojure.java.jdbc"} set-prepared-statement-params!
  "Set the `stmt`'s `params` based on `query-spec`."
  [^PreparedStatement stmt query-spec params]
  (doseq [[k ixs] (:params-idx query-spec) ix ixs]
    (jdbc/set-parameter (get params k) stmt ix))
  stmt)

(defn ^{:from "clojure.java.jdbc"} create-prepared-statement
  "Create a prepared statement from a connection, a SQL string and a map
   of options:
     :return-keys truthy | nil - default nil
       for some drivers, this may be a vector of column names to identify
       the generated keys to return, otherwise it should just be true
     :result-type :forward-only | :scroll-insensitive | :scroll-sensitive
     :concurrency :read-only | :updatable
     :cursors
     :fetch-size n
     :max-rows n
     :timeout n"
  [^Connection conn
   {{return-keys :return-keys
     result-type :result-type
     concurrency :concurrency
     cursors     :cursors
     fetch-size  :fetch-size
     max-rows    :max-rows
     timeout     :timeout} :options
    :as query-spec}]
  (let [^PreparedStatement stmt
        (cond
          (execute? query-spec)
          (cond
            (-> return-keys first string?)
            (do
              (.prepareStatement conn (:sql query-spec) (into-array String return-keys)))

            (-> return-keys first number?)
            (do
              (.prepareStatement conn (:sql query-spec) (into-array Integer return-keys)))

            return-keys
            (do
              (.prepareStatement conn (:sql query-spec) java.sql.Statement/RETURN_GENERATED_KEYS))

            :else
            (do
              (.prepareStatement conn (:sql query-spec))))

          (and result-type concurrency)
          (if cursors
            (.prepareStatement conn (:sql query-spec)
              (get result-set-type result-type result-type)
              (get result-set-concurrency concurrency concurrency)
              (get result-set-holdability cursors cursors))
            (.prepareStatement conn (:sql query-spec)
              (get result-set-type result-type result-type)
              (get result-set-concurrency concurrency concurrency)))

          :else
          (.prepareStatement conn (:sql query-spec)))]
    (when fetch-size (.setFetchSize stmt fetch-size))
    (when max-rows (.setMaxRows stmt max-rows))
    (when timeout (.setQueryTimeout stmt timeout))
    stmt))


;; https://leanpub.com/high-performance-java-persistence/read#leanpub-auto-retrieving-auto-generated-keys
;; http://stackoverflow.com/questions/19022175/executebatch-method-return-array-of-value-2-in-java
(defn collect-result
  "Collect results after an execute query (update, insert, delete). May return
   auto-increment values for insert operations (if `return-keys == true` and
   supported by driver) or err to `cnt`."
  [^PreparedStatement stmt
   {{return-keys :return-keys
     return-keys-naming-strategy :return-keys-naming-strategy} :options
    :as query-spec}
   cnt]
  (let [return-keys-naming-strategy (or return-keys-naming-strategy str/lower-case)
        ;; may or may not be supported by vendors
        ret-ks  (^{:once true} fn* [stmt alt-ret]
                 (try
                   (let [rs (.getGeneratedKeys stmt)]
                     (doall (jdbc/result-set-seq rs {:identifiers return-keys-naming-strategy})))
                   (catch Exception ex
                     (throw ex)
                     alt-ret)))
        ret-cnt (^{:once true} fn* [stmt alt-ret] alt-ret)]
    ((if return-keys ret-ks ret-cnt) stmt cnt)))


;; #######################################
;; Public API

(defn query
  "Query the database given `query-spec`."
  [conn query-spec & [params options]]
  (with-connection [conn conn]
    (let [query-spec (update query-spec :options merge options)
          stmt (create-prepared-statement conn query-spec)]
      (try
        (let [stmt (set-prepared-statement-params! stmt query-spec params)]
          (jdbc/query conn [stmt] (or options {})))
        (finally
          (.close stmt))))))

(defn execute!
  "Run an execute a query (insert, update, delete) against the dabase.
   May return auto-increment keys."
  [conn query-spec & [params {:keys [return-keys] :as options}]]
  (with-connection [conn conn]
    (let [query-spec (update query-spec :options merge options)
          ^PreparedStatement stmt (create-prepared-statement conn query-spec)]
      (try
        (if (vector? params)
          (do
            (doseq [params params]
              (set-prepared-statement-params! stmt query-spec params)
              (.addBatch stmt))
            (let [cnt (into [] (.executeBatch stmt))]
              (collect-result stmt query-spec cnt)))
          (do
            (set-prepared-statement-params! stmt query-spec params)
            (let [cnt (.executeUpdate stmt)]
              (collect-result stmt query-spec cnt))))
        (finally
          (.close stmt))))))

(defn insert!
  "Insert record(s) into the database. If `(= true (:return-keys options))`
   the inserted autogenerated key(s) will be returned (if supported by the
   underlying driver)."
  [conn query-spec & [params {:keys [return-keys] :as options}]]
  (execute! conn query-spec params options))

(defn update!
  "Update record(s) in the database. Return the affected rows."
  [conn query-spec & [params options]]
  (execute! conn query-spec params options))

(defn delete!
  "Delete record(s) in the database."
  [conn query-spec & [params options]]
  (execute! conn query-spec params options))
