(ns instilled.cljdbc
  (:require
    [clojure.string         :as str]
    #_[clojure.java.jdbc      :as jdbc])
  (:import
    [java.sql
     ResultSet
     PreparedStatement
     Connection
     DriverManager]))

;; TODO: remove dependency to clojure.java.jdbc

;; ########################################
;; Protos and proto exts

(defrecord QuerySpec
  [sql params-idx options meta])

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
  (pre-insert [conn query-spec params])
  (post-insert [conn query-spec rs])

  (pre-query [conn query-spec params])
  (post-query [conn query-spec params])

  (pre-delete [conn query-spec params])
  (post-delete [conn query-spec params])

  (pre-update [conn query-spec params])
  (post-update [conn query-spec params]))

(defprotocol ISQLValue
  "Protocol for creating SQL values from Clojure values. Default
   implementations (for Object and nil) just return the argument,
   but it can be extended to provide custom behavior to support
   exotic types supported by different databases."
  (sql-value [val] "Convert a Clojure value into a SQL value."))

(extend-protocol ISQLValue
  Object
  (sql-value [v] v)

  nil
  (sql-value [_] nil))

(defprotocol ISQLParameter
  "Protocol for setting SQL parameters in statement objects, which
   can convert from Clojure values. The default implementation just
   delegates the conversion to ISQLValue's sql-value conversion and
   uses .setObject on the parameter. It can be extended to use other
   methods of PreparedStatement to convert and set parameter values."
  (set-parameter [val stmt ix]
    "Convert a Clojure value into a SQL value and store it as the ix'th
     parameter in the given SQL statement object."))

(extend-protocol ISQLParameter
  Object
  (set-parameter [v ^PreparedStatement s ^long i]
    (.setObject s i (sql-value v)))

  nil
  (set-parameter [_ ^PreparedStatement s ^long i]
    (.setObject s i (sql-value nil))))

(defprotocol IResultSetReadColumn
  "Protocol for reading objects from the java.sql.ResultSet. Default
   implementations (for Object and nil) return the argument, and the
   Boolean implementation ensures a canonicalized true/false value,
   but it can be extended to provide custom behavior for special types."
  (result-set-read-column [val rsmeta idx]
    "Function for transforming values after reading them from the database"))

(extend-protocol IResultSetReadColumn
  Object
  (result-set-read-column [x _2 _3] x)

  Boolean
  (result-set-read-column [x _2 _3] (if (= true x) true false))

  nil
  (result-set-read-column [_1 _2 _3] nil))

(defn dml?
  "Whether or not `query-spec` is a DML statement (update,
   insert, delete)."
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
   `:named-param` (named).

   `options` may be a map with keys:
      :pk [^int col-nums] | [^String col-names] | nil

   TODO: more doc"
  ([^String sql-str]
   (parse-statement sql-str nil))
  ([^String sql-str options]
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
           (->QuerySpec
             (.replaceAll m "?")
             params-idx
             options
             meta)))))))


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
    (set-parameter (get params k) stmt ix))
  stmt)

(defn ^{:from "clojure.java.jdbc"} create-prepared-statement
  "Create prepared statement from a connection. Takes a `query-spec`
   with options:
     :return-keys true | nil
     :result-type :forward-only | :scroll-insensitive | :scroll-sensitive
     :concurrency :read-only | :updatable
     :cursors     :hold | :close
     :fetch-size  n
     :max-rows    n
     :timeout     n"
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
          (dml? query-spec)
          (cond
            (-> return-keys first string?)
            (.prepareStatement conn (:sql query-spec) (into-array String return-keys))
            (-> return-keys first number?)
            (.prepareStatement conn (:sql query-spec) (into-array Integer return-keys))
            return-keys
            (.prepareStatement conn (:sql query-spec) java.sql.Statement/RETURN_GENERATED_KEYS)
            :else
            (.prepareStatement conn (:sql query-spec)))

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

(defn result-set-seq
  ([rs]
   (result-set-seq
     rs
     {;; TODO: use execution aspects for parsing
      ;; based on dialect, e.g. no need for lower-case
      ;; on mysql as by default is lower-case
      :col-transform-fn (comp keyword str/lower-case)}))
  ([^ResultSet rs {:keys [col-transform-fn]}]
   (let [rsmeta    (.getMetaData rs)
         col-range (range 1 (inc (.getColumnCount rsmeta)))
         ks        (for [i col-range]
                     (-> (.getColumnLabel rsmeta i)
                         ;; make-cols-unique
                         (col-transform-fn)))
         vs        (fn []
                     (map (fn [^Integer i]
                            (result-set-read-column
                              (.getObject rs i)
                              rsmeta
                              i))
                       col-range))]
     ((fn thisfn []
        (when (.next rs)
          (cons (zipmap ks (vs)) (lazy-seq (thisfn)))))))))

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
                     (doall
                       (result-set-seq
                         rs
                         {:identifiers return-keys-naming-strategy})))
                   (catch Exception ex
                     (throw ex)
                     alt-ret)))
        ret-cnt (^{:once true} fn* [stmt alt-ret] alt-ret)]
    ((if return-keys ret-ks ret-cnt) stmt cnt)))


;; #######################################
;; Public API

(defn prepare-query
  "Query the database given `query-spec`. Return the open `ResultSet`.
   Usually you would use `query` or `with-query-rs` functions."
  [^Connection conn query-spec & [params options]]
  (let [query-spec (update query-spec :options merge options)
        stmt (create-prepared-statement conn query-spec)]
    (set-prepared-statement-params! stmt query-spec params)))

(defn query
  "Query the database given `query-spec`."
  [conn query-spec & [params options]]
  (with-connection [conn conn]
    (with-open [stmt (prepare-query conn query-spec params options)
                rs   (.executeQuery stmt)]
      (doall (result-set-seq rs)))))

(defn execute!
  "Run an DML query (insert, update, delete) against the dabase.
   May return auto-increment keys."
  [conn query-spec & [params {:keys [return-keys] :as options}]]
  (with-connection [conn conn]
    (let [query-spec (update query-spec :options merge options)
          ^PreparedStatement stmt (create-prepared-statement conn query-spec)]
      (try
        (if (vector? (first params))
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
