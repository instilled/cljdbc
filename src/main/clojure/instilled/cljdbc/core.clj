(ns instilled.cljdbc.core
  (:require
    [clojure.string         :as str])
  (:import
    [java.sql
     ResultSet
     PreparedStatement
     Connection
     DriverManager]
    [javax.sql
     DataSource]))

;; ########################################
;; Protos and proto exts

(def ^:const supported-drivers-version
  {"oracle" {:versions [7]}
   "mysql"  {:versions [6]}})

(defrecord QuerySpec
  [sql params-idx options meta])

(defprotocol ITransactionStrategy
  (begin! [this ^Connection conn options])
  (commit! [this ^Connection conn])
  (rollback! [this ^Connection conn]))

(defprotocol IExecutionAspect
  (pre-prepare [conn query-spec params])
  (post-prepare [conn query-spec params])

  (pre-insert [conn query-spec params])
  (post-insert [conn query-spec rs])

  (pre-query [conn query-spec params])
  (post-query [conn query-spec params])

  (pre-delete [conn query-spec params])
  (post-delete [conn query-spec params])

  (pre-update [conn query-spec params])
  (post-update [conn query-spec params]))

(defprotocol ^{:from "clojure.java.jdbc"} ISQLValue
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

(defprotocol ^{:from "clojure.java.jdbc"} ISQLParameter
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

(defprotocol ^{:from "clojure.java.jdbc"} IResultSetReadColumn
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

(defprotocol ICljdbcConnection
  (close
    [this])
  (get-connection
    [this options])
  (lift-connection
    [this])
  (get-transaction
    [this]))

(def ^{:private true :doc "Transaction isolation levels."}
  isolation-levels
  {:none             java.sql.Connection/TRANSACTION_NONE
   :read-committed   java.sql.Connection/TRANSACTION_READ_COMMITTED
   :read-uncommitted java.sql.Connection/TRANSACTION_READ_UNCOMMITTED
   :repeatable-read  java.sql.Connection/TRANSACTION_REPEATABLE_READ
   :serializable     java.sql.Connection/TRANSACTION_SERIALIZABLE})

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

(defrecord CljdbcProxyStringDataSource
  [^String jdbc-url active-connection transaction]
  ICljdbcConnection
  (close
    [this]
    (when active-connection
      (.close active-connection)))
  (get-connection
    [this options]
    (if active-connection
      active-connection
      (assoc this
        :active-connection (DriverManager/getConnection jdbc-url))))
  (lift-connection
    [this]
    (when-not active-connection
      (throw (IllegalStateException.
               "Boom! No active connection. Body shuold usually be wrapped in `with-connection`.")))
    active-connection)
  (get-transaction
    [this]
    transaction))

(defrecord CljdbcProxyDataSource
  [^DataSource ds active-connection transaction]
  ICljdbcConnection
  (close
    [this]
    (.close ds))
  (get-connection
    [this options]
    ;; TODO: allow nested connections?
    (if active-connection
      this
      (assoc this
        :active-connection (.getConnection ds))))
  (lift-connection
    [this]
    (when-not active-connection
      (throw (IllegalStateException.
               "Boom! No active connection. Body shuold usually be wrapped in `with-connection`.")))
    active-connection)
  (get-transaction
    [this]
    transaction))

(defrecord DefaultTransactionStrategy
  [ctx]
  ITransactionStrategy
  (begin!
    [this conn options]
    (vswap!
      ctx
      (fn [cur new] new)
      (let [{:keys [savepoints depth] :as ctx*} @ctx]
        (cond
          ;; Transaction start
          (= -1 depth)
          (do
            (let [{:keys [isolation read-only?]} options
                  new-ctx (merge
                            ctx*
                            {:depth 0
                             :auto-commit (.getAutoCommit conn)
                             :isolation   (.getTransactionIsolation conn)
                             :read-only?  (.isReadOnly conn)})]
              (.setAutoCommit conn false)
              (when isolation
                (.setTransactionIsolation conn (get isolation-levels isolation isolation)))
              (when read-only?
                (.setReadOnly conn true))
              new-ctx))

          ;; Nested transaction
          :else
          (let [sp (.setSavepoint conn)]
            (-> ctx*
                (update :depth inc)
                (update :savepoints conj sp))))))
    this)
  (commit!
    [this conn]
    (let [{:keys [read-only? isolation auto-commit savepoints depth]} @ctx]
      (if read-only?
        (rollback! this conn)
        (.commit conn))
      (vswap!
        ctx
        (fn [old new] new)
        (cond
          ;; outermost transaction
          (= 0 depth)
          (do
            ;; reset state
            (.setTransactionIsolation conn isolation)
            (.setReadOnly conn read-only?)
            (.setAutoCommit conn auto-commit)
            (assoc @ctx
              :depth -1))

          ;; TODO: fb: depth check should not be
          ;; necessary
          (and (< 0 depth)
               (< 0 (count savepoints)))
          (assoc @ctx
            :depth (dec depth)
            :savepoints (rest savepoints))

          :else
          (throw (IllegalStateException. "Unexpected transaction state!"))))
      this))
  (rollback!
    [this conn]
    (let [{:keys [savepoints depth]} @ctx]
      (vswap!
        ctx
        (fn [old new] new)
        (cond
          ;; Usually ex at transaction initialization
          (= -1 depth)
          @ctx

          (= 0 depth)
          (do
            (.rollback conn)
            (assoc
              @ctx
              :depth -1))

          ;; TODO: fb: depth check should not be
          ;; necessary
          (and (< 0 depth)
               (seq savepoints))
          (let [[sp & rest] savepoints]
            (.rollback conn sp)
            (assoc @ctx
              :depth (dec depth)
              rest))

          :else
          (throw
            (IllegalStateException.
              "Unexpected transaction state. This may be a bug. Please open Github issue with stacktrace!"))))
      this)))

(defn make-default-transaction-strategy
  []
  (DefaultTransactionStrategy.
    (volatile!
      {:savepoints (list)
       :depth (int -1)})))

(defn ^:private connection-pool-type
  [options]
  (cond
    (:hikari options)
    :hikari

    (:c3p0 options)
    :c3p0

    (:tomcatPool options)
    :tomcatPool))

(defn ^:private make-pooled-datasource
  [jdbc-url options]
  (let [impl (connection-pool-type options)
        ns (->> impl
                (name)
                (str "instilled.cljdbc.cp.")
                (symbol))]
    (eval `(do ~(require ns)))
    (let [pool-factory (ns-resolve ns 'make-pool)]
      (when-not pool-factory
        (throw
          (IllegalStateException.
            (str "Connection pool could not be loaded! `make-pool` not found in ns: " (name ns)))))
      (pool-factory jdbc-url (get options impl)))))

(defn bind-transaction
  [conn {:keys [transaction-strategy] :as options}]
  (if (get-transaction conn)
    conn
    (assoc conn
      :transaction (or transaction-strategy
                       (make-default-transaction-strategy)))))

(defn do-transactionally
  [conn options f]
  (when-not (satisfies? ICljdbcConnection conn)
    (throw
      (IllegalStateException.
        "Can not start a transaction on non ICljdbcConnection type! See (get-connection).")))
  (let [conn* (lift-connection conn)
        transaction (get-transaction conn)]
    (try
      (begin! transaction conn* options)
      (let [result (f)]
        (commit! transaction conn*)
        result)
      (catch Throwable t
        (rollback! transaction conn*)
        (throw t)))))

(defn dml?
  "Whether or not `query-spec` is a DML statement (update,
   insert, delete)."
  [query-spec]
  (case (get-in query-spec [:meta :op])
    :update true
    :insert true
    :execute true
    false))

(defn batched?
  "True if batching should be user in DML statements."
  [query-spec]
  (get-in query-spec [:options :batched?]))

(defn named-or-positional?
  [query-spec]
  (get query-spec :params-idx))

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

(defn set-prepared-statement-params!
  "Set the `stmt`'s `params` based on `query-spec`."
  [^PreparedStatement stmt query-spec params]
  (if (named-or-positional? query-spec)
    ;; TODO: validate params is also named or positional?
    (doseq [[k ixs] (:params-idx query-spec) ix ixs]
      (set-parameter (get params k) stmt ix))
    (dorun
      ;; TODO: is map-indexed in dorun the right choice?
      (map-indexed (fn [ix v] (set-parameter v stmt (inc ix))) params)))
  stmt)

(defn ^{:from "clojure.java.jdbc"} create-prepared-statement
  "Create prepared statement from a connection. Takes a `query-spec`
   with options:
     :returning true | nil
     :result-type :forward-only | :scroll-insensitive | :scroll-sensitive
     :concurrency :read-only | :updatable
     :cursors     :hold | :close
     :fetch-size  n
     :max-rows    n
     :timeout     n"
  [^Connection conn
   {{returning   :returning
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
            (-> returning first string?)
            (.prepareStatement conn (:sql query-spec) (into-array String returning))
            (-> returning first number?)
            (.prepareStatement conn (:sql query-spec) (into-array Integer returning))
            returning
            (.prepareStatement conn (:sql query-spec) java.sql.Statement/RETURN_GENERATED_KEYS)
            :else
            (.prepareStatement conn (:sql query-spec)))

          (and result-type concurrency)
          (if cursors
            (do
              (when-not (and (get result-set-type result-type)
                             (get result-set-concurrency concurrency)
                             (get result-set-holdability cursors))
                (throw
                  (IllegalArgumentException.
                    (str "Unsupported result-set-type, result-set-concurrency, or result-set-holdability: "
                      (select-keys query-spec [:result-type :concurrency :cursors])))))
              (.prepareStatement conn (:sql query-spec)
                (get result-set-type result-type)
                (get result-set-concurrency concurrency)
                (get result-set-holdability cursors)))
            (do
              (when-not (and (get result-set-type result-type)
                             (get result-set-concurrency concurrency))
                (throw
                  (IllegalArgumentException.
                    (str "Unsupported result-set-type, or result-set-concurrency: "
                      (select-keys query-spec [:result-type :concurrency])))))
              (.prepareStatement conn (:sql query-spec)
                (get result-set-type result-type result-type)
                (get result-set-concurrency concurrency concurrency))))

          :else
          (.prepareStatement conn (:sql query-spec)))]
    (when fetch-size
      (when (< fetch-size 0)
        (throw
          (IllegalArgumentException.
            (str "Unsupported fetch-size. Expecting >= 0. Value: " fetch-size))))
      (.setFetchSize stmt fetch-size))
    (when max-rows
      (when (< max-rows 0)
        (throw
          (IllegalArgumentException.
            (str "Unsupported max-rows. Expecting >= 0. Value: " max-rows))))
      (.setMaxRows stmt max-rows))
    (when timeout
      (when (< timeout 0)
        (throw
          (IllegalArgumentException.
            (str "Unsupported timeout, Expecting >= 0. Value: " timeout))))
      (.setQueryTimeout stmt timeout))
    stmt))

(defn result-set-seq
  [^ResultSet rs {:keys [col-transform-fn]}]
  (let [;; TODO: use execution aspects for parsing
        ;; based on dialect, e.g. no need for lower-case
        ;; on mysql as by default is lower-case
        col-transform-fn (or col-transform-fn
                             (comp keyword str/lower-case))
        rsmeta    (.getMetaData rs)
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
         (cons (zipmap ks (vs)) (lazy-seq (thisfn))))))))

;; https://leanpub.com/high-performance-java-persistence/read#leanpub-auto-retrieving-auto-generated-keys
;; http://stackoverflow.com/questions/19022175/executebatch-method-return-array-of-value-2-in-java
(defn collect-result
  "Collect results after a dml statement. May return
   auto-increment values for insert operations (if `returning == [pk]` and
   supported by driver) or err to `cnt`."
  [^PreparedStatement stmt
   {{returning :returning
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
    ((if returning ret-ks ret-cnt) stmt cnt)))

(defn load-dialect
  [jdbc-url]
  (try
    (let [conn (DriverManager/getConnection jdbc-url)]
      (println (.. conn getMetaData getDatabaseProductName)))
    (catch Exception e
      (throw (IllegalStateException. "Failed to load dialect extensions!" e)))

    ;;     * @return database product name
    ;;     * @exception SQLException if a database access error occurs
    ;;     */
    ;;    String getDatabaseProductName() throws SQLException;
    ;;
    ;;    /**
    ;;     * Retrieves the version number of this database product.
    ;;     *
    ;;     * @return database version number
    ;;     * @exception SQLException if a database access error occurs
    ;;     */
    ;;    String getDatabaseProductVersion() throws SQLException;
    ;;
    ;;    /**
    ;;     * Retrieves the name of this JDBC driver.
    ;;     *
    ;;     * @return JDBC driver name
    ;;     * @exception SQLException if a database access error occurs
    ;;     */
    ;;    String getDriverName() throws SQLException;
    ;;
    ;;    /**
    ;;     * Retrieves the version number of this JDBC driver as a <code>String</code>.
    ;;     *
    ;;     * @return JDBC driver version
    ;;     * @exception SQLException if a database access error occurs
    ;;     */
    ;;    String getDriverVersion() throws SQLException;
    )

  )
