(ns instilled.cljdbc
  (:require
    [clojure.string :as str])
  (:import
    [java.sql
     ResultSet
     ResultSetMetaData
     PreparedStatement
     DatabaseMetaData
     Connection
     DriverManager]
    [javax.sql
     DataSource]))

;; ########################################
;; Protos and proto exts
(def ^:const supported-driver-versions
  {"oracle" {:version [12]}
   "mysql"  {:version [5]}})

(defrecord QuerySpec
  [sql params-idx options meta])

(defprotocol ITransactionStrategy
  (begin! [this ^Connection conn options])
  (commit! [this ^Connection conn])
  (rollback! [this ^Connection conn]))

(defprotocol ISQLVendor
  (rs-col-name [this ^ResultSet rs ^ResultSetMetaData rsmeta i query-spec])
  (rs-col-value [this ^ResultSet rs ^ResultSetMetaData rsmeta i query-spec])
  (returning [this ^ResultSet rs query-spec returning-cols])
  (returning-count [this cnt]))

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
  (result-set-read-column
    [x rsmeta idx]
    x)

  Boolean
  (result-set-read-column [x _2 _3] (if (= true x) true false))

  nil
  (result-set-read-column [_1 _2 _3] nil))

;; This is mainly exists to support non datasource backed
;; connections.
(defprotocol ICljdbcConnectionAware
  (new-connection
    [this]))

(extend-type DataSource
  ICljdbcConnectionAware
  (new-connection
    [this]
    (.getConnection this)))

(defprotocol ICljdbcConnection
  (close
    [this])
  (get-connection
    [this options])
  (lift-connection
    [this])
  (get-transaction
    [this])
  (sql-vendor
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

(defrecord CljdbcProxyDataSource
  [^instilled.cljdbc.ICljdbcConnectionAware ds sql-vendor active-connection transaction]
  ICljdbcConnection
  (get-connection
    [this options]
    ;; TODO: allow nested connections?
    (if active-connection
      this
      (assoc this
        :active-connection (new-connection ds))))
  (lift-connection
    [this]
    (when-not active-connection
      (throw (IllegalStateException.
               "Boom! No active connection. Body shuold usually be wrapped in `with-connection`.")))
    active-connection)
  (get-transaction
    [this]
    transaction)
  (sql-vendor
    [this]
    sql-vendor)
  (close
    [this]
    (.close ds)))

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

(defn connection-pool-type
  "Extract connection pool type from `options`."
  [options]
  (cond
    (:hikari options)
    :hikari

    (:c3p0 options)
    :c3p0

    (:tomcatPool options)
    :tomcatPool))

(defn load-and-invoke
  [fqfn & args]
  (let [[ns fn] (map symbol (-> fqfn (name) (.split "/")))]
    (when-not (and ns fn)
      (throw
        (IllegalStateException.
          (str "Unsupported fqfn. Expecting ns/fn. Val: " fqfn))))
    (eval `(do ~(require (symbol ns))))
    (let [f (ns-resolve ns fn)]
      (when-not (fn? @f)
        (throw
          (IllegalStateException.
            (str "Fn does not exist! " fqfn))))
      (apply f args))))

(defn make-pooled-datasource
  [jdbc-url options]
  (let [t (connection-pool-type options)]
    (load-and-invoke
      (format "instilled.cljdbc.cp.%s/make-pool" (name t))
      jdbc-url
      (get options t))))

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

(defn ^:private jndi-spec?
  [spec]
  (and (string? spec)
       (-> spec
           (.toLowerCase)
           (.startsWith "jndi:"))))

(defn jndi-lookup
  "Lookup `name` from a jndi context. Expect `name` to start
   with `jndi:` which is removed before performing the lookup."
  [name]
  (javax.naming.InitialContext/doLookup (.substring name 5)))

(defn ^:private jdbc-url-spec?
  [spec]
  (and (string? spec)
       (-> spec
           (.toLowerCase)
           (.startsWith "jdbc"))))

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

(defn returning?
  [query-spec]
  (get-in query-spec [:options :returning]))

(defn named-or-positional?
  [query-spec]
  (get query-spec :params-idx))

(defn process-result-set
  "Process result set."
  [^ResultSet rs query-spec sql-vendor]
  (let [rs-meta (.getMetaData rs)
        col-cnt (inc (.getColumnCount rs-meta))
        ks (loop [i 1
                  res (transient [])]
             (if (< i col-cnt)
               (recur
                 (inc i)
                 (conj!
                   res
                   (rs-col-name sql-vendor rs rs-meta i query-spec)))
               (persistent! res)))
        vs (fn []
             (loop [i 1
                    res (transient [])]
               (if (< i col-cnt)
                 (recur
                   (inc i)
                   (conj!
                     res
                     (rs-col-value sql-vendor rs rs-meta i query-spec)))
                 (persistent! res))))]
    ((fn thisfn []
       (when (.next rs)
         (cons (zipmap ks (vs)) (lazy-seq (thisfn))))))))

(defn set-prepared-statement-params!
  "Set the `stmt`'s `params` based on `query-spec`."
  [^PreparedStatement stmt query-spec params]
  (if (named-or-positional? query-spec)
    ;; TODO: validate params is also named or positional?
    (doseq [[k ixs] (:params-idx query-spec) ix ixs] (set-parameter (get params k) stmt ix))
    ;; TODO: is map-indexed in dorun the right choice?
    (dorun (map-indexed (fn [ix v] (set-parameter v stmt (inc ix))) params)))
  stmt)

(defn ^{:from "clojure.java.jdbc"} create-prepared-statement
  "Create prepared statement from a connection. Takes a `query-spec`
   with options:
     :returning   vector with cols to be returned (for DML statements). If omitted
                  return the update count.
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
          (if (coll? returning)
            (.prepareStatement conn (:sql query-spec) (into-array String (map name returning)))
            (.prepareStatement conn (:sql query-spec))
            #_(cond
                (true? returning)
                (.prepareStatement conn (:sql query-spec) java.sql.Statement/RETURN_GENERATED_KEYS)

                (and (coll? returning) (-> returning first number?))
                (.prepareStatement conn (:sql query-spec) (into-array Integer returning))))

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
        ;; based on vendor, e.g. no need for lower-case
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

(defn validate-driver
  [connection-aware]
  (let [conn (new-connection connection-aware)
        ^DatabaseMetaData dm (.getMetaData conn)]
    (cond
      (not (.supportsSavepoints dm))
      (throw (IllegalStateException. "Savepoint support required!")))
    conn))

(defn load-vendor-extension
  [connection-aware]
  (try
    (let [conn (new-connection connection-aware)
          dn   (.. conn getMetaData getDriverName toLowerCase)
          mv   (.. conn getMetaData getDriverMajorVersion)
          ext  (->> supported-driver-versions
                    (filter
                      (fn [[d {[min-version] :version}]]
                        (and (.contains dn (name d))
                             (<= min-version mv))))
                    (first))]
      (load-and-invoke
        (format "instilled.cljdbc.vendor.%s/extension"
          (if ext (first ext) "generic"))))
    (catch Exception e
      (throw (IllegalStateException. "Failed to load vendor extension" e)))))

(defmacro with-open-statement
  "Shortcut to `(with-open [(first binding) (second binding)] ... body ...)`"
  [binding query-spec & body]
  (let [stmt-var (first binding)]
    `(with-open [^PreparedStatement ~stmt-var (-> (lift-connection ~(second binding))
                                                  (create-prepared-statement ~query-spec))]
       ~@body)))


;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
;; Public API
;;
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

(defn make-datasource
  "Make a datasource. Use `make-default-transaction-strategy` if
   not provided through as `:transaction-strategy` in `options`."
  [spec {:keys [transaction-strategy] :as options}]
  (let [connection-aware (cond
                           (and (string? spec)
                                (connection-pool-type options))
                           (make-pooled-datasource spec options)

                           (instance? javax.sql.DataSource spec)
                           spec

                           (jndi-spec? spec)
                           (jndi-lookup spec)

                           (jdbc-url-spec? spec)
                           (reify ICljdbcConnectionAware
                             (new-connection
                               [this]
                               (DriverManager/getConnection spec)))

                           :else
                           (throw
                             (IllegalStateException.
                               (str "Unsupported `spec` provided to `make-datasource`!"))))]
    (validate-driver connection-aware)
    (CljdbcProxyDataSource.
      connection-aware
      (load-vendor-extension connection-aware)
      nil
      nil)))

(defmacro with-connection
  "Evaluates body in the context of an active connection to the database.
   A new transaction is automatically spawned.

   An open connection should be used by only one thread concurrently.

   (with-connection [conn conn]
     ... con-db ...)"
  [binding & body-and-or-options]
  (let [conn-var (first binding)
        [options body] (if (map? (first body-and-or-options))
                         [(first body-and-or-options) (rest body-and-or-options)]
                         [{} body-and-or-options])]
    `(let [spec# ~(second binding)
           ~conn-var (-> spec#
                         (get-connection ~options)
                         (bind-transaction ~options))]
       (with-open [conn# (lift-connection ~conn-var)]
         (do-transactionally
           ~conn-var ~options
           (fn [] ~@body))))))

(defmacro with-transaction
  "Transactionally executes body.
   See also `with-connection`."
  [binding & body-and-or-options]
  ;; TODO: implement the following behaviour
  ;; If a new transaction context is required `{:requires-new true}`
  ;; must be set in `options`.
  (let [conn-var (first binding)
        [options body] (if (map? (first body-and-or-options))
                         [(first body-and-or-options) (rest body-and-or-options)]
                         [{} body-and-or-options])]
    `(let [~conn-var ~(second binding)]
       (do-transactionally
         ~conn-var ~options
         (fn [] ~@body)))))

(defn query
  "Query the database given `query-spec`."
  [conn query-spec & [params options]]
  (let [sql-vendor (sql-vendor conn)
        query-spec (update query-spec :options merge options)]
    (with-open-statement [stmt conn] query-spec
      (set-prepared-statement-params! stmt query-spec params)
      (with-open [rs (.executeQuery stmt)]
        (doall (process-result-set rs query-spec sql-vendor))))))

(defn execute!
  "Run a DML query (insert, update, delete) against the dabase.
   May return auto-generated primary keys."
  [conn query-spec & [params options]]
  (let [vendor        (sql-vendor conn)
        query-spec     (update query-spec :options merge options)
        returning-cols (get-in query-spec [:options :returning])
        result        (fn ^:once [stmt cnt]
                        (if returning-cols
                          (with-open [rs (.getGeneratedKeys stmt)]
                            (doall (returning vendor rs query-spec returning-cols)))
                          (returning-count vendor cnt)))]
    (if (and (sequential? params)
             (coll? (first params)))
      (if (batched? query-spec)
        (with-open-statement [stmt conn] query-spec
          (doseq [params params]
            (set-prepared-statement-params! stmt query-spec params)
            (.addBatch stmt))
          (result stmt (.executeBatch stmt)))
        (for [params params]
          (with-open-statement [stmt conn] query-spec
            (set-prepared-statement-params! stmt query-spec params)
            (result stmt (.executeUpdate stmt)))))
      (with-open-statement [stmt conn] query-spec
        (set-prepared-statement-params! stmt query-spec params)
        (result stmt (.executeUpdate stmt))))))

(defn insert!
  "Insert record(s) into the database.

   May return autogenerated primary keys if `{:returning ...}` is
   set in `options`. This is supported for:

   * MySQL
   * Oracle (starting from driver version 7)
   * Postgres
   * ???

   Assuming `id` is the column with the primary autoincrement
   key `{:returning [:id]}` should be used. If the driver
   natively supports the `RETURNING` clause more columns may
   be queried. For maximum compatibility it is recommended
   to provide the PK column as the first element in the vector."
  [conn query-spec & [params {:keys [returning] :as options}]]
  (execute! conn query-spec params options))

(defn update!
  "Update record(s) in the database."
  [conn query-spec & [params options]]
  (execute! conn query-spec params options))

(defn delete!
  "Delete record(s) in the database."
  [conn query-spec & [params options]]
  (execute! conn query-spec params options))
