(ns instilled.cljdbc
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

(defrecord QuerySpec
  [sql params-idx options meta])

(defprotocol ITransactionStrategy
  (begin! [this ^Connection conn options])
  (commit! [this ^Connection conn])
  (rollback! [this ^Connection conn]))

(defprotocol IExecutionAspect
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

(defprotocol ICljdbcConnection
  (close
    [this])
  (get-connection
    [this options])
  (lift-connection
    [this])
  (get-transaction
    [this]))

(defrecord CljdbcProxyStringDataSource
  [conn transaction]
  ICljdbcConnection
  (close
    [this]
    (.close conn))
  (get-connection
    [this options]
    conn)
  (lift-connection
    [this]
    conn)
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

          ;; inner transaction - nothing to do
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

          (and (< 0 depth)
               (seq savepoints))
          (let [[sp & rest] savepoints]
            (.rollback conn sp)
            (assoc @ctx
              :depth (dec depth)
              rest))

          :else
          (throw (IllegalStateException. "Unexpected transaction state!"))))
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

(defn make-datasource
  "Make a connection. Use `make-default-transaction-strategy` if
   not provided through as `:transaction-strategy` in `options`."
  [spec {:keys [transaction-strategy] :as options}]
  (cond
    (and (string? spec)
         (connection-pool-type options))
    (CljdbcProxyDataSource.
      (make-pooled-datasource spec options)
      nil
      nil)

    (instance? javax.sql.DataSource spec)
    (CljdbcProxyDataSource.
      spec
      nil
      nil)

    (string? spec)
    (CljdbcProxyStringDataSource.
      (DriverManager/getConnection spec)
      nil)

    :else
    (throw
      (IllegalStateException.
        (str "Unsupported `spec` provided to `make-datasource`!")))))

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
            (do
              (when-not (and (get result-set-type result-type)
                             (get result-set-concurrency concurrency)
                             (get result-set-holdability cursors))
                (throw
                  (IllegalArgumentException.
                    (str "Unsupported result-set-type, result-set-concurrency, or result-set-holdability, is: "
                      (select-keys query-spec [:result-type :concurrency :cursors])))))
              (.prepareStatement conn (:sql query-spec)
                (get result-set-type result-type)
                (get result-set-concurrency concurrency)
                (get result-set-holdability cursors)))
            (do
              (when-not (and (get result-set-type result-type)
                             (get result-set-concurrency concurrency)
                             (get result-set-holdability cursors))
                (throw
                  (IllegalArgumentException.
                    (str "Unsupported result-set-type, or result-set-concurrency, is: "
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
            (str "Unsupported fetch-size. Expecting >= 0, is: " fetch-size))))
      (.setFetchSize stmt fetch-size))
    (when max-rows
      (when (< max-rows 0)
        (throw
          (IllegalArgumentException.
            (str "Unsupported max-rows. Expecting >= 0, is: " max-rows))))
      (.setMaxRows stmt max-rows))
    (when timeout
      (when (< timeout 0)
        (throw
          (IllegalArgumentException.
            (str "Unsupported timeout, Expecting >= 0, is: " timeout))))
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

(defmacro with-connection
  "Evaluates body in the context of an active connection to the database.
   (with-connection [conn conn]
     ... con-db ...)"
  [binding & body-and-or-options]
  (let [conn-var (first binding)
        [options body] (if (map? (first body-and-or-options))
                         [(first body-and-or-options) (rest body-and-or-options)]
                         [{} body-and-or-options])]
    `(let [spec# ~(second binding)
           ~conn-var (get-connection spec# ~options)]
       (with-open [conn# (lift-connection ~conn-var)]
         ~@body))))

(defmacro transactionally
  "Transactionally executes body.

   Reuse existing connection or create a new one if none exists, similar
   to `with-connection`.

   See also `with-connection`."
  [binding & body-and-or-options]
  (let [conn-var (first binding)
        ;;[options body] (if-not body [nil options])
        [options body] (if (map? (first body-and-or-options))
                         [(first body-and-or-options) (rest body-and-or-options)]
                         [{} body-and-or-options])]
    `(let [~conn-var (-> ~(second binding)
                         (bind-transaction ~options))]
       #_(with-open [conn# (lift-connection ~conn-var)])
       (do-transactionally
         ~conn-var ~options
         (fn [] ~@body)))))

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
  (with-open [stmt (prepare-query (lift-connection conn) query-spec params options)
              rs   (.executeQuery stmt)]
    (doall (result-set-seq rs {}))))

(defn execute!
  "Run an DML query (insert, update, delete) against the dabase.
   May return auto-increment keys."
  [conn query-spec & [params {:keys [return-keys] :as options}]]
  (let [query-spec (update query-spec :options merge options)]
    (with-open
      [^PreparedStatement stmt (create-prepared-statement (lift-connection conn) query-spec)]
      ;; TODO: better cond
      (if (and (vector? params)
               (or (map? (first params))
                   (vector? (first params))))
        (do
          (doseq [params params]
            (set-prepared-statement-params! stmt query-spec params)
            (.addBatch stmt))
          (let [cnt (into [] (.executeBatch stmt))]
            (collect-result stmt query-spec cnt)))
        (do
          (set-prepared-statement-params! stmt query-spec params)
          (let [cnt (.executeUpdate stmt)]
            (collect-result stmt query-spec cnt)))))))

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
