(ns instilled.cljdbc.cp.hikari
  (:import
    [com.zaxxer.hikari
     HikariConfig
     HikariDataSource]
    [java.util
     Properties]))

;; see https://github.com/brettwooldridge/HikariCP#configuration-knobs-baby

(def available-properties
  {;; required
   :dataSourceClassName nil
   :jdbcUrl nil

   ;; optional
   :username nil
   :password nil
   :autoCommit nil
   :connectionTimeout nil
   :idleTimeout nil
   :maxLifetime nil
   :connectionTestQuery nil
   :minimumIdle nil
   :maximumPoolSize nil
   :metricRegistry nil
   :healthCheckRegistry nil
   :poolName nil
   :initializationFailFast nil
   :isolateInternalQueries nil
   :allowPoolSuspension nil
   :readOnly nil
   :registerMbeans nil
   :catalog nil
   :connectionInitSql nil
   :driverClassName nil
   :transactionIsolation nil
   :validationTimeout nil
   :leakDetectionThreshold nil

   ;; programmatic only
   :dataSource nil
   :threadFactory nil})

(defn make-pool
  [jdbc-url options]
  (let [config (doto (HikariConfig.
                       (reduce
                         (fn [p [k v]] (.setProperty p k (name v)))
                         (Properties.)
                         (dissoc
                           options
                           :dataSource
                           :threadFactory)))
                 (.setJdbcUrl jdbc-url))]
    (when-let [ds (:dataSource options)]
      (.setDataSource config ds))
    (when-let [tf (:threadFactory options)]
      (.setThreadFactory config tf))
    (HikariDataSource. config)))
