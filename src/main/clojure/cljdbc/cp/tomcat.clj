(ns cljdbc.cp.tomcat
  (:require
    [cljdbc.util :as jdbcu])
  (:import
    [org.apache.tomcat.jdbc.pool
     PoolProperties
     DataSource]
    [java.util
     Properties]
    [clojure.lang
     Reflector]))

(def available-properties
  {;; set by the pool factory method below
   :url nil
   :driverClassName nil

   ;; others
   :defaultAutoCommit nil
   :defaultReadOnly nil
   :defaultTransactionIsolation nil
   :defaultCatalog nil
   :connectionProperties nil
   :initialSize nil
   :maxActive nil
   :maxIdle nil
   :minIdle nil
   :maxWait nil
   :validationQuery nil
   :validationQueryTimeout nil
   :validatorClassName nil
   :validator nil
   :testOnBorrow nil
   :testOnReturn nil
   :testWhileIdle nil
   :timeBetweenEvictionRunsMillis nil
   :numTestsPerEvictionRun nil
   :minEvictableIdleTimeMillis nil
   :accessToUnderlyingConnectionAllowed nil
   :removeAbandoned nil
   :removeAbandonedTimeout nil
   :logAbandoned nil
   :name nil
   :password nil
   :username nil
   :validationInterval nil
   :jmxEnabled nil
   :initSQL nil
   :testOnConnect nil
   :jdbcInterceptors nil
   :fairQueue nil
   :useEquals nil
   :abandonWhenPercentageFull nil
   :maxAge nil
   :useLock nil
   :interceptors nil
   :suspectTimeout nil
   :dataSource nil
   :dataSourceJNDI nil
   :alternateUsernameAllowed nil
   :commitOnReturn nil
   :rollbackOnReturn nil
   :useDisposableConnectionFacade nil
   :logValidationErrors nil
   :propagateInterruptState nil
   :ignoreExceptionOnPreLoad nil})

(defn make-setter
  [m]
  (let [m (name m)]
    (str "set" (-> m (.substring 0 1) (.toUpperCase)) (.substring m 1))))

(defn invoke
  [obj method arg]
  (Reflector/invokeInstanceMethod obj method (into-array Object [arg])))

(defn make-pool
  [jdbc-url options]
  (let [driver (get options :driverClassName
                 (jdbcu/default-driver-class-for-url jdbc-url))]
    (DataSource.
      (reduce
        (fn [o [m v]] (invoke o (make-setter m) v) o)
        (PoolProperties.)
        (assoc options
          :driverClassName driver
          :url jdbc-url)))))
