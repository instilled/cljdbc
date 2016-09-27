(ns cljdbc.util
  (:require
    [cljdbc :as j])
  (:import
    [java.sql
     ResultSet
     PreparedStatement]))

(def jdbc-string->driver-map
  "Map used to map jdbc url to driver map."
  {"mysql"      "com.mysql.jdbc.Driver"
   "oracle"     "oracle.jdbc.OracleDriver"
   "postgresql" "org.postgresql.Driver"
   "sqlite"     "org.sqlite.JDBC"
   "h2"         "org.h2.Driver"})

(defn default-driver-class-for-url
  [jdbc-url]
  (if-let [[k clazz] (->> jdbc-string->driver-map
                          (filter (fn [[k v]] (.contains jdbc-url k)))
                          (first))]
    clazz
    (throw (IllegalStateException. "Could not determine driver!"))))
