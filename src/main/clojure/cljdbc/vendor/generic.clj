(ns cljdbc.vendor.generic
  (:require
    [cljdbc :as jdbc])
  (:import
    [java.sql
     ResultSet
     PreparedStatement]))

(defrecord GenericVendor []
  jdbc/ISQLVendor
  (rs-col-name
    [this rs rsmeta i query-spec]
    (-> (.getColumnLabel rsmeta i) (.toLowerCase) (keyword)))
  (rs-col-value
    [this rs rsmeta i query-spec]
    (jdbc/result-set-read-column (.getObject rs i) rsmeta i))
  (returning
    [this rs query-spec returning-cols]
    (jdbc/process-result-set rs query-spec this))
  (returning-count
    [this cnt]
    cnt))

(defn extension
  []
  (GenericVendor.))
