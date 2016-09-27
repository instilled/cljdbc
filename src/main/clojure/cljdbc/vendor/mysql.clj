(ns cljdbc.vendor.mysql
  (:require
    [cljdbc :as jdbc])
  (:import
    [java.sql
     ResultSet
     PreparedStatement]))

(defrecord MySQLVendor []
  jdbc/ISQLVendor
  (rs-col-name
    [this rs rsmeta i query-spec]
    (-> (.getColumnLabel rsmeta i) (.toLowerCase) (keyword)))
  (rs-col-value
    [this rs rsmeta i query-spec]
    (jdbc/result-set-read-column (.getObject rs i) rsmeta i))
  (returning
    [this rs query-spec returning-cols]
    (let [k (first returning-cols)]
      (->> (jdbc/process-result-set rs query-spec this)
           (map (fn [{:keys [generated_key]}] {k generated_key})))))
  (returning-count
    [this cnt]
    cnt))

(defn extension
  []
  (MySQLVendor.))
