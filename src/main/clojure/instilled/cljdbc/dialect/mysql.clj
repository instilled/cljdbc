(ns instilled.cljdbc.dialect.mysql
  (:require
    [instilled.cljdbc :as jdbc])
  (:import
    [java.sql
     ResultSet
     PreparedStatement]))

(defrecord MysqlDialect []
  jdbc/ISQLDialect
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

(defn dialect
  []
  (MysqlDialect.))


;;(defn default-sequence-name-generator-fn
;;  "Generate sequence name base on table name, e.g `table1` as `table1_pk_seq`."
;;  [query-spec]
;;  (if-let [tn (get-in query-spec [:meta :table])]
;;    (.toLowerCase (str tn "_pk_seq"))
;;    (throw
;;      (IllegalStateException.
;;        (format "Unknown table-name! Cannot build default table sequence name. QuerySpec: %s"
;;          query-spec)))))
;;
;;
;;;; ##############################
;;;; ojdbc6
;;
;;(defrecord Oracle7ExecutionAspect
;;  [options]
;;
;;  j/IExecutionAspect
;;  (pre [conn query-spec params]
;;    ;; query sequence to get the primary keys ... sequence name is configurable
;;    (let [seqn (:sequence-name-generator-fn options query-spec)]
;;
;;      ))
;;
;;  (post [conn query-spec rs]
;;
;;    ))
;;
;;
;;;; ##############################
;;;; ojdbc7 (with inserted key support)
;;
;;(defrecord Oracle7ExecutionAspect
;;  [options]
;;
;;  j/IExecutionAspect
;;  (pre [conn query-spec params]
;;
;;    )
;;
;;  (post [conn query-spec rs]
;;
;;    )
;;
;;  )
;;
;;
;;(defn new-oracle-execution-aspect
;;  [])
