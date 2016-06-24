(ns instilled.cljdbc.internal.oracle
  (:require
    [instilled.cljdbc :as j])
  (:import
    [java.sql
     ResultSet
     PreparedStatement]))


(defn default-sequence-name-generator-fn
  "Generate sequence name base on table name, e.g `table1` as `table1_pk_seq`."
  [query-spec]
  (if-let [tn (get-in query-spec [:meta :table])]
    (.toLowerCase (str tn "_pk_seq"))
    (throw
      (IllegalStateException.
        (format "Unknown table-name! Cannot build default table sequence name. QuerySpec: %s"
          query-spec)))))


;; ##############################
;; ojdbc6

(defrecord Oracle7ExecutionAspect
  [options]

  j/IExecutionAspect
  (pre [conn query-spec params]
    ;; query sequence to get the primary keys ... sequence name is configurable
    (let [seqn (:sequence-name-generator-fn options query-spec)]

      ))

  (post [conn query-spec rs]

    ))


;; ##############################
;; ojdbc7 (with inserted key support)

(defrecord Oracle7ExecutionAspect
  [options]

  j/IExecutionAspect
  (pre [conn query-spec params]

    )

  (post [conn query-spec rs]

    )

  )


(defn new-oracle-execution-aspect
  [])
