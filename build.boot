(def artifact
  {:project
   'instilled/cljdbc

   :version
   "0.0.1-SNAPSHOT"

   :description
   "An yesql and funcool/java.jdbc inspired sql query facility targeting clojure.java.jdbc - uuhm sorry what?."

   :url
   "https://github.org/instilled/cljdbc"

   :scm
   {:name "git"
    :url  "git@githib.org:instilled/cljdbc.git"}

   :license
   {"Eclipse Public License - v 1.0" "https://www.eclipse.org/legal/epl-v10.html"}})

(set-env!
  :dependencies
  '[[org.clojure/java.jdbc                           "0.5.8"]

    ;; provided
    [com.zaxxer/HikariCP                             "2.4.5"
     :scope "provided"]
    [org.clojure/clojure                             "1.8.0"
     :scope "provided"]

    ;; test dependencies
    [adzerk/boot-test                                "1.1.1"
     :scope "test"]]

  :source-paths
  #{"src/main/clojure"}

  :resource-paths
  #{"src/main/resources"}

  :target-path
  "target"

  ;;:deploy-repos {}
  )

(require '[adzerk.boot-test :refer :all])

(task-options!
  pom artifact
  test       {:filters #{'(not (-> % meta :integration))}})

(deftask test1
  "Add test sources and resources to the classpath."
  []
  (merge-env!
    :source-paths
    #{"src/test/clojure"}

    :resource-paths
    #{"src/test/resources"})
  identity)

(deftask mysql
  "Add mysql to the classpath."
  []
  (set-env!
    :dependencies
    #(conj % '[mysql/mysql-connector-java "5.1.31" :scope "test"]))
  (task-options!
    test {:filters #{'(-> % meta :mysql)}}))

(deftask oracle
  "Add oracle to the classpath."
  []
  (set-env!
    :dependencies
    #(conj % '[org.oracle/ojdbc7 "12.1.0.2" :scope "test"]))
  (task-options!
    test {:filters #{'(-> % meta :oracle)}}))

(replace-task!
  [t test]    (fn [& xs] (comp (test1) (apply t xs))))
