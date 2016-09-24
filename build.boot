(def artifact
  {:project
   'cljdbc/cljdbc

   :version
   "0.0.1-SNAPSHOT"

   :description
   "A yesql, funcool/java.jdbc and clojure.java.jdbc inspired sql query facility."

   :url
   "https://github.org/instilled/cljdbc"

   :scm
   {:name "git"
    :url  "git@githib.org:instilled/cljdbc.git"}

   :license
   {"Eclipse Public License - v 1.0" "https://www.eclipse.org/legal/epl-v10.html"}})

(set-env!
  :dependencies
  '[;; provided
    [com.zaxxer/HikariCP                             "2.4.5"
     :scope "provided"]
    [org.apache.tomcat/tomcat-jdbc                   "8.5.5"
     :score "provided"]
    [org.clojure/clojure                             "1.8.0"
     :scope "provided"]

    [org.slf4j/slf4j-api                             "1.7.21"
     :scope "provided"]
    [org.apache.logging.log4j/log4j-slf4j-impl       "2.6.2"
     :scope "provided"]
    [org.apache.logging.log4j/log4j-core             "2.6.2"
     :scope "provided"]
    [org.apache.logging.log4j/log4j-api              "2.6.2"
     :scope "provided"]

    ;; test dependencies
    [org.clojure/java.jdbc                           "0.5.8"
     :scope "test"]
    [adzerk/boot-test                                "1.1.1"
     :scope "test"]]

  :source-paths
  #{"src/main/clojure"}

  ;;:resource-paths
  ;;#{"src/main/resources"}

  :target-path
  "target"

  ;;:deploy-repos {}
  )

(require '[adzerk.boot-test :refer :all])

(task-options!
  pom  artifact
  test {:filters #{'(not (-> % meta :integration))}}
  jar  {:file (str "cljdbc-" (:version artifact) ".jar")})

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
    #(conj % '[mysql/mysql-connector-java "5.1.39" :scope "test"]))
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

(deftask postgres
  "Add postgres to the classpath."
  []
  (set-env!
    :dependencies
    #(conj % '[org.postgresql/postgresql "9.4.1210.jre7" :scope "test"]))
  (task-options!
    test {:filters #{'(-> % meta :postgres)}}))

(deftask sqlite
  "Add postgres to the classpath."
  []
  (set-env!
    :dependencies
    #(conj % '[org.xerial/sqlite-jdbc "3.8.11.2" :scope "test"]))
  (task-options!
    test {:filters #{'(-> % meta :sqlite)}}))

(deftask remove-ignored
  []
  (sift
    :invert true
    :include #{#".*\.swp" #".gitkeep"}))

(deftask build
  "Build the shizzle."
  []
  (merge-env!
    :resource-paths
    #{"src/main/clojure"})
  (comp
    (remove-ignored)
    (pom)
    (jar)
    (target)
    (install)))

(deftask deploy
  []
  (push
    :gpg-sign false
    :repo "clojars"))

(replace-task!
  [t test] (fn [& xs] (comp (test1) (apply t xs)))
  [r repl] (fn [& xs] (comp (test1) (apply r xs))))
