(defproject taxi-rides-clj "0.0.1-SNAPSHOT"
  :description "FIXME: write description"
  :dependencies [[org.clojure/clojure "1.8.0"]
                 [clojure.java-time "0.2.2"]
                 [org.clojure/data.json "0.2.6"]
                 [org.clojure/data.csv "0.1.3"]
                 [cc.qbits/spandex "0.3.4"]
                 [com.climate/claypoole "1.1.4"]
                 
                 [org.clojars.nikonyrh.utilities-clj "0.1.2"]]
  :javac-options ["-target" "1.8" "-source" "1.8" "-Xlint:-options"]
; :resource-paths ["resources/org.clojars.nikonyrh.utilities-clj-0.1.2-standalone.jar"]
  :aot [taxi-rides-clj.core]
  :main taxi-rides-clj.core)
