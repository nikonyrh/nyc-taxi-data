(defproject taxi-rides-clj "0.0.1-SNAPSHOT"
  :description "FIXME: write description"
  :dependencies [[org.clojure/clojure "1.8.0"]
                 [org.clojure/data.json "0.2.6"]
                 [org.clojure/data.csv "0.1.3"]
                 [cc.qbits/spandex "0.3.4"]
                 [org.clojure/core.async "0.3.441"]
                 [clojure.java-time "0.2.2"]
                 [com.climate/claypoole "1.1.4"]]
  :javac-options ["-target" "1.8" "-source" "1.8" "-Xlint:-options"]
  :aot [taxi-rides-clj.core]
  :main taxi-rides-clj.core)
