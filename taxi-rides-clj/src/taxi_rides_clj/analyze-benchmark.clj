(ns taxi-rides-clj.analyze-benchmark
  (:require [clojure.edn :as edn]
            [nikonyrh-utilities-clj.core :as u]
            [com.hypirion.clj-xchart :as c]))

(set! *warn-on-reflection* true)
(defn kw->str [kw] (-> kw str (subs 1)))
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(def group-keys [:db :filter :aggs])
(def results (->> ["sql_4770.edn" "es_6700k_16g.edn" "es_6700k_32g.edn" "es_6700k_64g.edn"]
                  (mapcat #(->> % (str "results/") slurp read-string))))
(def modes (into {} (for [key group-keys] [key (->> results (map key) set sort vec)])))

(let [group (apply juxt group-keys)]
  (def grouped-results (into {} (for [result results]
                                  [(group result)
                                   (->> result :stats (into (sorted-map)))]))))

; https://hypirion.github.io/clj-xchart/examples
(defn plot [res filter-kw aggs-kw]
  (->> (for [db (:db modes)]
         (let [data   (grouped-results [db filter-kw aggs-kw])
               d-keys (for [k (keys data) :when (-> k data pos?)] k)]
           (if (empty? d-keys)
             (u/my-println "Empty for [\"" db "\"" filter-kw " " aggs-kw "]")
             [db {:x    (map data d-keys)
                  :y    d-keys
                  :style {:marker-type :none}}])))
       (into {})
       (#(c/xy-chart % {:width  res
                        :height res
                      ; :title (str (kw->str filter-kw) "/" (kw->str aggs-kw))
                        :x-axis {:logarithmic? true}
                        :y-axis {:logarithmic? true}
                        :legend {:visible?     false}}))))

(defn plot-all [res]
  (->> (for [filter (modes :filter) agg (modes :aggs)]
         (plot res filter agg))
       (apply c/view)))

; (plot-all 400)
