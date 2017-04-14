(ns taxi-rides-clj.analyze-benchmark
  (:require [clojure.edn :as edn]
            [nikonyrh-utilities-clj.core :as u]
            [com.hypirion.clj-xchart :as c])
  (:import java.awt.image.BufferedImage))

(set! *warn-on-reflection* true)
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(def group-keys [:db :filter :aggs])
(def results (->> ["results_es.edn"] (mapcat #(-> % slurp read-string))))
(def modes   (into {} (for [type group-keys] [type (->> results (map type) set sort vec)])))

(defn kw->str [kw] (-> kw str (subs 1)))

(let [group (apply juxt group-keys)]
  (def grouped-results (into {} (for [result results]
                                  [(group result)
                                   (->> result :stats (into (sorted-map)))]))))

(defn plot [res filter-kw aggs-kw]
  (->> (for [db (:db modes)]
         (let [data   (grouped-results [db filter-kw aggs-kw])
               d-keys (for [k (keys data) :when (-> k data pos?)] k)]
           [db [d-keys
                (map data d-keys)]]))
       (into {})
       (#(c/xy-chart % {:width  res
                        :height res
                      ; :title (str (kw->str filter-kw) "/" (kw->str aggs-kw))
                        :x-axis {:logarithmic? true}
                        :y-axis {:logarithmic? true}
                        :legend {:visible?     false}}))))

(defn plot-all [res]
  (let [n-filters  (-> modes :filter count)
        n-aggs     (-> modes :aggs   count)]
    (->> (for [i-filter (range n-filters) i-aggs (range n-aggs)]
           (plot res (-> modes :filter (nth i-filter))
                     (-> modes :aggs   (nth i-aggs))))
         (apply c/view))))

; (plot-all 400)
