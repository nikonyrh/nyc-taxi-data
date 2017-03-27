(ns taxi-rides-clj.benchmark
  (:require [java-time :as t]
            [clojure.data.json :as json]
            [qbits.spandex :as s]
            [com.climate.claypoole :as cp]
            
            [taxi-rides-clj.utilities :as u]))

(defn get-fn-name [f] (->> f str (re-find #"\$(.+)@") second))
(defn percentiles [ps values]
  (let [values   (-> values sort vec)
        n-values (dec (count values))]
    (->> (zipmap ps (map #(get values (Math/round (* % 0.01 n-values))) ps))
         (into (sorted-map)))))

(let [km-per-degrees (/ 40075.0 360.0)
      random-obj     (java.util.Random.)
      randn          (fn [] (locking random-obj
                              (.nextGaussian random-obj)))
      
      ; Midpoint at Madison Square, radius of 5.11 km extends to the tip of Manhattan
      nyc-pos {:lat  40.742054 :lon -73.987984}
      nyc-rad (/ 5.11 km-per-degrees)]
  
  (defn gen-pos "A random position around Madison Square" []
    (into {} (for [[key value] nyc-pos] [key (->> (randn) (* nyc-rad) (+ value))])))
  
  (defn rect-generator "Generates rectangles around Madison Square of given size" [size-km]
    (let [half-size-deg (/ size-km km-per-degrees 2.0)
          corner        (fn [mid-pos signs] (into {} (map (fn [key sign]
                                                            [key (sign (key mid-pos) half-size-deg)])
                                                       [:lat :lon] signs)))]
      (fn [] (let [mid-pos (gen-pos)]
               {:top_left     (corner mid-pos [+ -])
                :bottom_right (corner mid-pos [- +])})))))

(defn range-generator "A random interval"
  ([start-from start-to interval-len] (range-generator start-from start-to interval-len identity))
  ([start-from start-to interval-len postprocess]
   (let [start-range (- start-to start-from)]
     (fn [] (let [start-range (-> (rand) (* start-range) (+ start-from))]
              (mapv postprocess [start-range (+ start-range interval-len)]))))))

(defn date-range-generator "A random interval of dates" [start-from-str start-to-str interval-len-days]
  (let [utc-zone             (java.time.ZoneId/of "UTC")
        int-parser           (fn [^String s] (Integer. s))
        date-parser         #(->> % (re-seq #"[0-9]+") (map int-parser) (apply t/local-date-time))
        to-epoch             (fn [^java.time.LocalDateTime dt] (-> dt (.atZone utc-zone) ((fn [^java.time.ZonedDateTime dt] (.toEpochSecond dt)))))
        s-in-day             (* 24 60 60)
        ms-in-day            (* s-in-day 1000)
        start-from           (-> start-from-str date-parser to-epoch (quot s-in-day))
        start-to             (-> start-to-str   date-parser to-epoch (quot s-in-day))]
    (range-generator start-from start-to interval-len-days (comp (partial * ms-in-day) int))))
;(->> (date-range-generator "2013-01-01" "2013-01-08" 2) (repeatedly 3))

(def filters
  {:from-to-time
   (let [rect-fn       (rect-generator 2)
         date-range-fn (date-range-generator "2013-01-01" "2016-12-01" 14)]
     (fn [] {:size 0
             :query
             {:bool
               {:filter
                 [{:range            {:pickup-dt  (zipmap [:gte :lte] (date-range-fn))}}
                  {:geo_bounding_box {:pickup-pos (rect-fn)}}]}}}))
   
   :travelh-npassengers
   (let [travelh-fn     (range-generator 0 3 0.25)
         npassengers-fn (fn [] (rand-nth [1 2 3 4 5 6]))]
     (fn [] {:size 0
             :query
             {:bool
               {:filter
                 [{:range {:travel-h     (zipmap [:gte :lte] (travelh-fn))}}
                  {:term  {:n-passengers (npassengers-fn)}}]}}}))
   
   :time-pickupday-paidtip
   (let [date-range-fn (date-range-generator "2013-01-01" "2016-10-01" 60)
         pickupday-fn  (fn [] (rand-nth [1 2 3 4 5 6 7]))
         paidtip-fn    (range-generator 0 15 2)]
     (fn [] {:size 0
             :query
             {:bool
               {:filter
                 [{:range {:pickup-dt  (zipmap [:gte :lte] (date-range-fn))}}
                  {:range {:paid-tip   (zipmap [:gte :lte] (paidtip-fn))}}
                  {:term  {:pickup-day (pickupday-fn)}}]}}}))})

; (->> filters vals (mapv #(%)) (json/write-str) println)

(let [make-fn (fn [aggs]
                (fn [client query]
                  (s/request client {:url "taxicab-*/_search" :method :post
                                     :body (if-not aggs query (assoc query :aggs aggs))})))]
  (def aggregations
    {:es-count
     (make-fn nil)
  
     :paid-total-stats-by-time-of-day 
     (make-fn
       {:time-of-day
        {:histogram
         {:field "pickup-time" :interval 1}
         :aggs {:paid-total-stats {:stats {:field "paid-total"}}}}})
    
     :paid-total-per-km-avg-by-company
     (make-fn
       {:company
        {:terms
         {:field "company" :size 10}
         :aggs
          {:paid-total-per-person-avg
           {:avg {:script
                  {:lang "painless"
                   :inline "doc['paid-total'].value / doc['travel-km'].value"}}}}}})

     :speed-kmph-percentiles
     (make-fn
       {:paid-total-perc
        {:percentiles {:field "speed-kmph"
                       :percents [5 10 25 50 75 90 95]}}})}))


; (->> ((:from-to-time filters)) (json/write-str) println)
; (->> ((:time-pickupday filters)) ((:paid-total-per-km-avg-by-company aggregations) (make-client)))

(def es-config
  {:server (u/getenv "ES_SERVER" "192.168.0.100:9200")})

(defn make-client [] (s/client {:hosts [(str "http://" (:server es-config))]}))

(defn benchmark-es [filter-generators aggregations n-tries n-parallels]
  (let [percentiles (partial percentiles [5 10 25 50 75 90 95])
        client    (make-client)
        benchmark (fn [filter-generator aggregation]
                    (let [t-start (System/nanoTime)
                          result  (aggregation client (filter-generator))
                          t-end   (System/nanoTime)]
                      {:took (- t-end t-start)
                     ; :result result
                       :hits (-> result :body :hits :total)}))]
    (doall (for [n-parallel                     n-parallels
                 [filter-name filter-generator] filter-generators
                 [aggregation-name aggregation] aggregations]
             (let [t-start   (System/nanoTime)
                   results   (doall (cp/upfor n-parallel [i (range n-tries)] (benchmark filter-generator aggregation)))
                   took      (-> (System/nanoTime) (- t-start) (* 0.000001) Math/round)
                   avg       (/ took n-tries 1.0)
                   _         (u/my-println "Finished " [filter-name aggregation-name n-parallel] " in " took " ms, avg " avg " ms")
                   
                   p-took    (->> results (map #(-> % :took (* 0.001) Math/round (* 0.001))) percentiles)
                   p-hits    (->> results (map :hits) percentiles)]
               {:n-parallel    n-parallel
                :filter        filter-name
                :aggs          aggregation-name
                :avg           avg
                :stats         (->> [p-took p-hits] (map vals) (apply zipmap) (into (sorted-map)))})))))

(comment
  (do (println "")
      (clojure.pprint/pprint (benchmark-es filters aggregations 100 [1]))))

