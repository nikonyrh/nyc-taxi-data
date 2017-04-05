(ns taxi-rides-clj.benchmark
  (:require [java-time :as t]
            [clojure.data.json :as json]
            [qbits.spandex :as s]
            [com.climate.claypoole :as cp]
            
            [taxi-rides-clj.utilities :as u]))

(defn percentiles [ps values]
  (let [values   (-> values sort vec)
        n-values (dec (count values))]
    (->> (zipmap ps (map #(get values (Math/round (* % 0.01 n-values))) ps))
         (into (sorted-map)))))

(def es-config
  {:server (u/getenv "ES_SERVER" "192.168.0.100:9200")})

(defn make-client [] (s/client {:hosts [(str "http://" (:server es-config))]}))

(defn run-query [client query]
  (s/request client {:url "taxicab-*/_search" :method :post
                     :body query}))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

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
              (->> [start-range (+ start-range interval-len)]
                (map postprocess)
                (zipmap [:gte :lte])))))))

(defn date-range-generator "A random interval of dates" [start-from-str start-to-str interval-len-days]
  (let [utc-zone          (java.time.ZoneId/of "UTC")
        int-parser        (fn [^String s] (Integer. s))
        date-parser      #(->> % (re-seq #"[0-9]+") (map int-parser) (apply t/local-date-time))
        to-epoch          (fn [^java.time.LocalDateTime dt] (-> dt (.atZone utc-zone) ((fn [^java.time.ZonedDateTime dt] (.toEpochSecond dt)))))
        s-in-day          (* 24 60 60)
        ms-in-day         (* s-in-day 1000)
        start-from        (-> start-from-str date-parser to-epoch (quot s-in-day))
        start-to          (-> start-to-str   date-parser to-epoch (quot s-in-day))]
    (range-generator start-from start-to interval-len-days (comp (partial * ms-in-day) int))))
; (->> (date-range-generator "2013-01-01" "2013-01-08" 2) (repeatedly 3))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(def filters
  {:from-to-time
   (let [rect-fn       (rect-generator 2)
         date-range-fn (date-range-generator "2013-01-01" "2016-12-01" 14)]
     (fn [] {:size 0
             :query
             {:bool
               {:filter
                 [{:range            {:pickup-dt  (date-range-fn)}}
                  {:geo_bounding_box {:pickup-pos (rect-fn)}}]}}}))
   
   :travelh-npassengers
   (let [travelh-fn     (range-generator 0 3 0.25)
         npassengers-fn (fn [] (rand-nth [1 2 3 4 5 6]))]
     (fn [] {:size 0
             :query
             {:bool
               {:filter
                 [{:range {:travel-h     (travelh-fn)}}
                  {:term  {:n-passengers (npassengers-fn)}}]}}}))
   
   :time-pickupday-paidtip
   (let [date-range-fn (date-range-generator "2013-01-01" "2016-10-01" 60)
         pickupday-fn  (fn [] (rand-nth [1 2 3 4 5 6 7]))
         paidtip-fn    (range-generator 0 15 2)]
     (fn [] {:size 0
             :query
             {:bool
               {:filter
                 [{:range {:pickup-dt  (date-range-fn)}}
                  {:range {:paid-tip   (paidtip-fn)}}
                  {:term  {:pickup-day (pickupday-fn)}}]}}}))})

; (->> filters vals (mapv #(%)) (json/write-str) println)

(let [make-fn (fn [aggs] (fn [query] (if-not aggs query (assoc query :aggs aggs))))]
  (def aggregations
    {:count
     (make-fn nil)
  
     :paid-total-stats-by-time-of-day 
     (make-fn
       {:time-of-day
        {:histogram
         {:field "pickup-time" :interval 2}
         :aggs {:paid-total-stats {:stats {:field "paid-total"}}}}})
    
     :paid-total-per-km-stats-by-company-and-day
     (make-fn
       {:company
        {:terms
         {:field "company" :size 10}
         :aggs
          {:day
           {:date_histogram
            {:field "pickup-dt" :interval "1d"}
            :aggs
             {:paid-total-per-person-avg
              {:stats {:script
                       {:lang "painless"
                        :inline "doc['paid-total'].value / doc['travel-km'].value"}}}}}}}})

     :speed-kmph-percentiles-by-day
     (make-fn
       {:day
        {:date_histogram
         {:field "pickup-dt" :interval "1d"}
         :aggs
          {:speed-kmph-perc
           {:percentiles {:field "speed-kmph"
                          :percents [5 10 25 50 75 90 95]}}}}})}))


; (->> ((:from-to-time filters)) (json/write-str) println)
; (->> ((:time-pickupday-paidtip filters)) ((:paid-total-per-km-avg-by-company aggregations)) (json/write-str) println)
; (->> ((:time-pickupday-paidtip filters)) ((:paid-total-per-km-avg-by-company aggregations)) (run-query (make-client)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(defn benchmark-es [filter-generators aggregations n-tries n-parallels]
  (let [percentiles (partial percentiles [5 10 25 50 75 90 95])
        client    (make-client)
        benchmark (fn [filter-generator aggregation]
                    (let [t-start (System/nanoTime)
                          result  (->> (filter-generator) aggregation (run-query client))
                          t-end   (System/nanoTime)]
                      {:took (- t-end t-start)
                     ; :result result
                       :hits (-> result :body :hits :total)}))]
    (doall (for [n-parallel                     n-parallels
                 [filter-name filter-generator] filter-generators
                 [aggregation-name aggregation] aggregations]
             (let [_         nil ; (u/my-println "Started " [filter-name aggregation-name n-parallel])
                   t-start   (System/nanoTime)
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
      (clojure.pprint/pprint (benchmark-es filters aggregations 50 [1]))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(defn to-sql-field [kw] (-> kw name (clojure.string/replace #"-" "_")))

(defn epoch-to-str [epoch]
  (-> epoch t/instant str (subs 0 19) (clojure.string/replace #"T" " ")))

(defn parse-filter [f]
  (let [[f-type f-rest] (first f)
        [field  f-args] (first f-rest)]
    [f-type (to-sql-field field) f-args]))

(defn col-a-as-b [a b] (if (= a b) a (format "%s AS %s" a b)))

(let [group-by? #{:terms :histogram :date_histogram}
      sql-fn?   #{:avg :sum :min :max :count}
      to-sql-fn (fn [agg-type field]
                  (let [sql-agg    (to-sql-field agg-type)
                        tidy-field (clojure.string/replace field #" [^ ] " "_")]
                    {:fun (col-a-as-b field tidy-field)
                     :tmp tidy-field
                     :col (format "%s(%s) AS %s_%s" sql-agg tidy-field sql-agg tidy-field)}))
      
      multi-fns  {:stats [:sum :min :max]}]  ; Leaving out count and avg from this...
  (defn parse-aggs [agg]
    (loop [agg agg result ()]
      (if-not agg
        (if-not (empty? result)
          (apply merge-with concat result)
          {:group-by [{:agg "COUNT(1)" :col "n_rows"}]})
        (let [_         (assert (= 1 (count agg)) "Currently only supports one aggregation / hierarchy level")
              [key agg] (-> agg first)
              agg-type  (-> agg keys set (disj :aggs) first)
              agg-data  (agg agg-type)
              field     (-> agg-data (#(or (some-> % :field to-sql-field)
                                           (-> % :script :inline
                                               (clojure.string/replace #"doc\['([^']+)'\]\.value" "$1")
                                               (clojure.string/replace #"([a-z])-([a-z])"         "$1_$2")))))
              
              gen-col-name (fn [field name interval] (format "%s_%s_%s" field name interval))
              
              this-agg  
              (if (group-by? agg-type)
                {:group-by 
                 (case agg-type
                   :terms
                   [{:agg field :col field}]
                   
                   :histogram
                   (let [interval (-> agg-data :interval float)]
                     [{:agg (format "FLOOR(%s / %.1f)" field interval)
                       :col (gen-col-name field "hist" (:interval agg-data))}])
                   
                   :date_histogram
                   (let [_            (assert (= \d (-> agg-data :interval last)) "Only days are supported for now")
                         interval     (-> agg-data :interval butlast (#(Float. (apply str %))))
                         interval-div (if (= 1.0 interval) "" (format " / %.1f" interval))]
                     [{:agg (format "FLOOR(CAST(CAST(%s AS datetime) AS float)%s)" field interval-div)
                       :col (gen-col-name field "datehist" (:interval agg-data))}]))}
                
                {:metrics
                 (cond
                   (sql-fn? agg-type)
                   [(to-sql-fn agg-type field)]
                   
                   (multi-fns agg-type)
                   (for [sub-agg-type (multi-fns agg-type)]
                     (to-sql-fn sub-agg-type field))
                   
                   (= :percentiles agg-type)
                   (for [percent (:percents agg-data)]
                     (let [gen-field (format "%s_p%02d" field percent)]
                       {:fun field
                        :tmp (format "PERCENTILE_DISC(0.%02d) WITHIN GROUP (ORDER BY %s) OVER (PARTITION BY {GROUP_BY}) AS %s" percent field gen-field)
                        :col (format "MIN(%s) AS %s" gen-field gen-field)})))})]
          (recur (:aggs agg) (conj result this-agg)))))))


; (->> ((:time-pickupday-paidtip filters)) ((:paid-total-per-km-stats-by-company-and-day aggregations)) clojure.pprint/pprint)
; (->> ((:time-pickupday-paidtip filters)) ((:speed-kmph-percentiles-by-day aggregations)) :aggs parse-aggs clojure.pprint/pprint)
; (->> ((:time-pickupday-paidtip filters)) ((:paid-total-per-km-stats-by-company-and-day aggregations)) :aggs parse-aggs clojure.pprint/pprint)
; (->> ((:count aggregations) {}) :aggs parse-aggs clojure.pprint/pprint)

(defn es-to-sql [query]
  (let [join        #(clojure.string/join % %2)
        wheres (for [[f-type field f-args] (->> query :query :bool :filter (map parse-filter))]
                  (let [convert (if (clojure.string/ends-with? field "_dt") epoch-to-str identity)]
                    (case f-type
                      :range {:str [(str field " BETWEEN ? AND ?")] :args (mapv (comp convert f-args) [:gte :lte])}
                      :term  {:str [(str field " = ?")]             :args [f-args]}
                      
                      :geo_bounding_box
                      {:str (let [field (subs field 0 (- (count field) 4))]
                              [(str field "_lat BETWEEN ? AND ?")
                               (str field "_lon BETWEEN ? AND ?")])
                       
                       ; This is truly terrible, but in essense we are picking just min and max of the bounding box's latitude and longitude
                       ; and I didn't feel like writing lookup keywords to the four corners in the correct order.
                       :args (mapcat #(apply (juxt min max) (map (comp % f-args) [:top_left :bottom_right])) [:lat :lon])})))
        where-str    (->> (mapcat :str wheres) (join " AND\n      ") (str " WHERE\n      "))
        where-args   (mapcat :args wheres)
        
        agg-params   (-> query :aggs parse-aggs)
        get-params   (fn [param-type param-key] (->> agg-params param-type (map param-key)))
        
        grb-aggs     (get-params :group-by :agg)
        grb-cols     (get-params :group-by :col)
        
        mtr-funs     (get-params :metrics  :fun)
        mtr-tmps     (get-params :metrics  :tmp)
        mtr-cols     (get-params :metrics  :col)
        
        inner-sql    (str "\n    SELECT\n      "
                            (join ",\n      " (concat
                                                (map col-a-as-b grb-aggs grb-cols)
                                                (-> mtr-funs set sort)))
                           "\n    FROM taxi_trips"
                           "\n   " where-str)
        
        outer-sql  (if (= (first grb-cols) "n_rows")
                     ; A special case when only row count is calculated
                     (-> inner-sql (clojure.string/replace "    " "") (str "\n\n"))
                     
                     ; The generic case with a few levels of aggregations
                     (-> (str "\nSELECT\n  " (join ",\n  " (concat grb-cols mtr-cols ["SUM(1) AS n_rows"]))
                              "\nFROM ("
                              "\n  SELECT\n    " (join ",\n    " (concat grb-cols (-> mtr-tmps set sort)))
                              "\n  FROM ("
                              inner-sql
                              "\n  ) t"
                              "\n) t GROUP BY {GROUP_BY}\n\n")
                         (clojure.string/replace "{GROUP_BY}" (join ", " grb-cols))))]
    (into [outer-sql] where-args)))

; (->> ((:time-pickupday-paidtip filters)) ((:speed-kmph-percentiles-by-day aggregations)) clojure.pprint/pprint)
; (->> ((:time-pickupday-paidtip filters)) ((:paid-total-per-km-stats-by-company-and-day aggregations)) es-to-sql print)
; (->> ((:time-pickupday-paidtip filters)) ((:speed-kmph-percentiles-by-day aggregations)) es-to-sql print)
; (->> ((:time-pickupday-paidtip filters)) ((:paid-total-stats-by-time-of-day  aggregations)) es-to-sql print)
; (->> ((:from-to-time filters)) ((:paid-total-per-km-stats-by-company-and-day aggregations)) es-to-sql print)


(comment
  (->> (for [[filter-name filter-generator] filters
             [aggregation-name aggregation] aggregations]
         (->> (filter-generator) aggregation es-to-sql
              ((fn [[sql & args]] (apply format (clojure.string/replace sql #"\?" "%s")
                                                (map #(if (string? %) (str \' % \') (str %)) args))))))
       (apply str)
       (spit "out.sql")))


(comment
  (->> (for [[filter-name filter-generator] filters
             [aggregation-name aggregation] aggregations]
         (aggregation (filter-generator)))
       (map json/write-str)
       (#(concat % ["" ""]))
       (clojure.string/join "\n")
       (spit "out.json")))
      
