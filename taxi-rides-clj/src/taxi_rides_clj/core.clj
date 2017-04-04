(ns taxi-rides-clj.core
  (:require [java-time :as t]
            [clojure.data.json :as json]
            [clojure.data.csv :as csv]
            [clojure.string :as string]
            [clojure.java.io :as io]
            [qbits.spandex :as s]
            [com.climate.claypoole :as cp]
            
            [taxi-rides-clj.utilities :as u]
            [taxi-rides-clj.benchmark-es :as b-es])
  (:use clojure.set)
  (:import java.util.zip.GZIPInputStream)
  (:gen-class :main true))


(set! *warn-on-reflection* true)

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(def data-folder (str (u/getenv "TAXI_DATA_FOLDER" "/home/wrecked/projects/taxi-rides/data") "/"))
(defn find-files [re] (->> data-folder io/file file-seq (filter #(re-find re (.getName ^java.io.File %))) sort))

(defmacro make-parser [f]
  `(fn [^String v#] (if-not (empty? v#)
                       (try (~f v#)
                            (catch Exception e#
                              (do (u/my-println "Invalid value '" v# "' for parser " (quote ~f))
                                  (u/my-println "Exception: " e#)))))))

(let [int-parser        (make-parser Integer.)
      double-parser     (make-parser Double.)
      digits            (into #{\T} (for [i (range 10)] (first (str i))))
      assert-len       #(if (= (count %2) %) %2 (throw (Exception. (str "Expected length " % ", got '" %2 "'!"))))
      basic-parser     #(->> % (re-seq #"[0-9]+") (map int-parser) (apply t/local-date-time))]

  (def parsers
    {:int               int-parser
     :float             double-parser
     :latlon          #(if-not (= % "0") (double-parser %))
     :keyword         #(let [s (string/trim %)] (if-not (empty? s) s))
     :basic-datetime   (make-parser basic-parser)})

  (defn datetime-to-str [datetime]
    (let [d (->> datetime str (filter digits) (apply str))
          d (if (and (= (count d) 13) (= (nth d 8) \T))
              (str d "00")
              d)]
       (str (assert-len 15 d) "Z")))
 
  (defn day-of-week [^java.time.LocalDateTime datetime]
     (-> datetime .getDayOfWeek .getValue)))

(let [[header & rows] (-> (str data-folder "central_park_weather.csv") slurp csv/read-csv)
      cols-to-keep #{"DATE" "PRCP" "SNWD" "SNOW" "TMAX" "TMIN" "AWND"}
      n-cols       (count header)
      avg-wind-fix (fn [row] (update row :weather-AWND #(if-not (neg? %) %)))
      rows (for [row rows :when (>= (count row) n-cols)]
             (into {} (map (fn [k v] (if (cols-to-keep k) [k v])) header row)))]
  (def weather
    (->> (for [row rows]
             [(row "DATE")
              (->>
                (-> row (dissoc "DATE") seq)
                (map (fn [[k v]] [(keyword (str "weather-" k)) ((:float  parsers) v)]))
                (into {})
                avg-wind-fix)])
         (into {}))))
;(->> weather seq (take 10))

(def col-mapping-raw
  {; Started from Green Taxi dataset
   "VendorID"                [:int             :vendor-id]
   "lpep_pickup_datetime"    [:basic-datetime  :pickup-dt]
   "Lpep_dropoff_datetime"   [:basic-datetime  :dropoff-dt]
   "Store_and_fwd_flag"      [:keyword         :store-flag]
   "RateCodeID"              [:keyword         :rate-code]
   "Pickup_longitude"        [:latlon          :pickup-lon]
   "Pickup_latitude"         [:latlon          :pickup-lat]
   "Dropoff_longitude"       [:latlon          :dropoff-lon]
   "Dropoff_latitude"        [:latlon          :dropoff-lat]
   "Passenger_count"         [:int             :n-passengers]
   "Trip_distance"           [:float           :travel-km]
   "Fare_amount"             [:float           :paid-fare]
   "MTA_tax"                 [:float           :paid-tax]
   "Tip_amount"              [:float           :paid-tip]
   "Tolls_amount"            [:float           :paid-tolls]
   "Ehail_fee"               [:float           :paid-ehail]
   "Total_amount"            [:float           :paid-total]
   "Payment_type"            [:keyword         :payment-type]
   "Trip_type"               [:keyword         :trip-type]
   
   ; These are from Yellow Taxi dataset
   "vendor_name"             [:keyword         :vendor-name]
   "vendor_id"               [:keyword         :vendor-name]
   
   "trip_pickup_datetime"    [:basic-datetime  :pickup-dt]
   "trip_dropoff_datetime"   [:basic-datetime  :dropoff-dt]
   
   "pickup_datetime"         [:basic-datetime  :pickup-dt]
   "dropoff_datetime"        [:basic-datetime  :dropoff-dt]
   
   "tpep_pickup_datetime"    [:basic-datetime  :pickup-dt]
   "tpep_dropoff_datetime"   [:basic-datetime  :dropoff-dt]
   
   "tip_amt"                 [:float           :paid-tip]
   "fare_amt"                [:float           :paid-fare]
   "tolls_amt"               [:float           :paid-tolls]
   "total_amt"               [:float           :paid-total]
   "rate_code"               [:keyword         :rate-code]
   "start_lon"               [:latlon          :pickup-lon]
   "start_lat"               [:latlon          :pickup-lat]
   "end_lon"                 [:latlon          :dropoff-lon]
   "end_lat"                 [:latlon          :dropoff-lat]
   "store_and_forward"       [:keyword         :store-flag]
   
   ; Ignored columns
   "surcharge"               nil
   "Extra"                   nil
   "improvement_surcharge"   nil
   "PULocationID"            nil
   "DOLocationID"            nil
   
   ; These are generated fields, not part of CSV. But also listed here to simplify ES mapping generation.
   :company      [:keyword  :company]
   :pickup-pos   [:geopoint :pickup-pos]
   :dropoff-pos  [:geopoint :dropoff-pos]
   :pickup-day   [:int      :pickup-day]
   :dlat-km      [:float    :dlat-km]
   :dlon-km      [:float    :dlon-km]
   :pickup-time  [:float    :pickup-time]
   :dropoff-time [:float    :dropoff-time]
   :travel-h     [:float    :travel-h]
   :speed-kmph   [:float    :speed-kmph]})

; Mapping CSV columns to lowercase to simplify schema definition
(def col-mapping (into {} (for [[k v] col-mapping-raw] [(if (string? k) (string/lower-case k) k) v])))

(def fields
  (->> (concat
         (for [weather-field (-> weather vals first keys)] [weather-field :float])
         (for [[field-type field-name & flag] (vals col-mapping) :when (and field-name (not ((set flag) :skip-in-es)))] [field-name field-type]))
      (into {})))

(let [doc-id-fields [:pickup-dt :dropoff-dt :pickup-lat :pickup-lon :dropoff-lat :dropoff-lon :travel-km :paid-total]]
  (defn get-doc-id [doc] (->> doc ((apply juxt doc-id-fields)) (interleave (repeat "/")) (apply str) u/sha1-hash)))

(defn parse-trips [file in]
  (let [company           (->> file str (re-find #"([a-z]+)[^/]+$") second)
        vendor-lookup     (if (= company "green") {1 "Creative Mobile Technologies" 2 "VeriFone"})
        
        csv-contents      (->> in io/reader csv/read-csv)
        header            (->> csv-contents first (map (comp string/lower-case string/trim)))
        missing           (clojure.set/difference (-> header set) (->> col-mapping keys (filter string?) set))
        _                 (if-not (empty? missing) (throw (Exception. (str "Missing cols for " missing " at " file))))
        
      ; I was going to drop these fields but now I think they'd be useful on Kibana heatmap as histogram aggregations
        drop-extra-fields  identity ; #(reduce dissoc % [:pickup-lat :pickup-lon :dropoff-lat :dropoff-lon])
        header            (map col-mapping header)
        n-cols            (count header)
        coalesce-to-zero #(or % 0.0)
        round            #(let [scale 1000.0] (-> % (* scale) Math/round (/ scale)))
        deg2km            (/ 40075.0 360.0)
        mile2km          #(-> % (* 1.60934) round)
        delta-km          (fn [from to] (if (and from to) (-> (- to from) (* deg2km) round)))
        dist              (fn [a b]     (if (and a b)     (Math/sqrt (+ (* a a) (* b b))) 0.0))
        
      ; Removing lat and lon deltas from trips with less than 0.2 km travel distance
        filter-km-deltas #(if (< (dist (:dlat-km %) (:dlon-km %)) 0.2) (reduce dissoc % [:dlat-km :dlon-km]) %)
        
        assoc-travel-h   #(assoc % :travel-h   (let [pickup-time  (:pickup-time  %)
                                                     dropoff-time (:dropoff-time %)]
                                                 (if (and dropoff-time pickup-time (not= dropoff-time pickup-time))
                                                   (round (let[delta (- dropoff-time pickup-time)]
                                                            (if (pos? delta) delta (+ delta 24)))))))
        assoc-speed-kmph #(assoc % :speed-kmph (if-let [travel-h (:travel-h %)] (if (> travel-h 0.17) (round (/ (:travel-km %) travel-h)))))
        to-time  (fn [datetime]
                   (let [time-scales [1.0 60.0 3600.0]
                         time (apply + (map / (map #(->> % (apply str) ((:int parsers))) (->> datetime str (drop 9) (partition 2))) time-scales))]
                     (if (not= time 0.0) (min (round time) 23.999))))
        docs     (for [row (rest csv-contents) :when (>= (count row) n-cols)]
                   (let [doc (into {} (map (fn [h v] (if h [(second h) ((-> h first parsers) v)])) header row))]
                     (-> doc
                       (assoc :index   (str "taxicab-" company "-" (-> doc :pickup-dt str (subs 0 4))))
                       (into           (weather (-> doc :pickup-dt str (subs 0 8)))))))
        assoc-time (fn [doc field]
                     (let [key   (-> field (str "-time") keyword)
                           value (-> field (str "-dt") keyword doc to-time)]
                        (assoc doc key value)))]
     (for [doc docs :when (pos? (:travel-km doc))]
     ;(for [doc docs :when (pos? (:travel-km doc))]
        (-> doc
            (assoc  :company        company)
            (update :vendor-name  #(get vendor-lookup (:vendor-id doc) %))
            (update :travel-km      mile2km)
            (assoc  :pickup-pos    (if (:pickup-lon  doc) (mapv doc [:pickup-lon  :pickup-lat])))
            (assoc  :dropoff-pos   (if (:dropoff-lon doc) (mapv doc [:dropoff-lon :dropoff-lat])))
            (assoc  :dlat-km       (delta-km (:pickup-lat doc) (:dropoff-lat doc)))
            (assoc  :dlon-km       (delta-km (:pickup-lon doc) (:dropoff-lon doc)))
            (update :paid-ehail     coalesce-to-zero)
            (update :paid-tax       coalesce-to-zero)
            (assoc  :pickup-day    (-> doc :pickup-dt day-of-week))
            (update :pickup-dt      datetime-to-str)
            (update :dropoff-dt     datetime-to-str)
            (assoc-time "pickup")
            (assoc-time "dropoff")
            assoc-travel-h
            assoc-speed-kmph
            filter-km-deltas
            drop-extra-fields))))


(comment
  (let [file (->> "yellow_tripdata_2010-02.csv.gz" (str data-folder) io/file)]
    (with-open [in (-> file clojure.java.io/input-stream java.util.zip.GZIPInputStream.)]
      (->> in io/reader line-seq (take 5)))))

(comment
  (let [file (->> "yellow_tripdata_2010-02.csv.gz" (str data-folder) io/file)]
    (with-open [in (-> file clojure.java.io/input-stream java.util.zip.GZIPInputStream.)]
      (->> (parse-trips file in) (take 3) (map #(into (sorted-map) %)) clojure.pprint/pprint))))

(comment
  (time (let [file (->> "yellow_tripdata_2009-09.csv.gz" (str data-folder) io/file)]
          (with-open [in (-> file clojure.java.io/input-stream java.util.zip.GZIPInputStream.)]
            (->> (parse-trips file in) count)))))
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(defn extract-to-csv [file]
  (let [file-out (string/replace file "/data/" "/data_out/")
        file-tmp (string/replace file-out #"$" ".tmp")
        n-docs   (atom 0)]
    (with-open [in  (-> file     io/file clojure.java.io/input-stream java.util.zip.GZIPInputStream.)
                out (-> file-tmp io/file clojure.java.io/output-stream java.util.zip.GZIPOutputStream. io/writer)]
      (let [_            (u/my-println-f "started")
            cols         (-> fields keys set (disj :pickup-pos :dropoff-pos) sort)
            docs         (parse-trips file in)
            dt-format   #(let [[y1 y2 m d h i s] (->> % (remove #{\T \Z}) (partition 2) (map (partial apply str)))] (str y1 y2 \- m \- d \space h \: i \: s))
            get-cols    #(map (fn [col] (or (col %) "")) cols)
            _            (csv/write-csv out
                           (concat
                             [(for [col cols] (-> col str (subs 1)))]
                             (for [doc docs]
                               (do (swap! n-docs inc)
                                   (-> doc
                                       (update :pickup-dt  dt-format)
                                       (update :dropoff-dt dt-format)
                                       get-cols))))
                           :newline :cr+lf)]))
    (.renameTo (java.io.File. file-tmp) (java.io.File. file-out))
    (u/my-println-f (str "finished, " @n-docs " rows"))
    {:generated @n-docs}))

;(extract-to-csv (str data-folder "green_tripdata_2013-08.csv.gz"))
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(def es-types
    {:float            {:type "float"}
     :latlon           {:type "float"}
     :double           {:type "double"}
     :int              {:type "integer"}
     :keyword          {:type "keyword"}
     :basic-date       {:type "date" :format "basic_date"}
     :basic-datetime   {:type "date" :format "basic_date_time_no_millis||year_month_day||epoch_millis"}
     :geopoint         {:type "geo_point"}})

(def taxi-mapping
  {:mappings {:ride  {:_all    {:enabled false}
                    ; :_source {:enabled false}
                      :properties (zipmap (keys fields) (map es-types (vals fields)))}}
   :settings {"index.translog.flush_threshold_size" "20g"
              "index.store.throttle.max_bytes_per_sec" "1000mb"
              :number_of_shards 10
              :number_of_replicas 0
              :refresh_interval "30s"}})
;(pprint taxi-mapping)

(def es-config
  {:server (u/getenv "ES_SERVER" "192.168.0.100:9200")})

(defn make-client [] (s/client {:hosts [(str "http://" (:server es-config))]}))

; curl -s 'http://localhost:9200/_template/*?pretty'
; curl -XDELETE 'http://localhost:9200/_template/taxicab' && curl -XDELETE 'http://localhost:9200/taxicab-*'
(defn create-template [client template-name body]
  (let [url (str "_template/" template-name)]
    (s/request client {:url url :method :put :body body})))

(defn create-taxicab-template []
  (create-template (make-client) "taxicab" (assoc taxi-mapping :template "taxicab-*")))
;(create-taxicab-template)
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(defn insert-file [file]
  (with-open [in (-> file io/file clojure.java.io/input-stream java.util.zip.GZIPInputStream.)]
    (let [client        (s/client {:hosts [(str "http://" (:server es-config))]})
          doc->bulk     (fn [doc] [{:index {:_index (:index doc) :_type :ride}} (dissoc doc :index)])
          insert-chunk  (fn [chunk-body]
                          (let [retry-t 120000]
                              (try
                                (->>
                                  {:url "_bulk" :method :put :headers {"content-type" "text/plain"} :body chunk-body}
                                  (s/request client) :body :items (map (comp :status :index)))
                                (catch java.io.IOException e
                                  (do 
                                    (u/my-println-f "Exception " (.getMessage e) ", retrying after " retry-t  " ms...")
                                    (Thread/sleep retry-t))))))
          process-chunk (fn [chunk-in]
                          (let [chunk-body (->> chunk-in (map doc->bulk) flatten s/chunks->body)]
                          ; Call insert-chunk until it returns non-nil. I was going to recur
                          ; but it could not be done from "catch" position so this is what I came up with
                            (->> #(insert-chunk chunk-body) repeatedly (filter some?) first)))
          _       (u/my-println-f "started")
          freqs   (->> (parse-trips file in) (partition-all 4000) (map process-chunk) flatten frequencies)
          _       (u/my-println-f "got " freqs)]
        freqs)))

(def main-funs
  {"insert"  insert-file
   "extract" extract-to-csv})

; time ls ../data/*.gz | grep -E '(yellow|green)' | shuf | xargs java -Xms16g -Xmx30g -jar target/taxi-rides-clj-0.0.1-SNAPSHOT-standalone.jar insert  4
; time ls ../data/*.gz | grep -E '(yellow|green)' | shuf | xargs java -Xms16g -Xmx30g -jar target/taxi-rides-clj-0.0.1-SNAPSHOT-standalone.jar extract 8
(defn -main [f-name n-parallel & files]
  (do
    (u/my-println "Started!")
    (create-taxicab-template)
    (do (->> (string/replace file #".+/" "")
             (str data-folder)
             ((main-funs f-name))
             (cp/upfor (Integer. ^String n-parallel) [file files])
             doall
             (apply merge-with +)))
    (shutdown-agents)
    (System/exit 0)))
