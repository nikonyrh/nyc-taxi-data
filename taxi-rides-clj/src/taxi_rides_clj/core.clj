(ns taxi-rides-clj.core
  (:require [clojure.data.json :as json]
            [clojure.data.csv :as csv]
            [clojure.string :as string]
            [clojure.java.io :as io]
            [qbits.spandex :as s]
            [clojure.core.async :as async]
            [java-time :as t]
            [com.climate.claypoole :as cp])
  (:use clojure.set)
  (:import java.util.zip.GZIPInputStream)
  (:gen-class))

(set! *warn-on-reflection* true)

; https://gist.github.com/prasincs/827272
(defn get-hash [type data] (.digest (java.security.MessageDigest/getInstance type) (.getBytes ^String data)))
(defn sha1-hash [data] (->> data (get-hash "sha1") (map #(.substring (Integer/toString (+ (bit-and % 0xff) 0x100) 16) 1)) (apply str)))

; http://yellerapp.com/posts/2014-12-11-14-race-condition-in-clojure-println.html
(defn my-println [& more]
  (do
    (.write *out* (str (clojure.string/join "" more) "\n"))
    (flush)))

(defmacro my-println-t [& forms]
  `(my-println (t/local-time) ":" ~'file " " ~@forms))

(defn getenv
  ([key] (getenv key nil))
  ([key default] (let [value (System/getenv key)] (if (-> value count pos?) value default))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(def data-folder (str (getenv "TAXI_DATA_FOLDER" "/home/wrecked/projects/taxi-rides/data") "/"))
(defn find-files [re] (->> data-folder io/file file-seq (filter #(re-find re (.getName ^java.io.File %))) sort))

(defmacro make-parser [f]
  `(fn [v#] (if-not (empty? v#)
              (try (~f ^String v#)
                   (catch Exception e#
                     (do (my-println "Invalid value '" v# "' for parser " (quote ~f))
                         (my-println "Exception: " e#)))))))

(let [int-parser      (make-parser Integer.)
      double-parser   (make-parser Double.)
      digits          (into #{\T} (for [i (range 10)] (first (str i))))
      assert-len     #(if (= (count %2) %) %2 (throw (Exception. (str "Expected length " % ", got '" %2 "'!"))))
      basic-parser   #(let [d (->> % (re-seq #"[0-9]+") (map int-parser) (apply t/local-date-time)
                                     str (filter digits) (apply str))
                            d (if (and (= (count d) 13) (= (nth d 8) \T))
                                (str d "00")
                                d)]
                        (str (assert-len 15 d) "Z"))]
  (def parsers
    {:int              int-parser
     :float            double-parser
     :latlon          #(if-not (= % "0") (double-parser %))
     :keyword         #(let [s (string/trim %)] (if-not (empty? s) s))
     :basic-datetime   (make-parser basic-parser)}))

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
   "Extra"                   nil
   "improvement_surcharge"   nil
   
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
   "surcharge"               nil
   
   ; These are generated fields, not part of CSV. But also listed here to simplify ES mapping generation.
   :company      [:keyword  :company]
   :pickup-pos   [:geopoint :pickup-pos]
   :dropoff-pos  [:geopoint :dropoff-pos]
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

; Used for filtering out duplicate documents, so instead of storing
; them to ES with an id we'll bear the CPU cost here. Documentation
; says the function of filter must not have side effects but I guess
; this is a valid exception.
(defn my-distinct [id-getter coll]
  (let [seen-ids (volatile! #{})
        seen?    (fn [id] (if-not (contains? @seen-ids id) (vswap! seen-ids conj id)))]
    (filter (comp seen? id-getter) coll)))

(defn parse-trips [file in]
  (let [doc-id-fields     [:pickup-dt :dropoff-dt :pickup-lat :pickup-lon :dropoff-lat :dropoff-lon :travel-km :paid-total]
        get-doc-id        (fn [doc] (->> doc-id-fields (map doc) (interleave (repeat "/")) (apply str) sha1-hash))
        
        company           (->> file str (re-find #"([a-z]+)[^/]+$") second)
        vendor-lookup     (if (= company "green") {1 "Creative Mobile Technologies" 2 "VeriFone"})
        
      ; This is simpler but reads the whole CSV into memory at once, which caused OOMs on larger files
      ; csv-contents      (-> in slurp csv/read-csv)
        
      ; This version feeds lines to read-csv one by one, making the file parsing lazy
        csv-contents      (->> in io/reader line-seq (map (comp first csv/read-csv)))
        
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
        
        assoc-speed-kmph #(assoc % :speed-kmph (if-let [travel-h (:travel-h %)] (if (> travel-h 0.17) (round (/ (:travel-km %) travel-h)))))
        assoc-travel-h   #(assoc % :travel-h   (let [pickup-time  (:pickup-time  %)
                                                     dropoff-time (:dropoff-time %)]
                                                 (if (and dropoff-time pickup-time (not= dropoff-time pickup-time))
                                                   (round (let[delta (- dropoff-time pickup-time)]
                                                            (if (pos? delta) delta (+ delta 24)))))))
        to-time  (fn [datetime]
                   (let [time-scales [1.0 60.0 3600.0]
                         time (apply + (map / (map #(->> % (apply str) ((:int parsers))) (->> datetime (drop 9) (partition 2))) time-scales))]
                     (if (not= time 0.0) (min (round time) 23.999))))
        docs     (for [row (rest csv-contents) :when (>= (count row) n-cols)]
                   (let [doc (into (sorted-map) (map (fn [h v] (if h [(second h) ((-> h first parsers) v)])) header row))]
                     (-> doc
                       (assoc :index   (apply str "taxicab-" company "-" (take 4 (:pickup-dt doc))))
                       (into           (weather (-> doc :pickup-dt (subs 0 8)))))))]
      (for [doc (my-distinct get-doc-id docs) :when (pos? (:travel-km doc))]
        (-> doc
            (assoc  :company        company)
            (update :vendor-name  #(get vendor-lookup (:vendor-id doc) %))
            (update :travel-km      mile2km)
            (assoc  :pickup-pos    (if (:pickup-lon  doc) (mapv doc [:pickup-lon  :pickup-lat])))
            (assoc  :dropoff-pos   (if (:dropoff-lon doc) (mapv doc [:dropoff-lon :dropoff-lat])))
            (assoc  :dlat-km       (delta-km (:pickup-lat doc) (:dropoff-lat doc)))
            (assoc  :dlon-km       (delta-km (:pickup-lon doc) (:dropoff-lon doc)))
            (assoc  :pickup-time   (to-time (:pickup-dt  doc)))
            (assoc  :dropoff-time  (to-time (:dropoff-dt doc)))
            (update :paid-ehail     coalesce-to-zero)
            (update :paid-tax       coalesce-to-zero)
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
      (->> (parse-trips file in) (take 3) clojure.pprint/pprint))))
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(defn extract-to-csv [file]
  (with-open [in (-> file io/file clojure.java.io/input-stream java.util.zip.GZIPInputStream.)]
    (with-open [out (-> file (string/replace #"/data/" "/data_out/") io/file clojure.java.io/output-stream java.util.zip.GZIPOutputStream. io/writer)]
      (let [_            (my-println-t "started")
            cols         (-> fields keys set (disj :pickup-pos :dropoff-pos) sort)
            docs         (parse-trips file in)
            dt-format   #(let [[y1 y2 m d h i s] (->> % (remove #{\T \Z}) (partition 2) (map (partial apply str)))] (str y1 y2 \- m \- d \space h \: i \: s))
            get-cols    #(map (fn [col] (or (col %) "NULL")) cols)
            n-docs       (atom 0)
            _            (csv/write-csv out
                           (concat
                             [(for [col cols] (-> col str (subs 1)))]
                             (for [doc docs]
                               (do (swap! n-docs inc)
                                   (-> doc
                                       (update :pickup-dt  dt-format)
                                       (update :dropoff-dt dt-format)
                                       get-cols)))))]
        (my-println-t (str "finished, " @n-docs " rows"))))))

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
                     ;:_source {:enabled false}
                      :properties (->> (map vector (keys fields) (map es-types (vals fields)))
                                       (into {}))}}
   :settings {"index.translog.flush_threshold_size" "1g"
              "index.store.throttle.max_bytes_per_sec" "1000mb"
              :number_of_shards 1
              :number_of_replicas 0
              :refresh_interval "10s"}})
;(pprint taxi-mapping)

(def es-config
  {:server (getenv "ES_SERVER" "192.168.0.100:9200")})

; curl -s 'http://localhost:9200/_template/*?pretty'
; curl -XDELETE 'http://localhost:9200/_template/taxicab' && curl -XDELETE 'http://localhost:9200/taxicab-*'
(defn create-template [client template-name body]
  (let [url (str "_template/" template-name)]
    (s/request client {:url url :method :put :body body})))

(defn create-taxicab-template []
  (let [client (s/client {:hosts [(str "http://" (:server es-config))]})]
    (create-template client "taxicab" (assoc taxi-mapping :template "taxicab-*"))))
;(create-taxicab-template)
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(defn insert-file [file]
  (with-open [in (-> file io/file clojure.java.io/input-stream java.util.zip.GZIPInputStream.)]
    (let [client    (s/client {:hosts [(str "http://" (:server es-config))]})
          doc->bulk (fn [doc] [{:index {:_index (:index doc) :_type :ride}} (dissoc doc :index)])
          ; TODO: Try-catch-retry logic for java.io.IOException if ES timeouts under heavy load
          process-chunk (fn [chunk]
                          (let [chunk-body (->> chunk (map doc->bulk) flatten s/chunks->body)
                                response   (s/request client {:url "_bulk" :method :put :headers {"content-type" "text/plain"} :body chunk-body})]
                            (->> response :body :items (map (comp :status :index)))))
          _       (my-println-t "started")
          freqs   (->> (parse-trips file in) (partition-all 1000) (map vec) (map process-chunk) flatten frequencies)
          _       (my-println-t "got " freqs)]
        freqs)))

(def main-funs
  {"insert"  insert-file
   "extract" extract-to-csv})

; time ls ../data/yellow_*.gz | shuf | xargs java -jar target/taxi-rides-clj-0.0.1-SNAPSHOT-standalone.jar insert 4
; time ls ../data/*.gz |grep -E '(yellow|green)' | shuf | xargs java -jar target/taxi-rides-clj-0.0.1-SNAPSHOT-standalone.jar extract 8
(defn -main [f-name n-parallel & files]
  (do
    (create-taxicab-template)
    (doall (cp/upfor (Integer. ^String n-parallel) [file files] (->> (string/replace file #".+/" "") (str data-folder) ((main-funs f-name)))))
    (shutdown-agents)
    (System/exit 0)))
