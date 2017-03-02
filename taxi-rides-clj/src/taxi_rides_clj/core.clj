(ns taxi-rides-clj.core
  (:require [clojure.data.json :as json]
            [clojure.data.csv :as csv]
            [clojure.string :as string]
            [clojure.java.io :as io]
            [qbits.spandex :as s]
            [clojure.core.async :as async]
            [java-time :as t])
  (:use clojure.set)
  (:import java.util.zip.GZIPInputStream)
  (:gen-class))

; https://gist.github.com/prasincs/827272
(defn get-hash [type data] (.digest (java.security.MessageDigest/getInstance type) (.getBytes data)))
(defn sha1-hash [data] (->> data (get-hash "sha1") (map #(.substring (Integer/toString (+ (bit-and % 0xff) 0x100) 16) 1)) (apply str)))

; http://yellerapp.com/posts/2014-12-11-14-race-condition-in-clojure-println.html
(defn my-println [& more]
  (do
    (.write *out* (str (clojure.string/join "" more) "\n"))
    (flush)))

; Based on standard pmap but with configurable n
(defn my-pmap [n f coll]
  (let [rets (map #(-> % f (try (catch Exception e (my-println "Exception: " e))) future) coll)
        step (fn step [[x & xs :as vs] fs]
               (lazy-seq
                 (if-let [s (seq fs)]
                   (cons (deref x) (step xs (rest s)))
                   (map deref vs))))]
     (step rets (drop n rets))))
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(def data-folder "/home/wrecked/projects/taxi-rides/data")
(defn find-files [re] (->> data-folder io/file file-seq (filter #(re-find re (-> % .getName))) sort))

(defmacro make-parser [f]
  `(fn [v#] (if-not (empty? v#)
              (try (~f v#)
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

(let [[header & rows] (-> (str data-folder "/central_park_weather.csv") slurp csv/read-csv)
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

(def col-mapping
  {"VendorID"                [:int             :vendor-id]
   "lpep_pickup_datetime"    [:basic-datetime  :pickup-dt]
   "Lpep_dropoff_datetime"   [:basic-datetime  :dropoff-dt]
   "Store_and_fwd_flag"      [:keyword         :store-flag]
   "RateCodeID"              [:int             :rate-code-id]
   "Pickup_longitude"        [:latlon          :pickup-lon      :skip-in-es]
   "Pickup_latitude"         [:latlon          :pickup-lat      :skip-in-es]
   "Dropoff_longitude"       [:latlon          :dropoff-lon     :skip-in-es]
   "Dropoff_latitude"        [:latlon          :dropoff-lat     :skip-in-es]
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
   
   ; These are formed by mergin latitude an longitude columns, not part of CSV
   :pickup-pos   [:geopoint :pickup-pos]
   :dropoff-pos  [:geopoint :dropoff-pos]
   :dlat-km      [:float    :dlat-km]
   :dlon-km      [:float    :dlon-km]
   :pickup-time  [:float    :pickup-time]
   :dropoff-time [:float    :dropoff-time]
   :travel-h     [:float    :travel-h]
   :speed-kmph   [:float    :speed-kmph]})

(def fields
  (->> (concat
         (for [weather-field (-> weather vals first keys)] [weather-field :float])
         (for [[field-type field-name & flag] (vals col-mapping) :when (and field-name (not ((set flag) :skip-in-es)))] [field-name field-type]))
      (into {})))

(defn parse-trips [file]
  (with-open [in (-> file clojure.java.io/input-stream java.util.zip.GZIPInputStream.)]
    (let [doc-id-fields [:pickup-dt :dropoff-dt :pickup-lat :pickup-lon :dropoff-lat :dropoff-lon :travel-km :paid-total]
          dataset (->> file str (re-find #"([a-z]+)[^/]+$") second)
          [header & rows] (-> in slurp csv/read-csv)
          header   (map string/trim header)
          missing  (clojure.set/difference (->> col-mapping keys (filter string?) set) (-> header set))
          check    (if-not (empty? missing) (throw (Exception. (str "Missing cols for " missing " at " file))))
          drop-fields identity ; #(reduce dissoc % [:pickup-lat :pickup-lon :dropoff-lat :dropoff-lon])
          header   (map col-mapping header)
          n-cols   (count header)
          round    (fn [n] (let [scale 1000.0] (-> n (* scale) Math/round (/ scale))))
          to-int-str (fn [v] (if v (let [v (int v)] (str (if (< v 10) "0" "") v))))
          deg2km   (/ 40075.0 360.0)
          mile2km  (fn [mile] (-> mile (* 1.60934) round))
          delta-km (fn [from to] (if (and from to) (-> (- to from) (* deg2km) round)))
          dist     (fn [a b] (if (and a b) (Math/sqrt (+ (* a a) (* b b))) 0.0))
          to-time  (fn [datetime]
                     (let [time (apply + (map / (map #(->> % (apply str) ((:int parsers))) (->> datetime (drop 9) (partition 2))) [1.0 60.0 3600.0]))]
                       (if (not= time 0.0) (round time))))
          docs     (for [row rows :when (>= (count row) n-cols)]
                     (let [doc (into (sorted-map) (map (fn [h v] (if h [(second h) ((-> h first parsers) v)])) header row))]
                       (-> doc
                         (assoc :id      (->> doc-id-fields (map doc) (interleave (repeat "/")) (apply str) sha1-hash))
                         (assoc :index   (apply str "taxicab-" dataset "-" (take 4 (:pickup-dt doc))))
                         (into           (weather (-> doc :pickup-dt (subs 0 8)))))))]
        (for [doc docs :when (pos? (:travel-km doc))]
          (-> doc
              (update :travel-km   mile2km)
              (assoc :pickup-pos   (if (:pickup-lon  doc) (mapv doc [:pickup-lon  :pickup-lat])))
              (assoc :dropoff-pos  (if (:dropoff-lon doc) (mapv doc [:dropoff-lon :dropoff-lat])))
              (assoc :dlat-km      (delta-km (:pickup-lat doc) (:dropoff-lat doc)))
              (assoc :dlon-km      (delta-km (:pickup-lon doc) (:dropoff-lon doc)))
              (assoc :pickup-time  (to-time (:pickup-dt  doc)))
              (assoc :dropoff-time (to-time (:dropoff-dt doc)))
              (#(assoc % :travel-h   (let [pickup-time  (:pickup-time  %)
                                           dropoff-time (:dropoff-time %)]
                                       (if (and dropoff-time pickup-time (not= dropoff-time pickup-time))
                                         (round (let[delta (- dropoff-time pickup-time)]
                                                  (if (pos? delta) delta (+ delta 24))))))))
             ;(update :pickup-time  to-int-str)
             ;(update :dropoff-time to-int-str)
              (#(assoc % :speed-kmph (if-let [travel-h (:travel-h %)] (round (/ (:travel-km %) travel-h)))))
              (#(if (< (dist (:dlat-km %) (:dlon-km %)) 0.2) (reduce dissoc % [:dlat-km :dlon-km]) %))
              drop-fields)))))
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
  {:mappings {:ride  {:properties (->> (map vector (keys fields) (map es-types (vals fields))) (into {}))}}
   :settings {:number_of_shards 1 :number_of_replicas 0}})
;(pprint taxi-mapping)

(def es-config
  {:server      (or (System/getenv "ES_SERVER")      "192.168.0.100:9200")})

; curl -s 'http://localhost:9200/_template/*?pretty'
; curl -XDELETE 'http://localhost:9200/_template/taxicab'
; curl -XDELETE 'http://localhost:9200/taxicab-green-*'
(defn create-template [client template-name body]
  (let [url (str "_template/" template-name)]
    (if (= 404 (:status (s/request client {:url url :method :head})))
      (s/request client {:url url :method :put :body body}))))

(defn create-taxicab-template []
  (let [client (s/client {:hosts [(str "http://" (:server es-config))]})]
    (create-template client "taxicab" (assoc taxi-mapping :template "taxicab-*"))))
;(create-taxicab-template)
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(defmacro my-println-t [& forms]
  `(my-println (t/local-time) ":" ~'file " " ~@forms))

(defn insert-file [file]
  (let [docs   (parse-trips file)
        client (s/client {:hosts [(str "http://" (:server es-config))]})
        doc->bulk (fn [doc] [{:index {:_index (:index doc) :_type :ride :_id (:id doc)}}
                             (reduce dissoc doc [:index :id])])
        exists? (fn [doc] (= 200 (:status (s/request client {:url (str (:index doc) "/ride/" (:id doc)) :method :head}))))]
    (let [p     (my-println-t "started")
          freqs (->> (for [chunk (map vec (partition-all 500 docs))]
                       (if (every? exists? [(first chunk) (->> chunk count dec (nth chunk))])
                         (repeat (count chunk) 200)
                         (let [chunk-body (->> chunk (map doc->bulk) flatten s/chunks->body)
                               response   (s/request client {:url "_bulk" :method :put :headers {"content-type" "text/plain"} :body chunk-body})]
                           (->> response :body :items (map (comp :status :index))))))
                     flatten
                     frequencies)
          p     (my-println-t "got " freqs)]
      freqs)))

(defn store-green [n]
  (let [p      (my-println (t/local-time) " Started...")
        result (doall (->> #"green_tripdata_201.+\.csv\.gz$" find-files (my-pmap n insert-file)))
        p      (my-println (t/local-time) " ...finished!")]
    result))
;(def results (store-green 6))

;(->> #"green_.+\.csv\.gz$" find-files (map insert-file))

;(->> #"green_tripdata_201[^34].+\.csv\.gz$" find-files (map str))
;(->> #"green_.+\.csv\.gz$" find-files first parse-trips (take 3) pprint)
;(->> #"green_.+\.csv\.gz$" find-files (take 1) (map insert-file))
;(->> #"green_.+\.csv\.gz$" find-files (my-pmap 1 insert-file))
;(->> #"green_.+\.csv\.gz$" find-files (my-pmap 7 insert-file)))
;(->> #"green_.+\.csv\.gz$" find-files count)


(defn -main [arg]
  (do
    (store-green ((:int parsers) arg))
    (shutdown-agents)))
