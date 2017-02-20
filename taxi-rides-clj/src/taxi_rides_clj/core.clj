(ns taxi-rides-clj.core
  (:require [clojure.data.json :as json]
            [clojure.data.csv :as csv]
            [clojure.string :as string]
            [clojure.java.io :as io])
  (:use clojure.set)
  (:import java.util.zip.GZIPInputStream)
  (:gen-class))

; https://gist.github.com/prasincs/827272
(defn get-hash [type data] (.digest (java.security.MessageDigest/getInstance type) (.getBytes data)))
(defn sha1-hash [data] (->> data (get-hash "sha1") (map #(.substring (Integer/toString (+ (bit-and % 0xff) 0x100) 16) 1)) (apply str)))

(def data-folder "/home/wrecked/projects/taxi-rides/data")
(defn find-files [re] (->> data-folder io/file file-seq (filter #(re-find re (-> % .getName))) sort))
(def files (find-files #"green_.+\.csv\.gz$"))

(defmacro make-parser [f] `(fn [v#] (if-not (empty? v#) (~f v#))))

(let [double-parser   (make-parser Double.)
      date-format     (java.text.SimpleDateFormat. "yyyy-MM-dd hh:mm:ss")
      ymd-format      (java.text.SimpleDateFormat. "yyyy-MM-dd")
      hms-format      (java.text.SimpleDateFormat. "hh:mm:ss")]
  (def parsers
    {:int       (make-parser Integer.)
     :double    double-parser
     :latlon   #(if-not (= % "0") (double-parser %))
     :keyword  #(let [s (string/trim %)] (if-not (empty? s) s))
     :date     #(let [d (.parse date-format %)] (str (.format ymd-format d) "T" (.format hms-format d) "Z"))}))

(def col-mapping
  {"VendorID"                [:int     :vendor-id]
   "lpep_pickup_datetime"    [:date    :pickup-dt]
   "Lpep_dropoff_datetime"   [:date    :dropff-dt]
   "Store_and_fwd_flag"      [:keyword :store-flag]
   "RateCodeID"              [:int     :rate-code-id]
   "Pickup_longitude"        [:latlon  :pickup-lon]
   "Pickup_latitude"         [:latlon  :pickup-lat]
   "Dropoff_longitude"       [:latlon  :dropoff-lon]
   "Dropoff_latitude"        [:latlon  :dropoff-lat]
   "Passenger_count"         [:int     :n-passengers]
   "Trip_distance"           [:double  :distance]
   "Fare_amount"             [:double  :paid-fare]
   "MTA_tax"                 [:double  :paid-tax]
   "Tip_amount"              [:double  :paid-tip]
   "Tolls_amount"            [:double  :paid-tolls]
   "Ehail_fee"               [:double  :paid-ehail]
   "Total_amount"            [:double  :paid-total]
   "Payment_type"            [:keyword :payment-type]
   "Trip_type"               [:keyword :trip-type]
   "Extra"                   nil})

(let [[header & rows] (-> (str data-folder "/central_park_weather.csv") slurp csv/read-csv)
      cols-to-keep #{"DATE" "PRCP" "SNWD" "SNOW" "TMAX" "TMIN" "AWND"}
      n-cols (count header)
      rows (for [row rows :when (>= (count row) n-cols)]
             (into {} (map (fn [k v] (if (cols-to-keep k) [k v])) header row)))]
  (def weather
    (->> rows
         (map (fn [row] (let [[y1 y2 m d] (partition 2 (row "DATE"))
                              date (apply str (concat y1 y2 "-" m "-" d))]
                          [date
                           (->>
                             (-> row (dissoc "DATE") seq)
                             (map (fn [[k v]] [(keyword (str "weather-" k)) ((:double parsers) v)]))
                             (into {}))])))
         (into {}))))
;(->> weather seq (take 10))

(defn parse-trips [file]
  (with-open [in (-> file clojure.java.io/input-stream java.util.zip.GZIPInputStream.)]
    (let [doc-id-fields [:pickup-dt :dropff-dt :pickup-lat :pickup-lon :dropoff-lat :dropoff-lon :distance  :paid-total]
          dataset (->> file str (re-find #"([a-z]+)[^/]+$") second)
          [header & rows] (-> in slurp csv/read-csv)
          header   (map string/trim header)
          missing  (clojure.set/difference (-> header set) (-> col-mapping keys set))
          check    (assert (empty? missing) (str "Missing cols for " missing))
          header   (map col-mapping header)
          n-cols   (count header)
          docs     (for [row rows :when (>= (count row) n-cols)]
                     (let [doc (into {} (map (fn [h v] (if h [(second h) ((-> h first parsers) v)])) header row))]
                       (-> doc
                         (assoc :id      (->> doc-id-fields (map doc) (interleave (repeat "/")) (apply str) sha1-hash))
                         (assoc :index   (apply str "nyc-taxi-" dataset "-" (take 7 (:pickup-dt doc))))
                         (into           (weather (-> doc :pickup-dt (subs 0 10)))))))]
        (filter #(pos? (:distance %)) docs))))

(def rows (->> files first parse-trips (group-by :index)))
(pprint (take 3 (rows (-> rows keys first))))

(defn -main []
  (println "Hello, World!"))
