(ns taxi-rides-clj.core
  (:require [clojure.data.json :as json]
            [clojure.data.csv :as csv]
            [clojure.string :as string]
            [clojure.java.io :as io]
            [qbits.spandex :as s]
            [clojure.core.async :as async]
            [clj-time.core :as t])
  (:use clojure.set)
  (:import java.util.zip.GZIPInputStream)
  (:gen-class))

; https://gist.github.com/prasincs/827272
(defn get-hash [type data] (.digest (java.security.MessageDigest/getInstance type) (.getBytes data)))
(defn sha1-hash [data] (->> data (get-hash "sha1") (map #(.substring (Integer/toString (+ (bit-and % 0xff) 0x100) 16) 1)) (apply str)))

; Based on standard pmap but with configurable n
(defn my-pmap [n f coll]
  (let [rets (map #(future (f %)) coll)
        step (fn step [[x & xs :as vs] fs]
               (lazy-seq
                 (if-let [s (seq fs)]
                   (cons (deref x) (step xs (rest s)))
                   (map deref vs))))]
     (step rets (drop n rets))))

; http://yellerapp.com/posts/2014-12-11-14-race-condition-in-clojure-println.html
(defn my-println [& more]
  (.write *out* (str (clojure.string/join " " more) "\n")))
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(def data-folder "/home/wrecked/projects/taxi-rides/data")
(defn find-files [re] (->> data-folder io/file file-seq (filter #(re-find re (-> % .getName))) sort))

(defmacro make-parser [f]
  `(fn [v#] (if-not (empty? v#)
              (try (~f v#)
                   (catch Exception e# (my-println (str "Invalid value '" v# "' for parser " (quote ~f))))))))

(let [double-parser   (make-parser Double.)
      date-format     (java.text.SimpleDateFormat. "yyyy-MM-dd hh:mm:ss")
      ymd-format      (java.text.SimpleDateFormat. "yyyyMMdd")
      hms-format      (java.text.SimpleDateFormat. "hhmmss")
      basic-parser   #(let [d (.parse date-format %)] (str (.format ymd-format d) "T" (.format hms-format d) "Z"))]
  (def parsers
    {:int              (make-parser Integer.)
     :float            double-parser
     :latlon          #(if-not (= % "0") (double-parser %))
     :keyword         #(let [s (string/trim %)] (if-not (empty? s) s))
     :basic-datetime   (make-parser basic-parser)}))
((:basic-datetime parsers) "2013-11-01 00:06:39")

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
   "Lpep_dropoff_datetime"   [:basic-datetime  :dropff-dt]
   "Store_and_fwd_flag"      [:keyword         :store-flag]
   "RateCodeID"              [:int             :rate-code-id]
   "Pickup_longitude"        [:latlon          :pickup-lon      :skip-in-es]
   "Pickup_latitude"         [:latlon          :pickup-lat      :skip-in-es]
   "Dropoff_longitude"       [:latlon          :dropoff-lon     :skip-in-es]
   "Dropoff_latitude"        [:latlon          :dropoff-lat     :skip-in-es]
   "Passenger_count"         [:int             :n-passengers]
   "Trip_distance"           [:float           :distance]
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
   :dropoff-pos  [:geopoint :dropoff-pos]})

(def fields
  (->> (concat
         (for [weather-field (-> weather vals first keys)] [weather-field :float])
         (for [[field-type field-name & flag] (vals col-mapping) :when (and field-name (not ((set flag) :skip-in-es)))] [field-name field-type]))
      (into {})))

(defn parse-trips [file]
  (with-open [in (-> file clojure.java.io/input-stream java.util.zip.GZIPInputStream.)]
    (let [doc-id-fields [:pickup-dt :dropff-dt :pickup-lat :pickup-lon :dropoff-lat :dropoff-lon :distance :paid-total]
          dataset (->> file str (re-find #"([a-z]+)[^/]+$") second)
          [header & rows] (-> in slurp csv/read-csv)
          header   (map string/trim header)
          missing  (clojure.set/difference (-> header set) (-> col-mapping keys set))
          check    (assert (empty? missing) (str "Missing cols for " missing))
          header   (map col-mapping header)
          n-cols   (count header)
          docs     (for [row rows :when (>= (count row) n-cols)]
                     (let [doc (into (sorted-map) (map (fn [h v] (if h [(second h) ((-> h first parsers) v)])) header row))]
                       (-> doc
                         (assoc :id      (->> doc-id-fields (map doc) (interleave (repeat "/")) (apply str) sha1-hash))
                         (assoc :index   (apply str "taxicab-" dataset "-" (take 4 (:pickup-dt doc))))
                         (into           (weather (-> doc :pickup-dt (subs 0 8)))))))]
        (for [doc docs :when (pos? (:distance doc))]
          (-> doc
              (assoc :pickup-pos  (if (:pickup-lon  doc) (mapv doc [:pickup-lon  :pickup-lat])))
              (assoc :dropoff-pos (if (:dropoff-lon doc) (mapv doc [:dropoff-lon :dropoff-lat])))
              (#(reduce dissoc % [:pickup-lat :pickup-lon :dropoff-lat :dropoff-lon])))))))
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(def es-types
    {:float            {:type "float"}
     :double           {:type "double"}
     :int              {:type "integer"}
     :keyword          {:type "keyword"}
     :basic-date       {:type "date" :format "basic_date"}
     :basic-datetime   {:type "date" :format "basic_date_time_no_millis"}
     :geopoint         {:type "geo_point"}})

(def taxi-mapping
  {:mappings {:ride  {:properties (->> (map vector (keys fields) (map es-types (vals fields))) (into {}))}}
   :settings {:number_of_shards 1 :number_of_replicas 0}})
;(pprint taxi-mapping)

(def es-config
  {:server      (or (System/getenv "ES_SERVER")      "192.168.0.100:9200")})

; curl -s 'http://localhost:9200/_template/*?pretty'
(defn create-template [client template-name body]
  (let [url (str "_template/" template-name)]
    (if (= 404 (:status (s/request client {:url url :method :head})))
      (s/request client {:url url :method :put :body body}))))

(defn create-taxicab-template []
  (let [client (s/client {:hosts [(str "http://" (:server es-config))]})]
    (create-template client "taxicab" (assoc taxi-mapping :template "taxicab-*"))))
;(create-taxicab-template)
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

; Got issue with channel having more than 1024 items at once, don't know how to fix :(
(comment
  (defn insert-file [file]
    (let [docs (parse-trips file)
          client (s/client {:hosts [(str "http://" (:server es-config))]})
          bulk-cfg {:flush-threshold 100 :flush-interval 1000 :max-concurrent-requests 6}
          {:keys [input-ch output-ch]} (s/bulk-chan client bulk-cfg)]
        (doall (for [doc docs] (async/put! input-ch [{:index {:_index (:index doc) :_type :ride :_id (:id doc)}}
                                                     (reduce dissoc doc [:index :id])])))
        (future (loop [] (async/<!! output-ch))))))

(defn insert-file [file]
  (let [p      (my-println (str "Started file " file "\n"))
        docs   (vec (parse-trips file))
        client (s/client {:hosts [(str "http://" (:server es-config))]})
        doc->bulk (fn [doc] [{:index {:_index (:index doc) :_type :ride :_id (:id doc)}}
                             (reduce dissoc doc [:index :id])])
        exists? (fn [doc] (= 200 (:status (s/request client {:url (str (:index doc) "/ride/" (:id doc)) :method :head}))))]
    (if (and (exists? (first docs)) (exists? (last docs)))
      (my-println (str "Skipped " file "!\n"))
      (->>
        (for [chunk (partition-all 500 docs)]
          (let [chunk-body (->> chunk (map doc->bulk) flatten s/chunks->body)
                response   (s/request client {:url "_bulk" :method :put :headers {"content-type" "text/plain"} :body chunk-body})]
            (->> response :body :items (map (comp :status :index)))))
        flatten
        frequencies))))

;(->> #"green_.+\.csv\.gz$" find-files (my-pmap 1 insert-file))
;(time (->> #"green_.+\.csv\.gz$" find-files (my-pmap 4 insert-file)))
;(-> #"green_.+\.csv\.gz$" find-files count)


(defn -main []
  (println "Hello, World!"))
