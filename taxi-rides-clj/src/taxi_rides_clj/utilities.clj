(ns taxi-rides-clj.utilities
  (:require [java-time :as t]))

; https://gist.github.com/prasincs/827272
(defn get-hash [type data] (.digest (java.security.MessageDigest/getInstance type) (.getBytes ^String data)))
(defn sha1-hash [data] (->> data (get-hash "sha1") (map #(.substring (Integer/toString (+ (bit-and % 0xff) 0x100) 16) 1)) (apply str)))
(defn sha1-hash-numeric [data] (->> data (get-hash "sha1") (map int)))


; http://yellerapp.com/posts/2014-12-11-14-race-condition-in-clojure-println.html
(defn my-println [& more]
  (do
    (.write *out* (str (t/local-time) " " (clojure.string/join "" more) "\n"))
    (flush)))

(defmacro my-println-f [& forms]
  `(my-println ~'file ": " ~@forms))

(defn getenv
  "Get environment variable's value, converts empty strings to nils"
  ([key]         (getenv key nil))
  ([key default] (let [value (System/getenv key)] (if (-> value count pos?) value default))))

; Used for filtering out duplicate documents, so instead of storing
; them to ES with an id we'll bear the CPU cost here. Documentation
; says the function of filter must not have side effects but I guess
; this is a valid exception.
(def collision-counter (atom 0))

(defn my-distinct
  "Returns distinct values from a seq, as defined by id-getter. Not thread safe!"
  [id-getter coll]
  (let [seen-ids (volatile! #{})
        seen?    (fn [id] (if (contains? @seen-ids id)
                            (and (swap! collision-counter inc) nil) ; Increment counter, return nil
                            (vswap! seen-ids conj id)))]            ; Add to seen-ids, returns truthy for filter
    (filter (comp seen? id-getter) coll)))

(defn make-sampler
  "Partitions input coll randomly into chunks of length n, returns them until exausted and then re-shuffles them."
  [n coll]
  (let [_       (assert (zero? (mod (count coll) n)) "Number of items must be divisible by the group size n!")
        reserve (atom [])]
    (fn [] (-> reserve
               (swap! (fn [reserve]
                         (if (<= (count reserve) 1)
                           (->> coll shuffle (partition n) (into []))
                           (subvec reserve 1))))
               first))))
