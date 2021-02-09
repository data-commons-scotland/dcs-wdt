(ns dcs.wdt.prototype-4.db
  (:require [clojure.pprint :as pp]
            [taoensso.timbre :as log]
            [dcs.wdt.prototype-4.dimensions :as dims]
            [dcs.wdt.prototype-4.ingest.api :as ingest]))

(def important-dims [:record-type
                     :region :business-sector :year :quarter
                     :type :management :site-name :permit :easting :northing :io :ewc-description
                     :population :tonnes :tonnes-input :tonnes-treated-recovered :tonnes-output])

(defn ord
  "Associates each important dimension with an ordinal value, for use when sorting."
  [dimension]
  (.indexOf important-dims dimension))

(defn select-important-dims [sub-db]
  (->> sub-db
       first
       keys
       (filter #(contains? (set important-dims) %))
       (sort-by ord)))

(defn print-samples [db]
  (doseq [record-type dims/record-types]
    (let [sub-db (filter #(= record-type (:record-type %)) db)
          headers (select-important-dims sub-db)]
      (pp/print-table headers (repeatedly 5 #(rand-nth sub-db))))))

(defn describe
  "Returns a seq of maps, each describing the records of a particular type."
  [db]
  (for [record-type dims/record-types]
    (let [sub-db (filter #(= record-type (:record-type %)) db)]
      {:record-type record-type
       :count (count sub-db)
       :important-dimensions (select-important-dims sub-db)
       ;:source (ingest/describe-source record-type)
       })))
