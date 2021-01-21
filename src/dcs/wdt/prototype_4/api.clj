(ns dcs.wdt.prototype-4.api
  (:require [clojure.pprint :as pp]
            [taoensso.timbre :as log]
            [dcs.wdt.prototype-4.dimensions :as dim]
            [dcs.wdt.prototype-4.ingest.api :as ingest]
            [dcs.wdt.prototype-4.export.time-series-map :as time-series-map]))

(defn counts [db]
  (into {}
        (cons [:all (count db)]
              (map (fn [type] [type (count (filter #(= type (:record-type %)) db))])
                   dim/record-types))))

(defn print-samples [db]
  (doseq [record-type dim/record-types]
    (let [sub-db (filter #(= record-type (:record-type %)) db)]
      (pp/print-table (repeatedly 5 #(rand-nth sub-db ))))))

(defn glimpse-db [_]
  (log/set-level! :info)
  (let [db (ingest/db-from-csv-files)]
    (print-samples db)
    (pp/pprint (counts db))))

(defn csv-files-from-sparql [_]
  (log/set-level! :info)
  (ingest/csv-files-from-sparql))

(defn generate-js-files-for-time-series-map [_]
  (log/set-level! :info)
  (let [db (ingest/db-from-csv-files)]
    (time-series-map/generate-js-files db)))