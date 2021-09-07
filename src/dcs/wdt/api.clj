(ns dcs.wdt.api
  (:require [taoensso.timbre :as log]
            [dcs.wdt.ingest.api :as ingest]
            [dcs.wdt.export.time-series-map :as time-series-map]
            [dcs.wdt.export.data_grid_and_graph :as data-grid-and-graph]
            [dcs.wdt.export.general-use :as general-use]
            [dcs.wdt.export.cluster-map :as cluster-map]))

(defn csv-files-from-sparql [_]
  (log/set-level! :info)
  (ingest/csv-files-from-sparql))

(defn db [_]
  (log/set-level! :info)
  (let [db (ingest/db-from-2nd-pass (ingest/db-from-csv-files))]
    (general-use/print-describing-tables db)))

(defn generate-js-files-for-time-series-map [_]
  (log/set-level! :info)
  (let [db (ingest/db-from-csv-files)]
    (time-series-map/generate-js-files db)))

(defn generate-json-file-for-data-grid-and-graph [_]
  (log/set-level! :info)
  (let [db (ingest/db-from-csv-files)]
    (data-grid-and-graph/generate-json-file db)))

(defn generate-csv-files-for-general-use [_]
  (log/set-level! :info)
  (let [db (ingest/db-from-2nd-pass (ingest/db-from-csv-files))]
    (general-use/generate-csv-files db)
    (general-use/generate-readme-file db)
    (general-use/generate-webapp-file db)))

(defn generate-json-file-for-cluster-map [_]
  (log/set-level! :info)
  (let [db (ingest/db-from-csv-files)]
    (cluster-map/generate-json-file db)))


(comment

  (db 0)

  (+ 2 5)

  (log/info "hi")

  (inc 3)

  )