(ns dcs.wdt.prototype-4.api
  (:require [clojure.pprint :as pp]
            [taoensso.timbre :as log]
            [dcs.wdt.prototype-4.ingest.api :as ingest]
            [dcs.wdt.prototype-4.export.time-series-map :as time-series-map]
            [dcs.wdt.prototype-4.export.data_grid_and_graph :as data-grid-and-graph]
            [dcs.wdt.prototype-4.export.general-use :as general-use]
            [dcs.wdt.prototype-4.export.cluster-map :as cluster-map]))

(defn csv-files-from-sparql [_]
  (log/set-level! :info)
  (ingest/csv-files-from-sparql))

(defn db [_]
  (log/set-level! :info)
  (let [db (ingest/db-from-csv-files)]
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
  (let [db (ingest/db-from-csv-files)]
    (general-use/generate-csv-files db)))

(defn generate-json-file-for-cluster-map [_]
  (log/set-level! :info)
  (let [db (ingest/db-from-csv-files)]
    (cluster-map/generate-json-file db)))
