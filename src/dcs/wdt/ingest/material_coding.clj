(ns dcs.wdt.ingest.material-coding
  (:require [clojure.string :as str]
            [clojure.java.io :as io]
            [taoensso.timbre :as log]
            [geocoordinates.core :as geo]
            [dcs.wdt.ingest.shared :as shared]))

(def expected-ewc-codes-count 557)

(def csv-file-str "data/ingesting/material-coding/csv-extract/material-coding.csv")

(defn customise-map
  "Converts an externally-oriented map to an internally-oriented map."
  [m]
  (let [material (-> m
                     (get "material")
                     (as-> v0
                           (get shared/material-aliases v0 v0)))]
    (if (contains? shared/materials-set material)
      {:material material
       :ewc-code (get m "ewc-code")}
      (do (log/debugf "Ignoring: %s" m)
          nil))))

(defn csv-file-to-maps
  "Parses a material CSV file
  to return a seq of :material-coding maps (internal DB records)."
  [file]
  (let [customise-map (partial shared/customise-map customise-map)]
    (->> file
         (#(do (log/infof "Reading CSV file: %s" (.getAbsolutePath %)) %))
         shared/csv-file-to-maps
         (#(do (log/infof "CSV data rows: %s" (count %)) %))
         (#(do (log/infof "Candidate records: %s" (count %)) %))
         (map customise-map)
         (remove nil?)
         (#(do (log/infof "Accepted records: %s" (count %)) %)))))

(defn db-from-csv-file []
  (let [db (->> csv-file-str
                io/file
                csv-file-to-maps
                (map #(assoc % :record-type :material-coding)))]
    (let [actual-ewc-codes-count (->> db (map :ewc-code) distinct count)]
      (when (not= expected-ewc-codes-count actual-ewc-codes-count)
        (throw (RuntimeException. (format "material-coding has a count error...\nExpected: %s\nActual: %s" expected-ewc-codes-count actual-ewc-codes-count)))))
    db))