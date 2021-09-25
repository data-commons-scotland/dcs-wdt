(ns dcs.wdt.ingest.co2e-multiplier
  (:require [clojure.java.io :as io]
            [taoensso.timbre :as log]
            [dcs.wdt.ingest.shared :as shared]))

(def expected-count 37)

(def csv-dir "data/ingesting/co2e-multiplier")

(defn customise-map
  "Converts an externally-oriented map to an internally-oriented map."
  [m]
  {:material   (get m "Waste Stream")
   :multiplier (bigdec (get m "Carbon Weighting"))})

(defn csv-file-to-maps
  "Parses a population CSV file
  to return a seq of :co2e-multiplier maps (internal DB records)."
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
  (let [db (->> (str csv-dir "/" (shared/dirname-with-max-supplied-date csv-dir) "/extract.csv")
                io/file
                csv-file-to-maps
                (map #(assoc % :record-type :co2e-multiplier)))]
    (let [actual-count (->> db (map :material) distinct count)]
      (when (not= expected-count actual-count)
        (throw (RuntimeException. (format "co2e-multipler has a count error...\nExpected: %s\nActual: %s" expected-count actual-count)))))
    db))