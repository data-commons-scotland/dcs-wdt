(ns dcs.wdt.ingest.bin-collection
  (:require [clojure.string :as str]
            [taoensso.timbre :as log]
            [dcs.wdt.ingest.shared :as shared]))

(def expected-year-totals {2018 32276.84M
                           2019 32166.32M
                           2020 33196.04M
                           2021 16820.22M})

(def recycling-aliases #{"Recycling"
                         "Stg Recycling"})

(def internal-transfer-aliases #{"Internal Stirling Council Transfer"
                                 "405 Transfer Station"})

(def material-aliases
  {"01 Household to Avondale"     "Household and similar wastes"
   "02 Garden Waste"              "Vegetal wastes"
   "105 Mixed Paper"              "Paper and cardboard wastes"
   "106 Fibre (Paper & Card)"     "Paper and cardboard wastes"
   "107 Containers Stream"        "Plastic wastes"
   "108 Comingled Organic"        "Animal and mixed food waste"
   "114 CITY CENTRE PURPLE SACKS" "Household and similar wastes"
   "114 City Centre purple sacks" "Household and similar wastes"
   "17 Cardboard"                 "Paper and cardboard wastes"
   "72 Mixed Glass"               "Glass wastes"})

(defn _2021-onwards?
  [m]
  (contains? m "Category"))

(defn customise-map
  "Converts an externally-oriented map to an internally-oriented map."
  [m]
  (let [internal-transfer?-column-label (if (_2021-onwards? m) "Route" "Account")
        internal-transfer? (contains? internal-transfer-aliases (str/trim (get m internal-transfer?-column-label)))
        material-column-label (if (_2021-onwards? m) "Waste Collected" "Waste")
        material (get material-aliases (str/trim (get m material-column-label)))
        recycling?-column-label (if (_2021-onwards? m) "Category" "Contract")
        recycling? (contains? recycling-aliases (str/trim (get m recycling?-column-label)))
        missed-bin? (= "189 Missed Bins" (str/trim (get m "Account")))]
    (if (and (not internal-transfer?)
             (contains? shared/materials-set material))
      {:quarter     (Integer/parseInt (get m "Quarter"))
       :material    material
       :recycling?  recycling?
       :missed-bin? missed-bin?
       :tonnes      (bigdec (get m "Quantity"))}
      (do (log/debugf "Ignoring: %s" m)
          nil))))

(defn rollup
  "Roll-up the tonnes for each (quarter, material, recycling?, missed-bin?) tuple"
  [coll]
  (->> coll
       (group-by (juxt :quarter :material :recycling? :missed-bin?))
       (map (fn [[[quarter material recycling? missed-bin?] sub-coll]] {:quarter     quarter
                                                                        :material    material
                                                                        :recycling?  recycling?
                                                                        :missed-bin? missed-bin?
                                                                        :tonnes      (->> sub-coll
                                                                                          (map :tonnes)
                                                                                          (apply +))}))))

(defn csv-file-to-maps
  "Parses a bin-collection CSV file
  to return a seq of :bin-collection maps (internal DB records)."
  [file]
  (let [year (Integer/parseInt (second (re-find #"(\d{4})\.csv" (.getName file))))
        customise-map (partial shared/customise-map customise-map)]
    (->> file
         (#(do (log/infof "Reading CSV file: %s" (.getAbsolutePath %)) %))
         shared/csv-file-to-maps
         (#(do (log/infof "CSV data rows: %s" (count %)) %))
         (map customise-map)
         (remove nil?)
         (#(do (log/infof "Records to be aggregated: %s" (count %)) %))
         rollup
         (#(do (log/infof "Accepted records: %s" (count %)) %))
         (map #(assoc % :year year)))))


(defn db-from-csv-files []
  (let [db (->> "data/ingesting/bin-collection/originals/"
                shared/find-csv-files
                (map csv-file-to-maps)
                flatten
                (map #(assoc % :record-type :bin-collection
                               :region "Stirling")))]
    (when-let [error (shared/check-year-totals :tonnes expected-year-totals db)]
      (throw (RuntimeException. (format "bin-collection has year-totals error...\nExpected: %s\nActual: %s" (first error) (second error)))))
    db))
