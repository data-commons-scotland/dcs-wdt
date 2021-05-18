(ns dcs.wdt.ingest.household-co2e
  (:require [clojure.string :as str]
            [clojure.java.io :as io]
            [taoensso.timbre :as log]
            [dcs.wdt.ingest.shared :as shared]))

(def expected-year-totals {2011 6767739.95M
                           2012 6304982.13M
                           2013 5977863.2M
                           2014 5946618.63M
                           2015 5937541.76M
                           2016 5971328.95M
                           2017 5864214.51M
                           2018 5759373.09M
                           2019 5664989.68M})

(def csv-file-str "data/ingesting/household-co2e/csv-extract/2011-onwards.csv")

(def region-column-label "Region")

(defn split-by-year [m]
  (let [region (get m region-column-label)
        ;; Remove the region entry
        remaining-m (dissoc m region-column-label)]
    (for [[k v] remaining-m]
      {:region region
       :year   k
       :tonnes v})))

(defn customise-map
  "Converts an externally-oriented map to an internally-oriented map."
  [m]
  (let [region (-> m
                   :region
                   (as-> v0
                         (get shared/region-aliases v0 v0)))]
    (if (contains? shared/regions-set region)
      {:region region
       :year   (Integer/parseInt (:year m))
       :tonnes (bigdec (:tonnes m))}
      (do (log/debugf "Ignoring: %s" m)
          nil))))

(defn csv-file-to-maps
  "Parses a household-co2e CSV file
  to return a seq of :household-co2e maps (internal DB records)."
  [file]
  (let [customise-map (partial shared/customise-map customise-map)]
    (->> file
         (#(do (log/infof "Reading CSV file: %s" (.getAbsolutePath %)) %))
         shared/csv-file-to-maps
         (#(do (log/infof "CSV data rows: %s" (count %)) %))
         (map split-by-year)
         flatten
         (#(do (log/infof "Candidate records: %s" (count %)) %))
         (map customise-map)
         (remove nil?)
         (#(do (log/infof "Accepted records: %s" (count %)) %)))))

(defn db-from-csv-file []
  (let [db (->> csv-file-str
                io/file
                csv-file-to-maps
                (map #(assoc % :record-type :household-co2e)))]
    (when-let [error (shared/check-year-totals :tonnes expected-year-totals db)]
      (throw (RuntimeException. (format "household-co2e has year-totals error...\nExpected: %s\nActual: %s" (first error) (second error)))))
    db))
