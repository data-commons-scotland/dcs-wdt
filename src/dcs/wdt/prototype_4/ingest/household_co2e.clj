(ns dcs.wdt.prototype-4.ingest.household-co2e
  (:require [clojure.string :as str]
            [taoensso.timbre :as log]
            [dcs.wdt.prototype-4.ingest.shared :as shared]))

(def expected-year-totals {2017 5863249
                           2018 5759986
                           2019 5664991}) ;; Upstream provided value is 5664990

(defn customise-map [m]
  (let [area (-> m
                 (get "Local Authority")
                 (str/replace "†" "")
                 (as-> v0
                       (get shared/area-aliases v0 v0)))]
    (if (contains? shared/areas-set area)
      {:area   area
       :tonnes (-> m
                   (get "Carbon Impact (TCO2e)")
                   (str/replace "," "")
                   Integer/parseInt)}
      (do (log/debugf "Ignoring: %s" m)
          nil))))

(defn csv-file-to-maps [file]
  (let [year (Integer/parseInt (re-find #"\d{4}" (.getName file)))
        customise-map (partial shared/customise-map-fn customise-map)]
    (->> file
         (#(do (log/infof "Reading CSV file: %s" (.getAbsolutePath %)) %))
         shared/csv-file-to-maps
         (#(do (log/infof "CSV data rows: %s" (count %)) %))
         (#(do (log/infof "Candidate records: %s" (count %)) %))
         (map customise-map)
         (remove nil?)
         (#(do (log/infof "Accepted records: %s" (count %)) %))
         (map #(assoc % :year year)))))

(defn db-from-csv-files []
  (let [db (->> "data/ingesting/household-co2e/csv-extracts/"
                shared/find-csv-files
                (map csv-file-to-maps)
                flatten
                (map #(assoc % :record-type :household-co2e)))]
    (when-let [error (shared/check-year-totals :tonnes expected-year-totals db)]
      (throw (RuntimeException. (format "household-co2e has year-totals error...\nExpected: %s\nActual: %s" (first error) (second error)))))
    (log/infof "household-co2e records: %s" (count db))
    db))

