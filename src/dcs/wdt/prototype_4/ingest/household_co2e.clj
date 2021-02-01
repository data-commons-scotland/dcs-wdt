(ns dcs.wdt.prototype-4.ingest.household-co2e
  (:require [clojure.string :as str]
            [clojure.java.io :as io]
            [taoensso.timbre :as log]
            [dcs.wdt.prototype-4.ingest.shared :as shared]))

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

(def area-column-label "Region")

(defn split-by-year [m]
  (let [area (get m area-column-label)
        ;; Remove the area entry
        remaining-m (dissoc m area-column-label)]
    (for [[k v] remaining-m]
      {:area area
       :year k
       :tonnes v})))

(defn customise-map [m]
  (let [area (-> m
                 :area
                 (as-> v0
                       (get shared/area-aliases v0 v0)))]
    (if (contains? shared/areas-set area)
      {:area area
       :year (Integer/parseInt (:year m))
       :tonnes (bigdec (:tonnes m))}
      (do (log/debugf "Ignoring: %s" m)
          nil))))

(defn csv-file-to-maps [file]
  (let [customise-map (partial shared/customise-map-fn customise-map)]
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
    (log/infof "household-co2e records: %s" (count db))
    db))
