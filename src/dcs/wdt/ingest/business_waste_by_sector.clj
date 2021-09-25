(ns dcs.wdt.ingest.business-waste-by-sector
  (:require [clojure.string :as str]
            [taoensso.timbre :as log]
            [dcs.wdt.ingest.shared :as shared]))

(def expected-year-totals {2011 4025735                     ;; Upstream provided value is 4025733
                           2012 3639623                     ;; Upstream provided value is 3639627
                           2013 3662431                     ;; Upstream provided value is 3662432
                           2014 3310123                     ;; Upstream provided value is 3310131
                           2015 3599053                     ;; Upstream provided value is 3599063
                           2016 3263466                     ;; Upstream provided value is 3263468
                           2017 3335632                     ;; Upstream provided value is 3335638
                           2018 3236538})                   ;; Upstream provided value is 3236534})

(def csv-dir "data/ingesting/business-waste-by-sector")

(def material-column-label "waste-category")                ;; Expected to have been edited into each CSV extract

(defn split-by-business-sector
  "Convert the given map with multiple business-sectors to a seq of maps, each with a single business-sector."
  [m]
  (let [material (get m material-column-label)
        ;; Remove the material entry
        remaining-m (dissoc m material-column-label)]
    (for [[k v] remaining-m]
      {:business-sector k
       :material        material
       :tonnes          v})))

(defn customise-map
  "Converts an externally-oriented map to an internally-oriented map."
  [m]
  (let [business-sector (-> m
                            :business-sector
                            (str/replace "†" "")
                            (str/replace "*" "")
                            (str/replace "\n" " ")
                            str/trim
                            (as-> v0
                                  (get shared/business-sector-aliases v0 v0)))
        material (-> m
                     :material
                     (str/replace "†" "")
                     (str/replace "*" "")
                     str/trim
                     (as-> v0
                           (get shared/material-aliases v0 v0)))]
    (if (and (contains? shared/business-sectors-set business-sector)
             (contains? shared/materials-set material))
      {:business-sector business-sector
       :material        material
       :tonnes          (-> m
                            :tonnes
                            (str/replace "," "")
                            (str/replace "-" "0")
                            str/trim
                            Integer/parseInt)}
      (do (log/debugf "Ignoring: %s" m)
          nil))))

(defn csv-file-to-maps
  "Parses a business-waste-by-sector CSV file
  to return a seq of :business-waste-by-sector maps (internal DB records)."
  [file]
  (let [year (Integer/parseInt (re-find #"\d{4}" (.getName file)))
        customise-map (partial shared/customise-map customise-map)]
    (->> file
         (#(do (log/infof "Reading CSV file: %s" (.getAbsolutePath %)) %))
         shared/csv-file-to-maps
         (#(do (log/infof "CSV data rows: %s" (count %)) %))
         (map split-by-business-sector)
         flatten
         (#(do (log/infof "Candidate records: %s" (count %)) %))
         (map customise-map)
         (remove nil?)
         (#(do (log/infof "Accepted records: %s" (count %)) %))
         (map #(assoc % :year year)))))

(defn db-from-csv-files []
  (let [db (->> (str csv-dir "/" (shared/dirname-with-max-supplied-date csv-dir) "/")
                shared/find-csv-files
                (map csv-file-to-maps)
                flatten
                (map #(assoc % :record-type :business-waste-by-sector)))]
    (when-let [error (shared/check-year-totals :tonnes expected-year-totals db)]
      (throw (RuntimeException. (format "business-waste-by-sector has year-totals error...\nExpected: %s\nActual: %s" (first error) (second error)))))
    db))

