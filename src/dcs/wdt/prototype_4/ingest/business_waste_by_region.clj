(ns dcs.wdt.prototype-4.ingest.business-waste-by-region
  (:require [clojure.string :as str]
            [taoensso.timbre :as log]
            [dcs.wdt.prototype-4.ingest.shared :as shared]))

(def expected-year-totals {2011 4025748                     ;; Upstream provided value is 4025733
                           2012 3638689                     ;; Upstream provided value is 3638683
                           2013 3662427                     ;; Upstream provided value is 3662432
                           2014 3310130                     ;; Upstream provided value is 3310131
                           2015 3599066                     ;; Upstream provided value is 3599063
                           2016 3263468
                           2017 3335634                     ;; Upstream provided value is 3335638
                           2018 3236531})                   ;; Upstream provided value is 3236534})

(def type-column-label "waste-category")                    ;; Expected to have been edited into each CSV extract

(defn split-by-region
  "Convert the given map with multiple regions to a seq of maps, each with a single region."
  [m]
  (let [type (get m type-column-label)
        ;; Remove the type entry
        remaining-m (dissoc m type-column-label)]
    (for [[k v] remaining-m]
      {:type   type
       :region k
       :tonnes v})))

(defn customise-map
  "Converts an externally-oriented map to an internally-oriented map."
  [m]
  (let [region (-> m
                   :region
                   str/trim
                   (str/replace "*" "")
                   (as-> v0
                         (get shared/region-aliases v0 v0)))
        type (-> m
                 :type
                 (str/replace "*" "")
                 str/trim
                 (as-> v0
                       (get shared/type-aliases v0 v0)))]
    (if (and (contains? shared/regions-set region)
             (contains? shared/types-set type))
      {:region region
       :type   type
       :tonnes (-> m
                   :tonnes
                   (str/replace "," "")
                   (str/replace "-" "0")
                   str/trim
                   Integer/parseInt)}
      (do (log/debugf "Ignoring: %s" m)
          nil))))

(defn csv-file-to-maps
  "Parses a business-waste-by-region CSV file
  to return a seq of :business-waste-by-region maps (internal DB records)."
  [file]
  (let [year (Integer/parseInt (re-find #"\d{4}" (.getName file)))
        customise-map (partial shared/customise-map customise-map)]
    (->> file
         (#(do (log/infof "Reading CSV file: %s" (.getAbsolutePath %)) %))
         shared/csv-file-to-maps
         (#(do (log/infof "CSV data rows: %s" (count %)) %))
         (map split-by-region)
         flatten
         (#(do (log/infof "Candidate records: %s" (count %)) %))
         (map customise-map)
         (remove nil?)
         (#(do (log/infof "Accepted records: %s" (count %)) %))
         (map #(assoc % :year year)))))

(defn db-from-csv-files []
  (let [db (->> "data/ingesting/business-waste/csv-extracts/by-region/"
                shared/find-csv-files
                (map csv-file-to-maps)
                flatten
                (map #(assoc % :record-type :business-waste-by-region)))]
    (when-let [error (shared/check-year-totals :tonnes expected-year-totals db)]
      (throw (RuntimeException. (format "business-waste-by-region has year-totals error...\nExpected: %s\nActual: %s" (first error) (second error)))))
    db))

