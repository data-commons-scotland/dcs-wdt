(ns dcs.wdt.ingest.household-waste
  (:require [clojure.string :as str]
            [clojure.java.io :as io]
            [taoensso.timbre :as log]
            [dcs.wdt.ingest.shared :as shared]))

(def expected-year-totals {2011 2536497                     ;; Upstream provided value is 2606759
                           2012 2445584                     ;; Upstream provided value is 2500995
                           2013 2413544                     ;; Upstream provided value is 2412630
                           2014 2458875                     ;; Upstream provided value is 2459557
                           2015 2468472                     ;; Upstream provided value is 2468781
                           2016 2498516                     ;; Upstream provided value is 2498978
                           2017 2460059                     ;; Upstream provided value is 2460820
                           2018 2404503                     ;; Upstream provided value is 2405246
                           2019 2421207})                   ;; Upstream provided value is 2421790

(def csv-file-str "data/ingesting/household-waste/csv-extract/2011-onwards.csv")

(def sparql "
PREFIX qb: <http://purl.org/linked-data/cube#>
PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
PREFIX pdmx: <http://purl.org/linked-data/sdmx/2009/dimension#>
PREFIX sdmx: <http://statistics.gov.scot/def/dimension/>
PREFIX snum: <http://statistics.gov.scot/def/measure-properties/>
PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>

SELECT
  ?year
  ?region
  ?management
  ?material
  ?tonnes

WHERE {
       VALUES ?wasteManagementUri {
          <http://statistics.gov.scot/def/concept/waste-management/recycled>
          <http://statistics.gov.scot/def/concept/waste-management/landfilled>
          <http://statistics.gov.scot/def/concept/waste-management/other-diversion>
        } # i.e. ignore the 'pre 2014' data and 'Waste Generated' summed data
       ###VALUES ?wasteCategoryUri {
       ###   <http://statistics.gov.scot/def/concept/type/total-waste>
       ###} # i.e. ignore the individual materials - just get their sum
       ### VALUES ?areaUri {
       ###  <http://statistics.gov.scot/id/statistical-geography/S12000017>
       ### } # i.e. just Highland

       ?tonnageObs qb:dataSet <http://statistics.gov.scot/data/household-waste> .
       ?tonnageObs pdmx:refArea ?areaUri .
       ?tonnageObs pdmx:refPeriod ?periodUri .
       ?tonnageObs sdmx:wasteCategory ?wasteCategoryUri .
       ?tonnageObs sdmx:wasteManagement ?wasteManagementUri .
       ?tonnageObs snum:count ?tonnes .

       ?areaUri rdfs:label ?region .
       ?periodUri rdfs:label ?year .
       ?wasteCategoryUri rdfs:label ?material .
       ?wasteManagementUri rdfs:label ?management .
}
")

(defn csv-file-from-sparql []
  (log/infof "Executing SPARQL query for household-waste against: %s" shared/scotgov-service-url)
  (let [contents (shared/exec-sparql shared/scotgov-service-url sparql)
        file (io/file csv-file-str)]
    (log/infof "CSV rows: %s" (count (str/split-lines contents)))
    (io/make-parents (.getAbsolutePath file))
    (spit file contents)
    (log/infof "Writing CSV file: %s" (.getAbsolutePath file))))

(defn customise-map
  "Converts an externally-oriented map to an internally-oriented map."
  [m]
  (let [region (let [v (get m "region")]
                 (get shared/region-aliases v v))
        material (let [v (get m "material")]
                   (get shared/material-aliases v v))
        management (get m "management")]
    (if (and (contains? shared/regions-set region)
             (contains? shared/materials-set material)
             (contains? shared/managements-set management))
      {:region     region
       :year       (Integer/parseInt (get m "year"))
       :material   material
       :management management
       :tonnes     (Integer/parseInt (get m "tonnes"))}
      (do (log/debugf "Ignoring: %s" m)
          nil))))

(defn csv-file-to-maps
  "Parses a household-waste CSV file
  to return a seq of :household-waste maps (internal DB records)."
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
                (map #(assoc % :record-type :household-waste)))]
    (when-let [error (shared/check-year-totals :tonnes expected-year-totals db)]
      (throw (RuntimeException. (format "household-waste has year-totals error...\nExpected: %s\nActual: %s" (first error) (second error)))))
    db))