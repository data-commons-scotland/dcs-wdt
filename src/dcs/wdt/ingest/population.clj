(ns dcs.wdt.ingest.population
  (:require [clojure.string :as str]
            [clojure.java.io :as io]
            [taoensso.timbre :as log]
            [dcs.wdt.ingest.shared :as shared])
  (:import java.util.Date))

(def expected-year-totals {2011 5299900
                           2012 5313600
                           2013 5327700
                           2014 5347600
                           2015 5373000
                           2016 5404700
                           2017 5424800
                           2018 5438100
                           2019 5463300})

(def csv-dir "data/ingesting/population")

(def sparql "
PREFIX qb: <http://purl.org/linked-data/cube#>
PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
PREFIX pdmx: <http://purl.org/linked-data/sdmx/2009/dimension#>
PREFIX sdmx: <http://statistics.gov.scot/def/dimension/>
PREFIX snum: <http://statistics.gov.scot/def/measure-properties/>
PREFIX uent: <http://statistics.data.gov.uk/def/statistical-entity#>
PREFIX ugeo: <http://statistics.data.gov.uk/def/statistical-geography#>

SELECT
?year
?region
?count

WHERE {
  VALUES ?areaType {
                <http://statistics.gov.scot/id/statistical-entity/S92>
                <http://statistics.gov.scot/id/statistical-entity/S12> }

  ?areaUri uent:code ?areaType;
           ugeo:status 'Live' ;
           rdfs:label ?region .

  ?populationUri qb:dataSet <http://statistics.gov.scot/data/population-estimates-current-geographic-boundaries> ;
                 pdmx:refArea ?areaUri ;
                 pdmx:refPeriod ?periodUri ;
                 sdmx:age <http://statistics.gov.scot/def/concept/age/all> ;
                 sdmx:sex <http://statistics.gov.scot/def/concept/sex/all> ;
                 snum:count ?count .

  ?periodUri rdfs:label ?year .

  FILTER (?year >= 2011) .
}
")

(defn csv-file-from-sparql []
  (log/infof "Executing SPARQL query for population against: %s" shared/scotgov-service-url)
  (let [contents (shared/exec-sparql shared/scotgov-service-url sparql)
        file (io/file (str csv-dir
                           "/"
                           (.format shared/yyyy-MM-dd-format (Date.))
                           "_"
                           (shared/max-year-from-sparql-query-result contents) "-12-31" ;; assume whole-year contents
                           "/"
                           "extract.csv"))]
    (log/infof "CSV rows: %s" (count (str/split-lines contents)))
    (io/make-parents (.getAbsolutePath file))
    (spit file contents)
    (log/infof "Writing CSV file: %s" (.getAbsolutePath file))))

(defn customise-map
  "Converts an externally-oriented map to an internally-oriented map."
  [m]
  (let [region (let [v (get m "region")]
                 (get shared/region-aliases v v))]
    (if (contains? shared/regions-set region)
      {:region region
       :year   (Integer/parseInt (get m "year"))
       :count  (Integer/parseInt (get m "count"))}
      (do (log/debugf "Ignoring: %s" m)
          nil))))

(defn csv-file-to-maps
  "Parses a population CSV file
  to return a seq of :population maps (internal DB records)."
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
                (map #(assoc % :record-type :population)))]
    (when-let [error (shared/check-year-totals :count expected-year-totals db)]
      (throw (RuntimeException. (format "population has year-totals error...\nExpected: %s\nActual: %s" (first error) (second error)))))
    db))