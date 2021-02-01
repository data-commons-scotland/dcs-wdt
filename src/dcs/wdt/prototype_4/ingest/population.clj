(ns dcs.wdt.prototype-4.ingest.population
  (:require [clojure.string :as str]
            [clojure.java.io :as io]
            [taoensso.timbre :as log]
            [dcs.wdt.prototype-4.ingest.shared :as shared]))

(def expected-year-totals {2011 5299900
                           2012 5313600
                           2013 5327700
                           2014 5347600
                           2015 5373000
                           2016 5404700
                           2017 5424800
                           2018 5438100
                           2019 5463300})

(def csv-file-str "data/ingesting/population/csv-extract/2011-onwards.csv")

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
?area
?population

WHERE {
  VALUES ?areaType {
                <http://statistics.gov.scot/id/statistical-entity/S92>
                <http://statistics.gov.scot/id/statistical-entity/S12> }

  ?areaUri uent:code ?areaType;
           ugeo:status 'Live' ;
           rdfs:label ?area .

  ?populationUri qb:dataSet <http://statistics.gov.scot/data/population-estimates-current-geographic-boundaries> ;
                 pdmx:refArea ?areaUri ;
                 pdmx:refPeriod ?periodUri ;
                 sdmx:age <http://statistics.gov.scot/def/concept/age/all> ;
                 sdmx:sex <http://statistics.gov.scot/def/concept/sex/all> ;
                 snum:count ?population .

  ?periodUri rdfs:label ?year .

  FILTER (?year >= 2011) .
}
")

(defn csv-file-from-sparql []
  (log/infof "Executing SPARQL query for population against: %s" shared/scotgov-service-url)
  (let [contents (shared/exec-sparql shared/scotgov-service-url sparql)
        file (io/file csv-file-str)]
    (log/infof "CSV rows: %s" (count (str/split-lines contents)))
    (io/make-parents (.getAbsolutePath file))
    (spit file contents)
    (log/infof "Writing CSV file: %s" (.getAbsolutePath file))))

(defn customise-map [m]
  (let [area (let [v (get m "area")]
               (get shared/area-aliases v v))]
    (if (contains? shared/areas-set area)
      {:area area
       :year (Integer/parseInt (get m "year"))
       :population (Integer/parseInt (get m "population"))}
      (do (log/debugf "Ignoring: %s" m)
          nil))))

(defn csv-file-to-maps [file]
  (let [customise-map (partial shared/customise-map-fn customise-map)]
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
                (map #(assoc % :record-type :population)))]
    (when-let [error (shared/check-year-totals :population expected-year-totals db)]
      (throw (RuntimeException. (format "population has year-totals error...\nExpected: %s\nActual: %s" (first error) (second error)))))
    (log/infof "population records: %s" (count db))
    db))