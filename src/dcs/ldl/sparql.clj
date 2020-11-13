(ns dcs.ldl.sparql
  (:require
    [taoensso.timbre :as log]
    [clj-http.client :as http]
    [clojure.string :as str]
    [clojure.data.csv :as csv]
    [clojure.pprint :as pp])
  (:import java.net.URLEncoder))

; Convert the CSV structure to a list-of-maps structure.
(defn to-maps [csv-data]
  (map zipmap (->> (first csv-data)
                   (map keyword)
                   repeat)
       (rest csv-data)))

; Map the name of a SPARQL service to its URL.
(def service-urls {:scotgov  "http://statistics.gov.scot/sparql"
                   :wikidata "https://query.wikidata.org/sparql"
                   :wikibase "http://strf8b46abcf478:8282/proxy/wdqs/bigdata/namespace/wdq/sparql"})

; Ask the service to execute the given SPARQL query
; and return its result as a list-of-maps.
(defn exec-query [service-name sparql]
  (->> (http/post (service-name service-urls)
                  {:body    (str "query=" (URLEncoder/encode sparql))
                   :headers {"Accept"       "text/csv"
                             "Content-Type" "application/x-www-form-urlencoded"}
                   :debug   false})
       :body
       csv/read-csv
       to-maps))

(def areas-sparql "

PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
PREFIX uent: <http://statistics.data.gov.uk/def/statistical-entity#>
PREFIX ugeo: <http://statistics.data.gov.uk/def/statistical-geography#>

SELECT 
  (strafter(str(?areaUri), 'http://statistics.gov.scot/id/statistical-geography/') as ?code) 
  ?label

WHERE {
  ?areaUri uent:code <http://statistics.gov.scot/id/statistical-entity/S12> ;
           ugeo:status 'Live' ;
           rdfs:label ?label .
}
")

(defn get-areas []
  (->> areas-sparql
       (exec-query :scotgov)))

(def populations-sparql "

PREFIX qb: <http://purl.org/linked-data/cube#>
PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
PREFIX pdmx: <http://purl.org/linked-data/sdmx/2009/dimension#>
PREFIX sdmx: <http://statistics.gov.scot/def/dimension/>
PREFIX snum: <http://statistics.gov.scot/def/measure-properties/>
PREFIX uent: <http://statistics.data.gov.uk/def/statistical-entity#>
PREFIX ugeo: <http://statistics.data.gov.uk/def/statistical-geography#>

SELECT 
  (strafter(str(?areaUri), 'http://statistics.gov.scot/id/statistical-geography/') as ?code) 
  ?label
  ?year
  ?population

WHERE {
  ?areaUri uent:code <http://statistics.gov.scot/id/statistical-entity/S12> ;
           ugeo:status 'Live' ;
           rdfs:label ?label .
           
  ?populationUri qb:dataSet <http://statistics.gov.scot/data/population-estimates-current-geographic-boundaries> ;
                 pdmx:refArea ?areaUri ;
                 pdmx:refPeriod ?periodUri ;
                 sdmx:age <http://statistics.gov.scot/def/concept/age/all> ;
                 sdmx:sex <http://statistics.gov.scot/def/concept/sex/all> ;
                 snum:count ?population .
  
  ?periodUri rdfs:label ?year .
}
")

(defn get-popuations []
  (->> populations-sparql
       (exec-query :scotgov)))

(def pq-number-sparql "

SELECT distinct (strafter(str(?entity), 'http://strf8b46abcf478/entity/') as ?pqnumber)  

WHERE {  
  ?entity ?label 'LABEL'@en .
  SERVICE wikibase:label { bd:serviceParam wikibase:language 'en'. } 
}
")

(defn get-pq-number [label]
  (let [response (exec-query :wikibase (str/replace pq-number-sparql "LABEL" label))
        n (count response)]
    (when (not= 1 n)
      (throw (RuntimeException. (str n " found"))))
    (-> response
        first
        :pqnumber)))
