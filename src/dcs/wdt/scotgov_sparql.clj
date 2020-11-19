(ns dcs.wdt.scotgov-sparql
  (:require
    [dcs.wdt.misc :as misc]))


(def service-url "http://statistics.gov.scot/sparql")


(def areas-sparql "

PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
PREFIX uent: <http://statistics.data.gov.uk/def/statistical-entity#>
PREFIX ugeo: <http://statistics.data.gov.uk/def/statistical-geography#>

SELECT 
  ?label
  (strafter(str(?areaUri), 'http://statistics.gov.scot/id/statistical-geography/') as ?ukGovCode) 

WHERE {
  ?areaUri uent:code <http://statistics.gov.scot/id/statistical-entity/S12> ;
           ugeo:status 'Live' ;
           rdfs:label ?label .
}
")

(defn areas []
  (->> areas-sparql
    (misc/exec-sparql service-url)
    (misc/patch :label)))


(def populations-sparql "

PREFIX qb: <http://purl.org/linked-data/cube#>
PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
PREFIX pdmx: <http://purl.org/linked-data/sdmx/2009/dimension#>
PREFIX sdmx: <http://statistics.gov.scot/def/dimension/>
PREFIX snum: <http://statistics.gov.scot/def/measure-properties/>
PREFIX uent: <http://statistics.data.gov.uk/def/statistical-entity#>
PREFIX ugeo: <http://statistics.data.gov.uk/def/statistical-geography#>

SELECT 
  ?areaLabel
  ?year
  ?quantity

WHERE {
  #VALUES ?areaLabel { <http://statistics.gov.scot/doc/statistical-geography/S12000033> } # i.e. Aberdeen City only # TODO remove this restriction 
  ?areaUri uent:code <http://statistics.gov.scot/id/statistical-entity/S12> ;
           ugeo:status 'Live' ;
           rdfs:label ?areaLabel .
           
  ?populationUri qb:dataSet <http://statistics.gov.scot/data/population-estimates-current-geographic-boundaries> ;
                 pdmx:refArea ?areaUri ;
                 pdmx:refPeriod ?periodUri ;
                 sdmx:age <http://statistics.gov.scot/def/concept/age/all> ;
                 sdmx:sex <http://statistics.gov.scot/def/concept/sex/all> ;
                 snum:count ?quantity .
  
  ?periodUri rdfs:label ?year .
}
")

(defn populations []
  (->> populations-sparql
       (misc/exec-sparql service-url)
       (misc/patch :areaLabel)))


(def waste-generated-sparql "

PREFIX qb: <http://purl.org/linked-data/cube#>
PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
PREFIX pdmx: <http://purl.org/linked-data/sdmx/2009/dimension#>
PREFIX sdmx: <http://statistics.gov.scot/def/dimension/>
PREFIX snum: <http://statistics.gov.scot/def/measure-properties/>
PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>

SELECT 
    ?year
    ?area
    ?endState
    ?material
    ?tonnes 
    
WHERE {
    VALUES ?periodUri { <http://reference.data.gov.uk/id/year/2017> } # i.e. 2017 only # TODO remove this restriction 

    ?tonnageObs qb:dataSet <http://statistics.gov.scot/data/household-waste> .
    ?tonnageObs pdmx:refArea ?areaUri .
    ?tonnageObs pdmx:refPeriod ?periodUri .
    ?tonnageObs sdmx:wasteCategory ?wasteCategoryUri .
    ?tonnageObs sdmx:wasteManagement ?wasteManagementUri .
    ?tonnageObs snum:count ?tonnes .
  
    ?areaUri rdfs:label ?area .
    ?periodUri rdfs:label ?year .
    ?wasteCategoryUri rdfs:label ?material .
    ?wasteManagementUri rdfs:label ?endState .
}
")

(defn waste-generated []
  (->> waste-generated-sparql
       (misc/exec-sparql service-url)
       (misc/patch :area)))

