(ns dcs.wdt.dataset.population
  (:require [clojure.java.io :as io]
            [clojure.data.csv :as csv]
            [dcs.wdt.misc :as misc]
            [dcs.wdt.wikibase-sparql :as wb-sparql]
            [dcs.wdt.writing :as writing]
            [dcs.wdt.dataset.base :refer [HAS_QUANTITY FOR_TIME FOR_CONCEPT]]
            [dcs.wdt.dataset.area :refer [FOR_AREA]]))

(def POPULATION_THE_CONCEPT "population (concept)")

(defn- essence-mapper [row]
  [(str "population " (:areaLabel row) " " (:year row))
   (str "the population of " (:areaLabel row) " in " (:year row))
   [(let [p HAS_QUANTITY] [(wb-sparql/pqid p) (writing/datatype p) (:quantity row)])
    (let [p FOR_AREA] [(wb-sparql/pqid p) (writing/datatype p) (wb-sparql/pqid (:areaLabel row))])
    (let [p FOR_TIME] [(wb-sparql/pqid p) (writing/datatype p) (:year row)])
    (let [p FOR_CONCEPT] [(wb-sparql/pqid p) (writing/datatype p) (wb-sparql/pqid POPULATION_THE_CONCEPT)])]])

(defn dataset []
  (->> "population-dataset.sparql"
    io/resource
    slurp
    (misc/exec-sparql "http://statistics.gov.scot/sparql")
    (misc/patch :areaLabel)))

(defn write-to-wikibase [wb-csrf-token dataset]
  (writing/write-dataset-to-wikibase-items wb-csrf-token [POPULATION_THE_CONCEPT "population, the concept" []] [:placeholder-row])
  (writing/write-dataset-to-wikibase-items wb-csrf-token essence-mapper dataset))
