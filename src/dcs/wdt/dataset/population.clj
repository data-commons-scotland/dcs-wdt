(ns dcs.wdt.dataset.population
  (:require [clojure.java.io :as io]
            [clojure.data.csv :as csv]
            [taoensso.timbre :as log]
            [dcs.wdt.misc :as misc]
            [dcs.wdt.wikibase-sparql :as wb-sparql]
            [dcs.wdt.writing :as writing]
            [dcs.wdt.dataset.base :as base :refer [has-quantity for-time for-concept]]
            [dcs.wdt.dataset.area :refer [for-area]]))

(def population-the-concept "population (concept)")

(defn- mapper [row]
  [(str "population " (:areaLabel row) " " (:year row))
   (str "the population of " (:areaLabel row) " in " (:year row))
   [[(wb-sparql/pqid has-quantity) (writing/datatype has-quantity) (:quantity row)]
    [(wb-sparql/pqid for-area) (writing/datatype for-area) (wb-sparql/pqid (:areaLabel row))]
    [(wb-sparql/pqid for-time) (writing/datatype for-time) (:year row)]
    [(wb-sparql/pqid for-concept) (writing/datatype for-concept) (wb-sparql/pqid population-the-concept)]]])

(def concept-dataset 
  [{:label population-the-concept :description "population, the concept"}])

(defn dataset []
  (->> "population-dataset.sparql"
    io/resource
    slurp
    (misc/exec-sparql "http://statistics.gov.scot/sparql")
    (misc/patch :areaLabel)))

(defn write-to-wikibase [wb-csrf-token dataset]
  (log/info "Writing supporting data (dimension values, predicates, etc.)")
  (writing/write-dataset-to-wikibase-items wb-csrf-token base/concept-mapper concept-dataset)
  (log/info "Writing essence data")
  (writing/write-dataset-to-wikibase-items wb-csrf-token mapper dataset))
