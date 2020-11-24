(ns dcs.wdt.dataset.population
  (:require [clojure.java.io :as io]
            [clojure.data.csv :as csv]
            [taoensso.timbre :as log]
            [dcs.wdt.misc :as misc]
            [dcs.wdt.wikibase-sparql :as wbq]
            [dcs.wdt.writing :as writing]
            [dcs.wdt.reading :as reading]
            [dcs.wdt.dataset.base :as base :refer [has-quantity for-time instance-of]]
            [dcs.wdt.dataset.area :refer [for-area]]))

(def population-class "population (class)")

(defn- mapper [row]
  [(str "population " (:area row) " " (:year row))
   (str "the population of " (:area row) " in " (:year row))
   [[(wbq/pqid-s has-quantity) (reading/datatype has-quantity) (:population row)]
    [(wbq/pqid-s for-area) (reading/datatype for-area) (wbq/pqid-s (:area row))]
    [(wbq/pqid-s for-time) (reading/datatype for-time) (:year row)]
    [(wbq/pqid-s instance-of) (reading/datatype instance-of) (wbq/pqid-s population-class)]]])

(def class-dataset
  [{:label population-class :description "population, the class"}])

(defn dataset []
  (->> "population-dataset.sparql"
    io/resource
    slurp
    (misc/exec-sparql "http://statistics.gov.scot/sparql")
    (misc/patch :area)))

(defn write-to-wikibase [wb-csrf-token dataset]
  (log/info "Writing supporting data (dimension values, predicates, etc.)")
  (writing/write-dataset-to-wikibase-items wb-csrf-token base/class-mapper class-dataset)
  (log/info "Writing core data")
  (writing/write-dataset-to-wikibase-items wb-csrf-token mapper dataset))

