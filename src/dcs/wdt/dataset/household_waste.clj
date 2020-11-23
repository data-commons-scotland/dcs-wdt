(ns dcs.wdt.dataset.household-waste
  (:require [clojure.java.io :as io]
            [clojure.data.csv :as csv]
            [taoensso.timbre :as log]
            [dcs.wdt.misc :as misc]
            [dcs.wdt.wikibase-sparql :as wbq]
            [dcs.wdt.writing :as writing]
            [dcs.wdt.dataset.base :as base :refer [has-quantity for-time for-concept]]
            [dcs.wdt.dataset.area :refer [for-area]]))

(def for-end-state "for end-state")
#_(def for-material "for material")

(def household-waste-the-concept "household waste (concept)")

(defn- predicate-mapper [row]
  [(:label row)
   (:description row)
   (:datatype row)
   [[(wbq/pqid for-concept) (writing/datatype for-concept) (wbq/pqid household-waste-the-concept)]]])

(defn- end-state-mapper [row]
  [(:endState row)
   "a household waste end-state"
   [[(wbq/pqid for-concept) (writing/datatype for-concept) (wbq/pqid household-waste-the-concept)]]])

#_(defn- material-mapper [row]
  [(:material row)
   "a waste generated material"
   [[(wbq/pqid for-concept) (writing/datatype for-concept) (wbq/pqid "household waste (concept)")]]])

(defn- mapper [row]
  [(str "household waste " (:area row) " " (:year row)  " " (:endState row) #_" " #_(:material row))
   (str "the household waste in " (:area row) " in " (:year row) " ending up " (:endState row) #_" comprised of " #_(:material row))
   [[(wbq/pqid has-quantity) (writing/datatype has-quantity) (:tonnes row)]
    [(wbq/pqid for-area) (writing/datatype for-area) (wbq/pqid (:area row))]
    [(wbq/pqid for-time) (writing/datatype for-time) (:year row)]
    [(wbq/pqid for-end-state) (writing/datatype for-end-state) (wbq/pqid (:endState row))]
    #_[(wbq/pqid for-material) (writing/datatype for-material) (wbq/pqid (:material row))]
    [(wbq/pqid for-concept) (writing/datatype for-concept) (wbq/pqid household-waste-the-concept)]]])

(def concept-dataset 
  [{:label household-waste-the-concept :description "household waste, the concept"}])

(def predicate-dataset
  [{:label for-end-state :description "the end-state of this" :datatype "wikibase-item"}])

(defn end-state-dataset [dataset]
  (->> dataset
    (map #(select-keys % [:endState]))
    distinct))

#_(defn material-dataset [dataset]
  (->> dataset
    (map #(select-keys % [:material]))
    distinct))

(defn dataset []
  (->> "household-waste-dataset.sparql"
    io/resource
    slurp
    (misc/exec-sparql "http://statistics.gov.scot/sparql")
    (misc/patch :area)))

(defn write-to-wikibase [wb-csrf-token dataset]
  (log/info "Writing supporting data (dimension values, predicates, etc.)")
  (writing/write-dataset-to-wikibase-items wb-csrf-token base/concept-mapper concept-dataset)
  (writing/write-dataset-to-wikibase-predicates wb-csrf-token predicate-mapper predicate-dataset)
  (writing/write-dataset-to-wikibase-items wb-csrf-token end-state-mapper (end-state-dataset dataset))
  #_(writing/write-dataset-to-wikibase-items wb-csrf-token material-mapper (material-dataset dataset))
  (log/info "Writing core data")
  (writing/write-dataset-to-wikibase-items wb-csrf-token mapper dataset))

(defn counts []
  {:concept-item (wbq/count (format "select (count(?item) as ?count) { ?item rdfs:label '%s'@en. }"
                                    household-waste-the-concept))
   :predicate-property -1
   :end-state-item -1
   ;:material-item -1
   :core-item (wbq/count (format "select (count(?item) as ?count) { ?item wdt:%s wd:%s; wdt:%s ?quantity. }"
                                 (wbq/pqid for-concept) (wbq/pqid household-waste-the-concept) 
                                 (wbq/pqid has-quantity)))})



