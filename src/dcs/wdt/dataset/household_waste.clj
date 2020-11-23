(ns dcs.wdt.dataset.household-waste
  (:require [clojure.java.io :as io]
            [clojure.data.csv :as csv]
            [taoensso.timbre :as log]
            [dcs.wdt.misc :as misc]
            [dcs.wdt.wikibase-sparql :as wb-sparql]
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
   [[(wb-sparql/pqid for-concept) (writing/datatype for-concept) (wb-sparql/pqid household-waste-the-concept)]]])

(defn- end-state-mapper [row]
  [(:endState row)
   "a household waste end-state"
   [[(wb-sparql/pqid for-concept) (writing/datatype for-concept) (wb-sparql/pqid household-waste-the-concept)]]])

#_(defn- material-mapper [row]
  [(:material row)
   "a waste generated material"
   [[(wb-sparql/pqid for-concept) (writing/datatype for-concept) (wb-sparql/pqid "household waste (concept)")]]])

(defn- mapper [row]
  [(str "household waste " (:area row) " " (:year row)  " " (:endState row) #_" " #_(:material row))
   (str "the household waste in " (:area row) " in " (:year row) " ending up " (:endState row) #_" comprised of " #_(:material row))
   [[(wb-sparql/pqid has-quantity) (writing/datatype has-quantity) (:tonnes row)]
    [(wb-sparql/pqid for-area) (writing/datatype for-area) (wb-sparql/pqid (:area row))]
    [(wb-sparql/pqid for-time) (writing/datatype for-time) (:year row)]
    [(wb-sparql/pqid for-end-state) (writing/datatype for-end-state) (wb-sparql/pqid (:endState row))]
    #_[(wb-sparql/pqid for-material) (writing/datatype for-material) (wb-sparql/pqid (:material row))]
    [(wb-sparql/pqid for-concept) (writing/datatype for-concept) (wb-sparql/pqid household-waste-the-concept)]]])

(def concept-dataset 
  [{:label household-waste-the-concept :description "household waste, the concept"}])

(def predicate-dataset
  [{:label for-end-state :description "the end-state of this" :datatype "wikibase-item"}])

(defn dataset []
  (->> "household-waste-dataset.sparql"
    io/resource
    slurp
    (misc/exec-sparql "http://statistics.gov.scot/sparql")
    (misc/patch :area)))

(defn end-state-dataset []
  (->> (dataset)
    (map #(select-keys % [:endState]))
    distinct))

#_(defn material-dataset []
  (->> (dataset)
    (map #(select-keys % [:material]))
    distinct))

(defn write-to-wikibase [wb-csrf-token dataset]
  (log/info "Writing supporting data (dimension values, predicates, etc.)")
  (writing/write-dataset-to-wikibase-items wb-csrf-token base/concept-mapper concept-dataset)
  (writing/write-dataset-to-wikibase-predicates wb-csrf-token predicate-mapper predicate-dataset)
  (writing/write-dataset-to-wikibase-items wb-csrf-token end-state-mapper end-state-dataset)
  #_(writing/write-dataset-to-wikibase-items wb-csrf-token material-mapper material-dataset)
  (log/info "Writing essence data")
  (writing/write-dataset-to-wikibase-items wb-csrf-token mapper dataset))
