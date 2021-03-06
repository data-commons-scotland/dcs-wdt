(ns dcs.wdt.wikibase.dataset.household-waste
  (:require [clojure.java.io :as io]
            [clojure.data.csv :as csv]
            [taoensso.timbre :as log]
            [dcs.wdt.wikibase.misc :as misc]
            [dcs.wdt.wikibase.wikibase-sparql :as wbq]
            [dcs.wdt.wikibase.writing :as writing]
            [dcs.wdt.wikibase.reading :as reading]
            [dcs.wdt.wikibase.dataset.base :as base :refer [has-quantity for-time instance-of]]
            [dcs.wdt.wikibase.dataset.area :refer [for-area]]))

(def for-end-state "for end-state")
#_(def for-material "for material")

(def household-waste-class "household waste (class)")
(def end-state-class "end-state (class)")
;(def material-class "material (class)")

(defn- predicate-mapper [row]
  [(:label row)
   (:description row)
   (:datatype row)
   []])

(defn- end-state-mapper [row]
  [(:endState row)
   "a household waste end-state"
   [[(wbq/pqid-s instance-of) (reading/datatype instance-of) (wbq/pqid-s end-state-class)]]])

#_(defn- material-mapper [row]
  [(:material row)
   "a waste generated material"
   [[(wbq/pqid instance-of) (writing/datatype instance-of) (wbq/pqid "household waste (concept)")]]])

(defn- mapper [row]
  [(str "household waste " (:area row) " " (:year row)  " " (:endState row) #_" " #_(:material row))
   (str "the household waste in " (:area row) " in " (:year row) " ending up " (:endState row) #_" comprised of " #_(:material row))
   [[(wbq/pqid-s has-quantity) (reading/datatype has-quantity) (:tonnes row)]
    [(wbq/pqid-s for-area) (reading/datatype for-area) (wbq/pqid-s (:area row))]
    [(wbq/pqid-s for-time) (reading/datatype for-time) (:year row)]
    [(wbq/pqid-s for-end-state) (reading/datatype for-end-state) (wbq/pqid-s (:endState row))]
    #_[(wbq/pqid for-material) (reading/datatype for-material) (wbq/pqid-s (:material row))]
    [(wbq/pqid-s instance-of) (reading/datatype instance-of) (wbq/pqid-s household-waste-class)]]])

(def class-dataset
  [{:label household-waste-class :description "household waste, the class"}
   {:label end-state-class :description "end-state, the class"}
   #_{:label material-class :description "material, the class"}])

(def predicate-dataset
  [{:label for-end-state :description "the end-state of this" :datatype "wikibase-item"}
   #_{:label for-material :description "the material of this" :datatype "wikibase-item"}])

(defn end-state-dataset [dataset]
  (->> dataset
    (map #(select-keys % [:endState]))
    distinct))

#_(defn material-dataset [dataset]
  (->> dataset
    (map #(select-keys % [:material]))
    distinct))

(defn dataset []
  (->> "dcs/wdt/wikibase/household-waste-dataset.sparql"
    io/resource
    slurp
    (misc/exec-sparql "http://statistics.gov.scot/sparql")
    (misc/patch :area)))

(defn write-to-wikibase [wb-csrf-token dataset]
  (log/info "Writing supporting data (dimension values, predicates, etc.)")
  (writing/write-dataset-to-wikibase-items wb-csrf-token base/class-mapper class-dataset)
  (writing/write-dataset-to-wikibase-predicates wb-csrf-token predicate-mapper predicate-dataset)
  (writing/write-dataset-to-wikibase-items wb-csrf-token end-state-mapper (end-state-dataset dataset))
  #_(writing/write-dataset-to-wikibase-items wb-csrf-token material-mapper (material-dataset dataset))
  (log/info "Writing core data")
  (writing/write-dataset-to-wikibase-items wb-csrf-token mapper dataset))






