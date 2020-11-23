(ns dcs.wdt.dataset.household-waste
  (:require [clojure.java.io :as io]
            [clojure.data.csv :as csv]
            [taoensso.timbre :as log]
            [dcs.wdt.misc :as misc]
            [dcs.wdt.wikibase-sparql :as wb-sparql]
            [dcs.wdt.writing :as writing]
            [dcs.wdt.dataset.base :refer [HAS_QUANTITY FOR_TIME FOR_CONCEPT]]
            [dcs.wdt.dataset.area :refer [FOR_AREA]]))

(def FOR_END_STATE "for end-state")
#_(def FOR_MATERIAL "for material")

(def HOUSEHOLD_WASTE_THE_CONCEPT "household waste (concept)")

(defn- concept-mapper [row]
  [HOUSEHOLD_WASTE_THE_CONCEPT
   "household waste, the concept" 
   []])

(defn- end-state-mapper [row]
  [(:endState row)
   "a household waste end-state"
   [(let [p "for concept"] [(wb-sparql/pqid p) (writing/datatype p) (wb-sparql/pqid HOUSEHOLD_WASTE_THE_CONCEPT)])]])

#_(defn- material-mapper [row]
  [(:material row)
   "a waste generated material"
   [(let [p "for concept"] [(wb-sparql/pqid p) (writing/datatype p) (wb-sparql/pqid "household waste (concept)")])]])

(defn- essence-mapper [row]
  [(str "household waste " (:area row) " " (:year row)  " " (:endState row) #_" " #_(:material row))
   (str "the household waste in " (:area row) " in " (:year row) " ending up " (:endState row) #_" comprised of " #_(:material row))
   [(let [p HAS_QUANTITY] [(wb-sparql/pqid p) (writing/datatype p) (:tonnes row)])
    (let [p FOR_AREA] [(wb-sparql/pqid p) (writing/datatype p) (wb-sparql/pqid (:area row))])
    (let [p FOR_TIME] [(wb-sparql/pqid p) (writing/datatype p) (:year row)])
    (let [p FOR_END_STATE] [(wb-sparql/pqid p) (writing/datatype p) (wb-sparql/pqid (:endState row))])
    #_(let [p FOR_MATERIAL] [(wb-sparql/pqid p) (writing/datatype p) (wb-sparql/pqid (:material row))])
    (let [p FOR_CONCEPT] [(wb-sparql/pqid p) (writing/datatype p) (wb-sparql/pqid HOUSEHOLD_WASTE_THE_CONCEPT)])]])

(defn dataset []
  (->> "household-waste-dataset.sparql"
    io/resource
    slurp
    (misc/exec-sparql "http://statistics.gov.scot/sparql")
    (misc/patch :area)))

(defn write-to-wikibase [wb-csrf-token dataset]
  (log/info "Writing supporting data (dimension values, predicates, etc.)")
  (writing/write-dataset-to-wikibase-items wb-csrf-token concept-mapper [:placeholder-row])
  (writing/write-dataset-to-wikibase-items wb-csrf-token end-state-mapper (distinct (map #(select-keys % [:endState]) dataset)))
  #_(writing/write-dataset-to-wikibase-items wb-csrf-token material-mapper (distinct (map #(select-keys % [:material]) dataset)))
  (log/info "Writing essence data")
  (writing/write-dataset-to-wikibase-items wb-csrf-token essence-mapper dataset))
