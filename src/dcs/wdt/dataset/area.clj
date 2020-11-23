(ns dcs.wdt.dataset.area
  (:require [clojure.java.io :as io]
            [clojure.data.csv :as csv]
            [taoensso.timbre :as log]
            [dcs.wdt.misc :as misc]
            [dcs.wdt.wikibase-sparql :as wb-sparql]
            [dcs.wdt.writing :as writing]
            [dcs.wdt.dataset.base :refer [FOR_TIME FOR_CONCEPT PART_OF]]))

(def FOR_AREA "for area")
(def HAS_UK_GOV_CODE "has UK government code")

(def AREA_THE_CONCEPT "area (concept)")

(defn- concept-mapper [row]
  [AREA_THE_CONCEPT
   "area, the concept" 
   []])

(defn- predicate-mapper [row]
  [(:label row)
   (:description row)
   (:datatype row)
   [[(let [p FOR_CONCEPT] [(wb-sparql/pqid p) (writing/datatype p) (wb-sparql/pqid AREA_THE_CONCEPT)])]]])

(defn- essence-mapper [row]
  [(:label row)
   "a Scottish council area"
   [(let [p HAS_UK_GOV_CODE] [(wb-sparql/pqid p) (writing/datatype p) (:ukGovCode row)])
    (let [p PART_OF] [(wb-sparql/pqid p) (writing/datatype p) (wb-sparql/pqid "Scotland")])
    (let [p FOR_CONCEPT] [(wb-sparql/pqid p) (writing/datatype p) (wb-sparql/pqid AREA_THE_CONCEPT)])]])
   
(def predicate-dataset
  [{:label FOR_AREA :description "the area of this" :datatype "wikibase-item"}
   {:label HAS_UK_GOV_CODE :description "has the nine-character UK Government Statistical Service code" :datatype "external-id"}])

(defn dataset []
  (->> "area-dataset.sparql"
    io/resource
    slurp
    (misc/exec-sparql "http://statistics.gov.scot/sparql")
    (misc/patch :label)))

(defn write-to-wikibase [wb-csrf-token dataset]
  (log/info "Writing supporting data (dimension values, predicates, etc.)")
  (writing/write-dataset-to-wikibase-items wb-csrf-token concept-mapper [:placeholder-row])
  (writing/write-dataset-to-wikibase-predicates wb-csrf-token predicate-mapper predicate-dataset)
  (log/info "Writing essence data")
  (writing/write-dataset-to-wikibase-items wb-csrf-token essence-mapper dataset))
