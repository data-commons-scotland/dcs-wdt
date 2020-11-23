(ns dcs.wdt.dataset.area
  (:require [clojure.java.io :as io]
            [clojure.data.csv :as csv]
            [taoensso.timbre :as log]
            [dcs.wdt.misc :as misc]
            [dcs.wdt.wikibase-sparql :as wb-sparql]
            [dcs.wdt.writing :as writing]
            [dcs.wdt.dataset.base :as base :refer [for-time for-concept part-of]]))

(def for-area "for area")
(def has-uk-gov-code "has UK government code")

(def area-the-concept "area (concept)")

(defn- predicate-mapper [row]
  [(:label row)
   (:description row)
   (:datatype row)
   [[(wb-sparql/pqid for-concept) (writing/datatype for-concept) (wb-sparql/pqid area-the-concept)]]])

(defn- mapper [row]
  [(:label row)
   "a Scottish council area"
   [[(wb-sparql/pqid has-uk-gov-code) (writing/datatype has-uk-gov-code) (:ukGovCode row)]
    ;TODO [(wb-sparql/pqid part-of) (writing/datatype part-of) (wb-sparql/pqid "Scotland")]
    [(wb-sparql/pqid for-concept) (writing/datatype for-concept) (wb-sparql/pqid area-the-concept)]]])
   
(def concept-dataset 
  [{:label area-the-concept :description "area, the concept"}])

(def predicate-dataset
  [{:label for-area :description "the area of this" :datatype "wikibase-item"}
   {:label has-uk-gov-code :description "has the nine-character UK Government Statistical Service code" :datatype "external-id"}])

(defn dataset []
  (->> "area-dataset.sparql"
    io/resource
    slurp
    (misc/exec-sparql "http://statistics.gov.scot/sparql")
    (misc/patch :label)))

(defn write-to-wikibase [wb-csrf-token dataset]
  (log/info "Writing supporting data (dimension values, predicates, etc.)")
  (writing/write-dataset-to-wikibase-items wb-csrf-token base/concept-mapper concept-dataset)
  (writing/write-dataset-to-wikibase-predicates wb-csrf-token predicate-mapper predicate-dataset)
  (log/info "Writing essence data")
  (writing/write-dataset-to-wikibase-items wb-csrf-token mapper dataset))
