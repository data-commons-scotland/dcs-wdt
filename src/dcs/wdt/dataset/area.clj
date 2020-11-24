(ns dcs.wdt.dataset.area
  (:require [clojure.java.io :as io]
            [clojure.data.csv :as csv]
            [taoensso.timbre :as log]
            [dcs.wdt.misc :as misc]
            [dcs.wdt.wikibase-sparql :as wbq]
            [dcs.wdt.writing :as writing]
            [dcs.wdt.dataset.base :as base :refer [for-time instance-of part-of]]))

(def for-area "for area")
(def has-uk-gov-code "has UK government code")

(def area-class "area (class)")

(defn- predicate-mapper [row]
  [(:label row)
   (:description row)
   (:datatype row)
   []])

(defn- mapper [row]
  [(:label row)
   "a Scottish council area"
   [[(wbq/pqid has-uk-gov-code) (reading/datatype has-uk-gov-code) (:ukGovCode row)]
    ;TODO [(wbq/pqid part-of) (reading/datatype part-of) (wbq/pqid "Scotland")]
    [(wbq/pqid instance-of) (reading/datatype instance-of) (wbq/pqid area-class)]]])
   
(def class-dataset
  [{:label area-class :description "area, the class"}])

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
  (writing/write-dataset-to-wikibase-items wb-csrf-token base/class-mapper class-dataset)
  (writing/write-dataset-to-wikibase-predicates wb-csrf-token predicate-mapper predicate-dataset)
  (log/info "Writing core data")
  (writing/write-dataset-to-wikibase-items wb-csrf-token mapper dataset))



