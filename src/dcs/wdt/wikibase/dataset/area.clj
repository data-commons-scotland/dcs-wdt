(ns dcs.wdt.wikibase.dataset.area
  (:require [clojure.java.io :as io]
            [clojure.data.csv :as csv]
            [taoensso.timbre :as log]
            [dcs.wdt.wikibase.misc :as misc]
            [dcs.wdt.wikibase.wikibase-sparql :as wbq]
            [dcs.wdt.wikibase.writing :as writing]
            [dcs.wdt.wikibase.reading :as reading]
            [dcs.wdt.wikibase.dataset.base :as base :refer [for-time instance-of part-of]]))

(def for-area "for area")
(def has-uk-gov-code "has UK government code")

(def area-class "area (class)")

(defn- predicate-mapper [row]
  [(:label row)
   (:description row)
   (:datatype row)
   []])

(defn- scotland-mapper [row]
  [(:label row)
   "a UK country area"
   [[(wbq/pqid-s has-uk-gov-code) (reading/datatype has-uk-gov-code) (:ukGovCode row)]
    [(wbq/pqid-s instance-of) (reading/datatype instance-of) (wbq/pqid-s area-class)]]])

(defn- mapper [row]
  [(:label row)
   "a Scottish council area"
   [[(wbq/pqid-s has-uk-gov-code) (reading/datatype has-uk-gov-code) (:ukGovCode row)]
    [(wbq/pqid-s part-of) (reading/datatype part-of) (wbq/pqid-s "Scotland")]
    [(wbq/pqid-s instance-of) (reading/datatype instance-of) (wbq/pqid-s area-class)]]])
   
(def class-dataset
  [{:label area-class :description "area, the class"}])

(def predicate-dataset
  [{:label for-area :description "the area of this" :datatype "wikibase-item"}
   {:label has-uk-gov-code :description "has the nine-character UK Government Statistical Service code" :datatype "external-id"}])

(defn scotland-dataset [dataset]
  (filter #(= "Scotland" (:label %)) dataset))

(defn main-dataset [dataset]
  (filter #(not= "Scotland" (:label %)) dataset))

(defn dataset []
  (->> "dcs/wdt/wikibase/area-dataset.sparql"
    io/resource
    slurp
    (misc/exec-sparql "http://statistics.gov.scot/sparql")
    (misc/patch :label)))

(defn write-to-wikibase [wb-csrf-token dataset]
  (log/info "Writing supporting data (dimension values, predicates, etc.)")
  (writing/write-dataset-to-wikibase-items wb-csrf-token base/class-mapper class-dataset)
  (writing/write-dataset-to-wikibase-predicates wb-csrf-token predicate-mapper predicate-dataset)
  (log/info "Writing core data")
  (writing/write-dataset-to-wikibase-items wb-csrf-token scotland-mapper (scotland-dataset dataset))
  (writing/write-dataset-to-wikibase-items wb-csrf-token mapper (main-dataset dataset)))



