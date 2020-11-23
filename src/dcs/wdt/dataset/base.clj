(ns dcs.wdt.dataset.base
  (:require [clojure.java.io :as io]
            [clojure.data.csv :as csv]
            [taoensso.timbre :as log]
            [dcs.wdt.misc :as misc]
            [dcs.wdt.wikibase-sparql :as wb-sparql]
            [dcs.wdt.writing :as writing]))

(def has-quantity "has quantity")
(def for-time "for time")
(def for-concept "for concept")
(def part-of "part of")

; Used by other datasets 
(defn concept-mapper [row]
  [(:label row)
   (:description row)
   []])

(defn- mapper [row]
  [(:label row)
   (:description row)
   (:datatype row)
   []])   

(def dataset
  [{:label has-quantity :description "the quantity of this" :datatype "quantity"}
   {:label for-time :description "the year of this" :datatype "time"}
   {:label for-concept :description "the concept of this" :datatype "wikibase-item"}
   {:label part-of :description "the containment structure of this" :datatype "wikibase-item"}])

(defn write-to-wikibase [wb-csrf-token dataset]
 (log/info "Writing essence data")
 (writing/write-dataset-to-wikibase-predicates wb-csrf-token mapper dataset))