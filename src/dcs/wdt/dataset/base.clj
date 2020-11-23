(ns dcs.wdt.dataset.base
  (:require [clojure.java.io :as io]
            [clojure.data.csv :as csv]
            [taoensso.timbre :as log]
            [dcs.wdt.misc :as misc]
            [dcs.wdt.wikibase-sparql :as wb-sparql]
            [dcs.wdt.writing :as writing]))

(def HAS_QUANTITY "has quantity")
(def FOR_TIME "for time")
(def FOR_CONCEPT "for concept")
(def PART_OF "part of")

(defn- essence-mapper [row]
  [(:label row)
   (:description row)
   (:datatype row)
   []])   

(def dataset
  [{:label HAS_QUANTITY :description "the quantity of this" :dataset "quantity"}
   {:label FOR_TIME :description "the year of this" :datatype "time"}
   {:label FOR_CONCEPT :description "the concept of this" :datatype "wikibase-item"}
   {:label PART_OF :description "the containment structure of this" :datatype "wikibase-item"}])

(defn write-to-wikibase [wb-csrf-token dataset]
 (log/info "Writing essence data")
 (writing/write-dataset-to-wikibase-predicates wb-csrf-token essence-mapper dataset))