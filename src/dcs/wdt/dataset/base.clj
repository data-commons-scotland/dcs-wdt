(ns dcs.wdt.dataset.base
  (:require [clojure.java.io :as io]
            [clojure.data.csv :as csv]
            [taoensso.timbre :as log]
            [dcs.wdt.misc :as misc]
            [dcs.wdt.wikibase-sparql :as wbq]
            [dcs.wdt.writing :as writing]))

(def has-quantity "has quantity")
(def for-time "for time")
(def instance-of "instance of")
(def part-of "part of")
#_(def has-source "has source")
#_(def has-author "has author")
#_(def when-authored "when authored")
#_(def has-publisher "has publisher")
#_(def when-published "when published")
#_(def has-publication-location "has publication location")

; Used by other datasets 
(defn class-mapper [row]
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
   {:label instance-of :description "the classification of this" :datatype "wikibase-item"}
   {:label part-of :description "the containment structure of this" :datatype "wikibase-item"}
   #_{:label has-source :description "the source of the dataset of this" :datatype "wikibase-item"}
   #_{:label has-author :description "who authored it" :datatype "wikibase-item"}
   #_{:label when-authored :description "when it was authored" :datatype "time"}
   #_{:label has-publisher :description "who published it" :datatype "wikibase-item"}
   #_{:label when-published :description "when it was published" :datatype "time"}
   #_{:label has-publication-location :description "e.g. a URL or filename" :datatype "string"}])

(defn write-to-wikibase [wb-csrf-token dataset]
 (log/info "Writing core data")
 (writing/write-dataset-to-wikibase-predicates wb-csrf-token mapper dataset))


(defn count-instances [class-label]
  (wbq/count (format "%sselect (count(?item) as ?count) { ?item wdt:%s wd:%s . }"
                     wbq/prefixes
                     (wbq/pqid instance-of) 
                     (wbq/pqid class-label))))