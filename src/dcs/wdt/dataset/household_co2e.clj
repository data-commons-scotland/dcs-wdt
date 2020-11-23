(ns dcs.wdt.dataset.household-co2e
  (:require [clojure.java.io :as io]
            [clojure.data.csv :as csv][taoensso.timbre :as log]
            [dcs.wdt.misc :as misc]
            [dcs.wdt.wikibase-sparql :as wb-sparql]
            [dcs.wdt.writing :as writing]
            [dcs.wdt.dataset.base :as base :refer [has-quantity for-time for-concept]]
            [dcs.wdt.dataset.area :refer [for-area]]))

(def carbon-equiv-the-concept "carbon equivalent (concept)")

(defn- mapper [row]
  [(str "carbon equivalent " (:council row) " " (:year row))
   (str "the CO2e emitted from " (:council row) " household waste in " (:year row))
   [[(wb-sparql/pqid has-quantity) (writing/datatype has-quantity) (:TCO2e row)]
    [(wb-sparql/pqid for-area) (writing/datatype for-area) (wb-sparql/pqid (:council row))]
    [(wb-sparql/pqid for-time) (writing/datatype for-time) (:year row)]
    [(wb-sparql/pqid for-concept) (writing/datatype for-concept) (wb-sparql/pqid carbon-equiv-the-concept)]]])

(def concept-dataset 
  [{:label carbon-equiv-the-concept :description "carbon equivalent, the concept"}])

(defn dataset []
  (with-open [reader (io/reader (io/resource "household-co2e-dataset.csv"))]
    (doall
      (->> reader
        csv/read-csv
        misc/to-maps
        (misc/patch :council)))))

(defn write-to-wikibase [wb-csrf-token dataset]
  (log/info "Writing supporting data (dimension values, predicates, etc.)")
  (writing/write-dataset-to-wikibase-items wb-csrf-token base/concept-mapper concept-dataset)
  (log/info "Writing essence data")(log/info "Writing essence data")
  (writing/write-dataset-to-wikibase-items wb-csrf-token mapper dataset))
