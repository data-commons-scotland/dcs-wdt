(ns dcs.wdt.dataset.household-co2e
  (:require [clojure.java.io :as io]
            [clojure.data.csv :as csv]
            [taoensso.timbre :as log]
            [dcs.wdt.misc :as misc]
            [dcs.wdt.wikibase-sparql :as wbq]
            [dcs.wdt.writing :as writing]
            [dcs.wdt.dataset.base :as base :refer [has-quantity for-time instance-of]]
            [dcs.wdt.dataset.area :refer [for-area]]))

(def carbon-equiv-class "carbon equivalent (class)")

(defn- mapper [row]
  [(str "carbon equivalent " (:council row) " " (:year row))
   (str "the CO2e emitted from " (:council row) " household waste in " (:year row))
   [[(wbq/pqid has-quantity) (reading/datatype has-quantity) (:TCO2e row)]
    [(wbq/pqid for-area) (reading/datatype for-area) (wbq/pqid (:council row))]
    [(wbq/pqid for-time) (reading/datatype for-time) (:year row)]
    [(wbq/pqid instance-of) (reading/datatype instance-of) (wbq/pqid carbon-equiv-class)]]])

(def class-dataset
  [{:label carbon-equiv-class :description "carbon equivalent, the class"}])

(defn dataset []
  (with-open [reader (io/reader (io/resource "household-co2e-dataset.csv"))]
    (doall
      (->> reader
        csv/read-csv
        misc/to-maps
        (misc/patch :council)))))

(defn write-to-wikibase [wb-csrf-token dataset]
  (log/info "Writing supporting data (dimension values, predicates, etc.)")
  (writing/write-dataset-to-wikibase-items wb-csrf-token base/class-mapper class-dataset)
  (log/info "Writing core data")(log/info "Writing essence data")
  (writing/write-dataset-to-wikibase-items wb-csrf-token mapper dataset))
