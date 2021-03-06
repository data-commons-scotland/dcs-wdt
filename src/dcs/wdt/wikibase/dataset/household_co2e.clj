(ns dcs.wdt.wikibase.dataset.household-co2e
  (:require [clojure.java.io :as io]
            [clojure.data.csv :as csv]
            [taoensso.timbre :as log]
            [dcs.wdt.wikibase.misc :as misc]
            [dcs.wdt.wikibase.wikibase-sparql :as wbq]
            [dcs.wdt.wikibase.writing :as writing]
            [dcs.wdt.wikibase.reading :as reading]
            [dcs.wdt.wikibase.dataset.base :as base :refer [has-quantity for-time instance-of]]
            [dcs.wdt.wikibase.dataset.area :refer [for-area]]))

(def carbon-equiv-class "carbon equivalent (class)")

(defn- mapper [row]
  [(str "carbon equivalent " (:council row) " " (:year row))
   (str "the CO2e emitted from " (:council row) " household waste in " (:year row))
   [[(wbq/pqid-s has-quantity) (reading/datatype has-quantity) (:TCO2e row)]
    [(wbq/pqid-s for-area) (reading/datatype for-area) (wbq/pqid-s (:council row))]
    [(wbq/pqid-s for-time) (reading/datatype for-time) (:year row)]
    [(wbq/pqid-s instance-of) (reading/datatype instance-of) (wbq/pqid-s carbon-equiv-class)]]])

(def class-dataset
  [{:label carbon-equiv-class :description "carbon equivalent, the class"}])

(defn dataset []
  (with-open [reader (io/reader (io/resource "dcs/wdt/wikibase/household-co2e-dataset.csv"))]
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
