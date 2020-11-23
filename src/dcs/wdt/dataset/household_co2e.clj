(ns dcs.wdt.dataset.household-co2e
  (:require [clojure.java.io :as io]
            [clojure.data.csv :as csv]
            [dcs.wdt.misc :as misc]
            [dcs.wdt.wikibase-sparql :as wb-sparql]
            [dcs.wdt.writing :as writing]
            [dcs.wdt.dataset.base :refer [HAS_QUANTITY FOR_TIME FOR_CONCEPT]]
            [dcs.wdt.dataset.area :refer [FOR_AREA]]))

(def CARBON_EQUIVALENT_THE_CONCEPT "carbon equivalent (concept)")

(defn- value-mapper [row]
  [(str "carbon equivalent " (:council row) " " (:year row))
   (str "the CO2e emitted from " (:council row) " household waste in " (:year row))
   [(let [p HAS_QUANTITY] [(wb-sparql/pqid p) (writing/datatype p) (:TCO2e row)])
    (let [p FOR_AREA] [(wb-sparql/pqid p) (writing/datatype p) (wb-sparql/pqid (:council row))])
    (let [p FOR_TIME] [(wb-sparql/pqid p) (writing/datatype p) (:year row)])
    (let [p FOR_CONCEPT] [(wb-sparql/pqid p) (writing/datatype p) (wb-sparql/pqid CARBON_EQUIVALENT_THE_CONCEPT)])]])

(defn dataset []
  (with-open [reader (io/reader (io/resource "household-co2e-dataset.csv"))]
    (doall
      (->> reader
        csv/read-csv
        misc/to-maps
        (misc/patch :council)))))

(defn write-to-wikibase [wb-csrf-token dataset]
  (writing/write-dataset-to-wikibase-items wb-csrf-token [CARBON_EQUIVALENT_THE_CONCEPT "carbon equivalent, the concept" []] [:placeholder-row])
  (writing/write-dataset-to-wikibase-items wb-csrf-token value-mapper dataset))
