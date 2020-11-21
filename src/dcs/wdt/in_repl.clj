(ns dcs.wdt.in-repl
  (:require
    [taoensso.timbre :as log]
    [clojure.pprint :refer [pprint print-table]]
    [dcs.wdt.predicate :as predicate]
    [dcs.wdt.concept :as concept]
    [dcs.wdt.mappingx :as mapping]
    [dcs.wdt.writingx :as writing]
    [dcs.wdt.misc :as misc]
    [dcs.wdt.wikibase-apix :as wb-api]
    [dcs.wdt.scotgov-sparql :as sg-sparql]
    [dcs.wdt.sepa-file :as sepa-file]
    [clojure.tools.namespace.repl :refer [refresh]]))


(log/set-level! :info)


(println "Authenticating: as" (misc/envvar "WB_USERNAME"))
(def wb-csrf-token (wb-api/do-login-seq (misc/envvar "WB_USERNAME") (misc/envvar "WB_PASSWORD")))
(println "Token:" (if (some? wb-csrf-token) "yes" "no"))


(def areas-dataset (atom nil))
(def population-dataset (atom nil))
(def waste-generated-dataset (atom nil))
(def co2e-dataset (atom nil))

(defn load-datasets-from-upstream []
  
  (println "Loading: areas dataset from Scot Gov")
  (reset! areas-dataset (sg-sparql/areas))
  (println "Loaded:" (count @areas-dataset))
  
  (println "Loading: population dataset from Scot Gov")
  (reset! population-dataset (sg-sparql/populations))
  (println "Loaded:" (count @population-dataset))
  
  (println "Loading: waste-generated dataset from Scot Gov")
  (reset! waste-generated-dataset (sg-sparql/waste-generated))
  (println "Loaded:" (count @waste-generated-dataset))
  
  (println "Loading: C02e dataset from SEPA")
  (reset! co2e-dataset (sepa-file/co2es))
  (println "Loaded:" (count @co2e-dataset)))


(defn write-predicates-to-wikibase []
  (writing/write-predicates-to-wikibase wb-csrf-token))

(defn write-concepts-dataset-to-wikibase []
  (writing/write-dataset-to-wikibase wb-csrf-token mapping/concepts-dataset-mapper concept/concepts-dataset))

(defn write-areas-dataset-to-wikibase []
  (writing/write-dataset-to-wikibase wb-csrf-token mapping/areas-dataset-mapper @areas-dataset))

(defn write-population-dataset-to-wikibase []
  (writing/write-dataset-to-wikibase wb-csrf-token mapping/population-dataset-mapper @population-dataset))

(defn write-waste-generated-dataset-to-wikibase []
  (writing/write-dataset-to-wikibase wb-csrf-token mapping/end-states-dataset-mapper (distinct (map #(select-keys % [:endState]) @waste-generated-dataset)))
  (writing/write-dataset-to-wikibase wb-csrf-token mapping/materials-dataset-mapper (distinct (map #(select-keys % [:material]) @waste-generated-dataset)))
  (writing/write-dataset-to-wikibase wb-csrf-token mapping/waste-generated-dataset-mapper @waste-generated-dataset))

(defn write-co2e-dataset-to-wikibase []
  (writing/write-dataset-to-wikibase wb-csrf-token mapping/co2e-dataset-mapper @co2e-dataset))

(comment """

write-x-dataset-to-wikibase   ...really, write a dataset with the shape of x 

write-dataset-x-layer-n-to-wikibase  ...where n will probably be:
                                            0   given predicates and items
                                            1.m source item
                                            2   predicates
                                            3.m supporting dimensions
                                            4   the actual 'quantity' items




""")




