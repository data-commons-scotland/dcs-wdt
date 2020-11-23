(ns dcs.wdt.in-repl
  (:require
    [clojure.pprint :refer [pprint print-table]]
    [clojure.tools.namespace.repl :refer [refresh]]
    [taoensso.timbre :as log]
    [taoensso.timbre.appenders.core :as log-appenders]
    [dcs.wdt.writing :as writing]
    [dcs.wdt.misc :as misc]
    [dcs.wdt.wikibase-api :as wb-api]
    [dcs.wdt.wikibase-sparql :as wb-sparql]
    [dcs.wdt.dataset.base :as base]
    [dcs.wdt.dataset.area :as area]
    [dcs.wdt.dataset.population :as population]
    [dcs.wdt.dataset.household-waste :as household-waste]
    [dcs.wdt.dataset.household-co2e :as household-co2e]))

(log/merge-config!
  {:appenders {:spit (log-appenders/spit-appender {:fname "log.txt"})}})

(log/set-level! :info)


(println "Authenticating: as" (misc/envvar "WB_USERNAME"))
(def wb-csrf-token (wb-api/do-login-seq (misc/envvar "WB_USERNAME") (misc/envvar "WB_PASSWORD")))
(println "Token:" (if (some? wb-csrf-token) "yes" "no"))


(defn write-base-dataset-to-wikibase []
  (base/write-to-wikibase wb-csrf-token base/dataset))

(defn write-area-dataset-to-wikibase []
  (area/write-to-wikibase wb-csrf-token (area/dataset)))

(defn write-population-dataset-to-wikibase []
  (population/write-to-wikibase wb-csrf-token (population/dataset)))

(defn write-household-waste-dataset-to-wikibase []
  (household-waste/write-to-wikibase wb-csrf-token (household-waste/dataset)))

(defn write-co2e-dataset-to-wikibase []
  (household-co2e/write-to-wikibase wb-csrf-token (household-co2e/dataset)))

(comment """

write-x-dataset-to-wikibase   ...really, write a dataset with the shape of x 

write-dataset-x-layer-n-to-wikibase  ...where n will probably be:
                                            0   given predicates and items
                                            1.m source item
                                            2   predicates
                                            3.m supporting dimensions
                                            4   the actual 'quantity' items

wdt.
    in-repl
    wb-api
    wb-sparql
    writing
    misc
    dataset.
            foundation
            area
            population
            household-solids
            household-co2e






""")




