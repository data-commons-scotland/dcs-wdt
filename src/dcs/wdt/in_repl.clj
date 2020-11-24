(ns dcs.wdt.in-repl
  (:require
    [clojure.pprint :refer [pprint print-table]]
    [clojure.tools.namespace.repl :refer [refresh]]
    [taoensso.timbre :as log]
    [taoensso.timbre.appenders.core :as log-appenders]
    [dcs.wdt.writing :as writing]
    [dcs.wdt.misc :as misc]
    [dcs.wdt.wikibase-api :as wbi]
    [dcs.wdt.wikibase-sparql :as wbq]
    [dcs.wdt.dataset.base :as base]
    [dcs.wdt.dataset.area :as area]
    [dcs.wdt.dataset.population :as population]
    [dcs.wdt.dataset.household-waste :as household-waste]
    [dcs.wdt.dataset.household-co2e :as household-co2e]))

(log/merge-config!
  {:appenders {:spit (log-appenders/spit-appender {:fname "log.txt"})}})

(log/set-level! :info)


(println "Authenticating: as" (misc/envvar "WB_USERNAME"))
(def wb-csrf-token (wbi/do-login-seq (misc/envvar "WB_USERNAME") (misc/envvar "WB_PASSWORD")))
(println "Token:" (if (some? wb-csrf-token) "yes" "no"))


(defn write-base-dataset-to-wikibase []
  (base/write-to-wikibase wb-csrf-token base/dataset))

(defn write-area-dataset-to-wikibase []
  (area/write-to-wikibase wb-csrf-token (area/dataset)))

(defn write-population-dataset-to-wikibase []
  (population/write-to-wikibase wb-csrf-token (population/dataset)))

(defn write-household-waste-dataset-to-wikibase []
  (household-waste/write-to-wikibase wb-csrf-token (household-waste/dataset)))

(defn write-household-co2e-dataset-to-wikibase []
  (household-co2e/write-to-wikibase wb-csrf-token (household-co2e/dataset)))


(defn counts-in-wikibase[]
  (print-table
    [:dataset :concept-item :predicate-property :end-state-item :core-item ]
    [(assoc (base/count-in-wikibase) :dataset "base")
     (assoc (area/count-in-wikibase) :dataset "area")
     (assoc (population/count-in-wikibase) :dataset "population")
     (assoc (household-waste/count-in-wikibase) :dataset "household waste")
     (assoc (household-co2e/count-in-wikibase) :dataset "household co2e")]))
