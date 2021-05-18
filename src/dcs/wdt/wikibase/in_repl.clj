(ns dcs.wdt.wikibase.in-repl
  (:require
    [clojure.pprint :refer [pprint print-table]]
    [clojure.tools.namespace.repl :refer [refresh]]
    [taoensso.timbre :as log]
    [taoensso.timbre.appenders.core :as log-appenders]
    [dcs.wdt.wikibase.writing :as writing]
    [dcs.wdt.wikibase.misc :as misc]
    [dcs.wdt.wikibase.wikibase-api :as wbi]
    [dcs.wdt.wikibase.wikibase-sparql :as wbq]
    [dcs.wdt.wikibase.reading :as reading]
    [dcs.wdt.wikibase.dataset.base :as base]
    [dcs.wdt.wikibase.dataset.area :as area]
    [dcs.wdt.wikibase.dataset.population :as population]
    [dcs.wdt.wikibase.dataset.household-waste :as household-waste]
    [dcs.wdt.wikibase.dataset.household-co2e :as household-co2e]))

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
    [{:class area/area-class :count-of-instances (base/count-instances area/area-class)}
     {:class population/population-class :count-of-instances (base/count-instances population/population-class)}
     {:class household-waste/end-state-class :count-of-instances (base/count-instances household-waste/end-state-class)}
     {:class household-waste/household-waste-class :count-of-instances (base/count-instances household-waste/household-waste-class)}
     {:class household-co2e/carbon-equiv-class :count-of-instances (base/count-instances household-co2e/carbon-equiv-class)}]))
