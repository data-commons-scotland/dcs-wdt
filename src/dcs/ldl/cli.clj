(ns dcs.ldl.cli
  (:require
    [taoensso.timbre :as log]
    [clojure.pprint :as pp]
    [dcs.ldl.procedures :as procedures]))

(defn -main []
  (log/set-level! :info)
  ;(procedures/create-scotland-in-wikibase)
  ;(procedures/create-areas-in-wikibase)
  ;(procedures/create-part-of-statements-in-wikibase)
  (procedures/create-properties-in-wikibase)
  )

