(ns dcs.wdt.reading
  (:require [dcs.wdt.wikibase-api :as wbi]
            [dcs.wdt.wikibase-sparql :as wbq]))

; The datatype of the predicate that has the given label.
(defn datatype [label]
  (-> label
      wbq/pqid
      wbi/datatype))