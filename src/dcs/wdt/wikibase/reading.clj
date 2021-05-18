(ns dcs.wdt.wikibase.reading
  (:require [dcs.wdt.wikibase.wikibase-api :as wbi]
            [dcs.wdt.wikibase.wikibase-sparql :as wbq]))

; The datatype of the predicate that has the given label.
(defn datatype [label]
  (-> label
      wbq/pqid-s
      wbi/datatype))