(ns dcs.wdt.reading
  (:require [dcs.wdt.wikibase-api :as wbi]
            [dcs.wdt.wikibase-sparql :as wbq]))

; The datatype of the predicate that has the given label.
(defn datatype [label]
  (-> label
      wbq/pqid
      wbi/datatype))

(defn count-instances [class-label]
  (wbq/count (format "select (count(?item) as ?count) { ?item wdt:%s wd:%s. }"
                     (wbq/pqid instance-of) (wbq/pqid class-label))))