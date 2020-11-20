(ns dcs.wdt.writing
  (:require [dcs.wdt.predicate :as predicate]
            [dcs.wdt.wikibase-api :as wb-api]
            [dcs.wdt.wikibase-sparql :as wb-sparql]
            [dcs.wdt.scotgov-sparql :as sg-sparql]))
  


(defn write-predicates-to-wikibase [wb-csrf-token]
  (let [number-of-rows (count predicate/predicates)]
    (doseq [[ix row] (map-indexed vector predicate/predicates)] ; remember that a row is really a map
      (println "Predicate row:" (inc ix) "of" number-of-rows)
      (let [label (:label row)]
        (println "Writing property:" label)
        (let [[pid status] (if-let [pid (wb-sparql/pq-number label)]
                             [pid "already"]
                             [(wb-api/create-property wb-csrf-token label (:description row) (:datatype row)) "new"])]
          (println "Property:" pid (str "[" status "]")))))))