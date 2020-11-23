(ns dcs.wdt.writing
  (:require [dcs.wdt.wikibase-api :as wbi]
            [dcs.wdt.wikibase-sparql :as wbq]))
  

(defn write-dataset-to-wikibase-items [csrf-token mapper dataset] ; dataset should be a list of uniform maps
  (let [number-of-rows (count dataset)]
    (doseq [[ix row] (map-indexed vector dataset)] ; remember that a row is really a map
      (println "Dataset row:" (inc ix) "of" number-of-rows)
      (let [[label description threes] (mapper row)]
        (println "Writing item:" label)
        (println "Detail:" label "|" description "|" threes)
        (if-let [qid (wbq/pqid label)]
          (println "Item:" qid "[unmodified]")
          ;(println "Item:" (wbi/overwrite-item csrf-token qid label description threes) "[modified]")
          (println "Item:" (wbi/create-item csrf-token label description threes) "[new]"))))))
          

(defn write-dataset-to-wikibase-predicates [csrf-token mapper dataset] ; dataset should be a list of uniform maps
  (let [number-of-rows (count dataset)]
    (doseq [[ix row] (map-indexed vector dataset)] ; remember that a row is really a map
      (println "Predicate row:" (inc ix) "of" number-of-rows)
      (let [[label description datatype threes] (mapper row)]
        (println "Writing property:" label)
        ;(println "Detail:" label "|" description "|" datatype "|" threes)
        (if-let [pid (wbq/pqid label)]
          (println "Property:" pid "[unmodified]")
          ;(println "Property:" (wbi/overwrite-property csrf-token pid label description datatype []) "[modified]")
          (println "Property:" (wbi/create-property csrf-token label description datatype threes) "[new]"))))))

(defn datatype [label]
  (-> label
    wbq/pqid
    wbi/datatype))