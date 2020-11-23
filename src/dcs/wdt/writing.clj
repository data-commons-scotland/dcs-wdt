(ns dcs.wdt.writing
  (:require [dcs.wdt.wikibase-api :as wb-api]
            [dcs.wdt.wikibase-sparql :as wb-sparql]))
  

(defn write-dataset-to-wikibase-items [csrf-token mapper dataset] ; dataset should be a list of uniform maps
  (let [number-of-rows (count dataset)]
    (doseq [[ix row] (map-indexed vector dataset)] ; remember that a row is really a map
      (println "Dataset row:" (inc ix) "of" number-of-rows)
      (let [[label description threes] (mapper row)]
        (println "Writing item:" label)
        (if-let [qid (wb-sparql/pqid label)]
          (println "Item:" qid "[unmodified]")
          ;(println "Item:" (wb-api/overwrite-item csrf-token qid label description threes) "[modified]")
          (println "Item:" (wb-api/create-item csrf-token label description threes) "[new]"))))))
          

(defn write-dataset-to-wikibase-predicates [csrf-token mapper dataset] ; dataset should be a list of uniform maps
  (let [number-of-rows (count dataset)]
    (doseq [[ix row] (map-indexed vector dataset)] ; remember that a row is really a map
      (println "Predicate row:" (inc ix) "of" number-of-rows)
      (let [[label description datatype threes] (mapper row)]
        (println "Writing property:" label)
        (if-let [pid (wb-sparql/pqid label)]
          (println "Property:" pid "[unmodified]")
          ;(println "Property:" (wb-api/overwrite-property csrf-token pid label description datatype []) "[modified]")
          (println "Property:" (wb-api/create-property csrf-token label description datatype threes) "[new]"))))))

(defn datatype [label]
  (-> label
    wb-sparql/pqid
    wb-api/datatype))