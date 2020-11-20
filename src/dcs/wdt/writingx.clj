(ns dcs.wdt.writingx
  (:require [dcs.wdt.predicate :as predicate]
            [dcs.wdt.wikibase-apix :as wb-apix]
            [dcs.wdt.wikibase-sparql :as wb-sparql]))
  

(defn write-dataset-to-wikibase [csrf-token mapper dataset] ; dataset should be a list of uniform maps
  (let [number-of-rows (count dataset)]
    (doseq [[ix row] (map-indexed vector dataset)] ; remember that a row is really a map
      (println "Dataset row:" (inc ix) "of" number-of-rows)
      (let [[label description threes] (mapper row)]
        (println "Writing item:" label)
        (if-let [qid (wb-sparql/pqid label)]
          (println "Item:" qid "[unmodified]")
          ;(println "Item:" (wb-apix/overwrite-item csrf-token qid label description threes) "[modified]")
          (println "Item:" (wb-apix/create-item csrf-token label description threes) "[new]"))))))
          

(defn write-predicates-to-wikibase [csrf-token] ; dataset should be a list of uniform maps
  (let [number-of-rows (count predicate/predicates)]
    (doseq [[ix row] (map-indexed vector predicate/predicates)] ; remember that a row is really a map
      (println "Predicate row:" (inc ix) "of" number-of-rows)
      (let [{:keys [label description datatype]} row]
        (println "Writing property:" label)
        (if-let [pid (wb-sparql/pqid label)]
          (println "Property:" pid "[unmodified]")
          ;(println "Property:" (wb-apix/overwrite-property csrf-token pid label description datatype []) "[modified]")
          (println "Property:" (wb-apix/create-property csrf-token label description datatype []) "[new]"))))))