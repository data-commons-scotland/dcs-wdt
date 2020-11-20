(ns dcs.wdt.writingx
  (:require [dcs.wdt.wikibase-apix :as wb-apix]
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
          

(comment """

example invocation args

                 "carbon equivalent North Lanarkshire 2018"
                 "the CO2e quantity from North Lanarkshire in 2018"
                 [["P19" "wikibase-item" "Q41"]
                  ["P107" "time" 2018]
                  ["P110" "quantity" 353976.8102]
                  ["P25" "wikibase-item" "Q218"]
                  ["P111" "wikibase-item" "Q222"]]
""")