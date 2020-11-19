(ns dcs.wdt.dataset
  (:require [dcs.wdt.predicate :as predicate]
            [dcs.wdt.wikibase-api :as wb-api]
            [dcs.wdt.wikibase-sparql :as wb-sparql]
            [dcs.wdt.scotgov-sparql :as sg-sparql]))
  
  
(def concepts-dataset [{:label "population, the concept" :description "the concept: population"}
                       {:label "carbon equivalent, the concept" :description "the concept: carbon equivalent"}
                       {:label "waste generated, the concept" :description "the concept: waste generated"}])


(def concepts-dataset-mapper
  {:item-label-fn (fn [row] (:label row))
   :item-description-fn (fn [row] (:description row))
   :claim-mappers []})

(def areas-dataset-mapper
  {:item-label-fn (fn [row] (:label row))
   :item-description-fn (fn [row] "a Scottish council area")
   :claim-mappers [{:predicate-label "has UK government code" :object-fn (fn [row] (:ukGovCode row))}
                   {:predicate-label "part of" :object-fn (fn [_] "Scotland")}]})

(def population-dataset-mapper
  {:item-label-fn (fn [row] (str "population " (:areaLabel row) " " (:year row)))
   :item-description-fn (fn [row] (str "the population of " (:areaLabel row) " in " (:year row)))
   :claim-mappers [{:predicate-label "has quantity" :object-fn (fn [row] (:quantity row))}
                   {:predicate-label "for area" :object-fn (fn [row] (:areaLabel row))}
                   {:predicate-label "for time" :object-fn (fn [row] (:year row))}
                   {:predicate-label "for concept" :object-fn (fn [_] "population, the concept")}]})

(def end-states-dataset-mapper
  {:item-label-fn (fn [row] (:endState row))
   :item-description-fn (fn [row] (str "a waste generated end-state"))
   :claim-mappers [{:predicate-label "for concept" :object-fn (fn [_] "waste generated, the concept")}]})

(def materials-dataset-mapper
  {:item-label-fn (fn [row] (:material row))
   :item-description-fn (fn [row] (str "a waste generated material"))
   :claim-mappers [{:predicate-label "for concept" :object-fn (fn [_] "waste generated, the concept")}]})

(def waste-generated-dataset-mapper
  {:item-label-fn (fn [row] (str "waste generated " (:area row) " " (:year row) " " (:endState row) " " (:material row)))
   :item-description-fn (fn [row] (str "the waste generated in " (:area row) " in " (:year row) " ending up as " (:endState row) " comprised of " (:material row)))
   :claim-mappers [{:predicate-label "has quantity" :object-fn (fn [row] (:tonnes row))}
                   {:predicate-label "for area" :object-fn (fn [row] (:area row))}
                   {:predicate-label "for time" :object-fn (fn [row] (:year row))}
                   {:predicate-label "for end-state" :object-fn (fn [row] (:endState row))}
                   {:predicate-label "for material" :object-fn (fn [row] (:material row))}
                   {:predicate-label "for concept" :object-fn (fn [_] "waste generated, the concept")}]})

(def co2e-dataset-mapper
  {:item-label-fn (fn [row] (str "carbon equivalent " (:council row) " " (:year row)))
   :item-description-fn (fn [row] (str "the CO2e emitted from " (:council row) " household waste in " (:year row)))
   :claim-mappers [{:predicate-label "has quantity" :object-fn (fn [row] (:TCO2e row))}
                   {:predicate-label "for area" :object-fn (fn [row] (:council row))}
                   {:predicate-label "for time" :object-fn (fn [row] (:year row))}
                   {:predicate-label "for concept" :object-fn (fn [_] "carbon equivalent, the concept")}]})


(defn write-dataset-to-wikibase [wb-csrf-token mapper dataset] ; dataset should be a list of uniform maps
  (let [number-of-rows (count dataset)]
    (doseq [[ix row] (map-indexed vector dataset)]          ; remember that a row is really a map
      (println "Dataset row:" (inc ix) "of" number-of-rows)
      (let [item-label ((:item-label-fn mapper) row)]
        (println "Writing item:" item-label)
        (let [[item-qid item-status] (if-let [item-qid (wb-sparql/pq-number item-label)]
                                       [item-qid "already"]
                                       (let [item-description ((:item-description-fn mapper) row)
                                             item-qid (wb-api/create-item wb-csrf-token item-label item-description)]
                                         [item-qid "new"]))]
          (println "Item:" item-qid (str "[" item-status "]"))
          (doseq [claim-mapper (:claim-mappers mapper)]
            (let [predicate-label (:predicate-label claim-mapper)
                  object ((:object-fn claim-mapper) row)]
              (println "Writing claim:" item-label predicate-label object)
              (let [[claim-id claim-status] (if-let [claim-id (wb-sparql/claim-id item-label predicate-label)]
                                              [claim-id "already"]
                                              (let [predicate-pid (wb-sparql/pq-number predicate-label)
                                                    claim-id ((predicate/claim-creator-fn predicate-label) wb-csrf-token item-qid predicate-pid object)]
                                                [claim-id "new"]))]
                (println "Claim:" claim-id (str "[" claim-status "]"))))))))))