(ns dcs.wdt.mapping)


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


