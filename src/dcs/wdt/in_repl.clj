(ns dcs.wdt.in-repl
  (:require
    [taoensso.timbre :as log]
    [clojure.pprint :as pp]
    [dcs.wdt.misc :as misc]
    [dcs.wdt.wikibase-api :as wb-api]
    [dcs.wdt.wikibase-sparql :as wb-sparql]
    [dcs.wdt.scotgov-sparql :as sg-sparql]
    [dcs.wdt.sepa-file :as sepa-file]
    [clojure.tools.namespace.repl :refer [refresh]]))


(log/set-level! :info)


(println "Authenticating: as" (misc/envvar "WB_USERNAME"))
(def wb-csrf-token (wb-api/do-login-seq (misc/envvar "WB_USERNAME") (misc/envvar "WB_PASSWORD")))
(println "Token:" (if (some? wb-csrf-token) "yes" "no"))


(def predicates [; common classification or composition
                 {:label "for concept" :description "the concept of this" :datatype "wikibase-item"}
                 {:label "part of" :description "the containment structure of this" :datatype "wikibase-item"}
                 
                 ; common value, points to built-in data values
                 {:label "has quantity" :description "the quantity of this" :datatype "quantity"}
                 {:label "has UK government code" :description "has the nine-character UK Government Statistical Service code" :datatype "external-id"}
                 
                 ; common "dimension", points to built-in datatype values
                 {:label "for time" :description "the year of this" :datatype "time"}
                 
                 ; common "dimension", points item values
                 {:label "for area" :description "the area of this" :datatype "wikibase-item"}
                 
                 ; Household Waste "dimension", points to item values
                 {:label "for end-state" :description "the waste management end-state of this" :datatype "wikibase-item"}
                 {:label "for material" :description "the waste management material of this" :datatype "wikibase-item"}])

(defn write-predicates-to-wikibase []
  (let [number-of-rows (count predicates)]
    (doseq [[ix row] (map-indexed vector predicates)] ; remember that a row is really a map
      (println "Predicate row:" (inc ix) "of" number-of-rows)
      (let [label (:label row)]
        (println "Writing property:" label)
        (let [[pid status] (if-let [pid (wb-sparql/pq-number label)]
                             [pid "already"]
                             [(wb-api/create-property wb-csrf-token label (:description row) (:datatype row)) "new"])]
          (println "Property:" pid (str "[" status "]")))))))


(def areas-dataset (atom nil))
(def population-dataset (atom nil))
(def co2e-dataset (atom nil))

(defn load-datasets-from-upstream []
  
  (println "Loading: areas dataset from Scot Gov")
  (reset! areas-dataset (sg-sparql/areas))
  (println "Loaded:" (count @areas-dataset))
  
  (println "Loading: population dataset from Scot Gov")
  (reset! population-dataset (sg-sparql/populations))
  (println "Loaded:" (count @population-dataset))
  
  (println "Loading: C02e dataset from SEPA")
  (reset! co2e-dataset (sepa-file/co2es))
  (println "Loaded:" (count @co2e-dataset)))

(def concepts-dataset [{:label "population, the concept" :description "the concept: population"}
                       {:label "carbon equivalent, the concept" :description "the concept: carbon equivalent"}
                       {:label "waste generated, the concept" :description "the concept: waste generated"}])


(defn create-item-object-claim [wb-csrf-token item-qid predicate-pid object]
  (wb-api/create-item-object-claim wb-csrf-token item-qid predicate-pid (wb-sparql/pq-number object)))

(defn claim-creator-fn [predicate-label]
  (->> predicates
    (filter #(= predicate-label (:label %)))
    first ; shouldbe only 1 anyway
    :datatype
    (get {"wikibase-item" create-item-object-claim
          "quantity" wb-api/create-quantity-object-claim
          "external-id" wb-api/create-string-object-claim
          "time" wb-api/create-year-object-claim})))

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
                                                    claim-id ((claim-creator-fn predicate-label) wb-csrf-token item-qid predicate-pid object)]
                                                [claim-id "new"]))]
                (println "Claim:" claim-id (str "[" claim-status "]"))))))))))

(defn write-concepts-dataset-to-wikibase []
  (write-dataset-to-wikibase wb-csrf-token concepts-dataset-mapper concepts-dataset))

(defn write-areas-dataset-to-wikibase []
  (write-dataset-to-wikibase wb-csrf-token areas-dataset-mapper @areas-dataset))

(defn write-population-dataset-to-wikibase []
  (write-dataset-to-wikibase wb-csrf-token population-dataset-mapper @population-dataset))

(defn write-co2e-dataset-to-wikibase []
  (write-dataset-to-wikibase wb-csrf-token co2e-dataset-mapper @co2e-dataset))

;----------------------



(defn write-scotland-into-wikibase []
  (let [label "Scotland"]
		  (println (str label "... "))
    (try
      (if-let [q-number (wb-sparql/pq-number label)]
        (println "\t" q-number "[already]")
        (println "\t" (wb-api/create-item wb-csrf-token label "a UK country area")))
      (catch Throwable t 
        (println (.getMessage t))))))




