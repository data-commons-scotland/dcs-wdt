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


(def sg-areas (atom nil))
(def sg-population-dataset (atom nil))
(def sepa-co2es (atom nil))

(defn load-datasets-from-upstream []
  
  (println "Loading: Scot Gov areas dataset")
  (reset! sg-areas (sg-sparql/areas))
  (println "Loaded:" (count @sg-areas))
  
  (println "Loading: Scot Gov population dataset")
  (reset! sg-population-dataset (sg-sparql/populations))
  (println "Loaded:" (count @sg-population-dataset))
  
  
  (println "Loading: SEPA CO2e dataset")
  (reset! sepa-co2es (sepa-file/co2es))
  (println "Loaded:" (count @sepa-co2es)))


(def predicates [; common classification or composition
                 ["for concept" "the concept of this" "wikibase-item"]
                 ["part of" "the containment structure of this" "wikibase-item"]
                 
                 ; common value, points to built-in data values
                 ["has quantity" "the quantity of this" "quantity"]
                 ["has UK government code" "has the nine-character UK Government Statistical Service code" "external-id"]
                 
                 ; common "dimension", points to built-in datatype values
                 ["for time" "the year of this" "time"]
                 
                 ; common "dimension", points item values
                 ["for area" "the area of this" "wikibase-item"]
                 
                 ; Household Waste "dimension", points to item values
                 ["for end-state" "the waste management end-state of this" "wikibase-item"]
                 ["for material" "the waste management material of this" "wikibase-item"]])

(defn write-predicates-into-wikibase []
  (doseq [[label description datatype] predicates]
    (println "Writing:" label)
    (if-let [p-number (wb-sparql/pq-number label)]
      (println "Property:" p-number "[already]")
      (println "Property:" (wb-api/create-property wb-csrf-token label description datatype) "[new]"))))


(def concepts [["population, the concept" "the concept: population"]
               ["carbon equivalent, the concept" "the concept: carbon equivalent"]
               ["waste generated, the concept" "the concept: waste generated"]])

(defn write-concept-items-into-wikibase []
  (doseq [[label description] concepts]
      (println "Writing:" label)
      (if-let [q-number (wb-sparql/pq-number label)]
        (println "Item:" q-number "[already]")
        (println "Item:" (wb-api/create-item wb-csrf-token label description) "[new]"))))

(defn write-area-items-in-wikibase []
  (doseq [area @sg-areas]
    (let [label (:label area)]
      (println "Writing:" label)
      (if-let [q-number (wb-sparql/pq-number label)]
        (println "Item:" q-number "[already]")
        (println "Item:" (wb-api/create-item wb-csrf-token label "a Scottish council area") "[new]")))))

(defn write-area-claims-in-wikibase []
  (let [predicate-label "has UK government code"
        predicate-p-number (wb-sparql/pq-number predicate-label)]
    (doseq [area @sg-areas]
      (let [subject-label (:label area)
            object (:ukGovCode area)]
        (println "Writing" subject-label predicate-label "...")
        (if-let [claim-id (wb-sparql/claim-id subject-label predicate-label)]
          (println "Claim:" claim-id "[already]")
          (println "Claim:" (wb-api/create-string-object-claim wb-csrf-token (wb-sparql/pq-number subject-label) predicate-p-number object) "[new]"))))))

;----------------------

(defn create-item-object-claim [wb-csrf-token item-qid predicate-pid object]
  (wb-api/create-item-object-claim wb-csrf-token item-qid predicate-pid (wb-sparql/pq-number object)))

(defn claim-creator-fn [predicate-label]
  (->> predicates
    (filter #(= predicate-label (first %)))
    first ; shouldbe only 1 anyway
    (#(nth % 2)) ; 3rd column
    (get {"wikibase-item" create-item-object-claim
          "quantity" wb-api/create-quantity-object-claim
          "external-id" wb-api/create-string-object-claim
          "time" wb-api/create-year-object-claim})))

(def population-dataset-mapper
  {:item-label-fn (fn [row] (str "population " (:areaLabel row) " " (:year row)))
   :item-description-fn (fn [row] (str "the population of " (:areaLabel row) " in " (:year row)))
   :claim-mappers [{:predicate-label "has quantity" :object-fn (fn [row] (:quantity row))}
                   {:predicate-label "for area" :object-fn (fn [row] (:areaLabel row))}
                   {:predicate-label "for time" :object-fn (fn [row] (:year row))}
                   {:predicate-label "for concept" :object-fn (fn [_] "population, the concept")}]})


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

(defn write-population-dataset-to-wikibase []
  (write-dataset-to-wikibase wb-csrf-token population-dataset-mapper @sg-population-dataset))

;----------------------



(defn write-co2e-items-into-wikibase []
  (doseq [co2e @sepa-co2es]
    (let [area (:council co2e)
          year (:year co2e)
          label (str "carbon equivalent " area " " year)]
      (println "Writing:" label)
      (if-let [q-number (wb-sparql/pq-number label)]
        (println "Item:" q-number "[already]")
        (println "Item" (wb-api/create-item wb-csrf-token label (str "the CO2e emitted from " area " household waste in " year)) "[new]")))))

(defn write-co2e-claims-in-wikibase []
  (doseq [src-co2e @sepa-co2es]
    (let [area (:council src-co2e)
          year (:year src-co2e)
          quantity (:TCO2e src-co2e)
          co2e-label (str "carbon equivalent " area " " year)]
      (println "Writing:" co2e-label "is a" "carbon equivalent quantity")
      (if-let [claim-id (wb-sparql/claim-id co2e-label "is a")]
          (println "Claim:" claim-id "[already]")
          (println "Claim:" (wb-api/create-item-object-claim 
                                 wb-csrf-token 
                                 (wb-sparql/pq-number co2e-label) 
                                 (wb-sparql/pq-number "is a")
                                 (wb-sparql/pq-number "carbon equivalent quantity")) "[new]"))
      (println "Writing:" co2e-label "has area" area)
      (if-let [claim-id (wb-sparql/claim-id co2e-label "for area")]
          (println "Claim:" claim-id "[already]")
          (println "Claim:" (wb-api/create-item-object-claim 
                                 wb-csrf-token 
                                 (wb-sparql/pq-number co2e-label) 
                                 (wb-sparql/pq-number "for area")
                                 (wb-sparql/pq-number area)) "[new]"))
      (println "Writing:" co2e-label "for time" year)
      (if-let [claim-id (wb-sparql/claim-id co2e-label "for time")]
          (println "Statement:" claim-id "[already]")
          (println "Statement:" (wb-api/create-year-object-claim 
                                 wb-csrf-token 
                                 (wb-sparql/pq-number co2e-label) 
                                 (wb-sparql/pq-number "for time")
                                 year) "[new]"))
      (println "Writing:" co2e-label "has quantity" quantity)
      (if-let [claim-id (wb-sparql/claim-id co2e-label "has quantity")]
          (println "Statement:" claim-id "[already]")
          (println "Statement:" (wb-api/create-quantity-object-claim 
                                 wb-csrf-token 
                                 (wb-sparql/pq-number co2e-label) 
                                 (wb-sparql/pq-number "has quantity")
                                 quantity) "[new]")))))



(defn write-scotland-into-wikibase []
  (let [label "Scotland"]
		  (println (str label "... "))
    (try
      (if-let [q-number (wb-sparql/pq-number label)]
        (println "\t" q-number "[already]")
        (println "\t" (wb-api/create-item wb-csrf-token label "a UK country area")))
      (catch Throwable t 
        (println (.getMessage t))))))

(defn write-part-ofs-into-wikibase []
  (let [areas (sg-sparql/areas)
        scotland (wb-sparql/pq-number "Scotland")
        part-of (wb-sparql/pq-number "part of")]
    (doseq [area areas]
      (let [label (:label area)]
        (println (str label "... "))
        (try
          (println "\t" (wb-api/create-item-object-claim wb-csrf-token (wb-sparql/pq-number label) part-of scotland))
          (catch Throwable t (println (.getMessage t))))))))



