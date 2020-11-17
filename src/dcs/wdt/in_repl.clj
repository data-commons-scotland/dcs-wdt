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


(println "Authenticating to Wikibase... ")
(def wb-csrf-token (wb-api/do-login-seq (misc/envvar "WB_USERNAME") (misc/envvar "WB_PASSWORD")))
(println "\t" (if (some? wb-csrf-token) "got" "didn't get") "auth token")


(println "Loading Scot Gov areas... ")
(def sg-areas (sg-sparql/areas))
(println "\t" (count sg-areas) "loaded")


(print "Loading Scot Gov populations... ")
(def sg-populations (sg-sparql/populations))
(println "\t" (count sg-populations) "loaded")


(print "Loading SEPA co2es... ")
(def sepa-co2es (sepa-file/co2es))
(println "\t" (count sepa-co2es) "loaded")


(def predicates [; common classification or composition
                 ["is instance of" "the class of this" "wikibase-item"]
                 ["part of" "the containment structure of this" "wikibase-item"]
                 
                 ; common value, points to built-in data values
                 ["has quantity" "the quantity of this" "quantity"]
                 ["has UK government code" "has the nine-character UK Government Statistical Service code" "external-id"] ;TODO "external-id"?
                 
                 ; common "dimension", points to built-in datatype values
                 ["for time" "the year of this" "time"]
                 
                 ; common "dimension", points item values
                 ["for area" "the area of this" "wikibase-item"]
                 
                 ; Household Waste "dimension", points to item values
                 ["for end-state" "the waste management end-state of this" "wikibase-item"]
                 ["for material" "the waste management material of this" "wikibase-item"]])

(defn write-predicates-into-wikibase []
  (doseq [[label description datatype] predicates]
    (println "Writing" label "...")
    (if-let [p-number (wb-sparql/pq-number label)]
      (println p-number "[already]")
      (println (wb-api/create-property wb-csrf-token label description datatype) "[new]"))))


(def wb-ref-items [["council area" ""]
                   ["household waste solids quantity" ""]
                   ["population quantity" ""]
                   ["household waste solids material" ""]
                   ["waste management end-state" ""]])


(defn write-area-items-in-wikibase []
  (doseq [area sg-areas]
    (let [label (:label area)]
      (println "Writing" label "...")
      (if-let [q-number (wb-sparql/pq-number label)]
        (println q-number "[already]")
        (println (wb-api/create-item wb-csrf-token label "a Scottish council area") "[new]")))))

(defn write-area-claims-in-wikibase []
  (let [predicate-label "has UK government code"
        predicate-p-number (wb-sparql/pq-number predicate-label)]
    (doseq [area sg-areas]
      (let [subject-label (:label area)
            object (:ukGovCode area)]
        (println "Writing" subject-label predicate-label "...")
        (if-let [claim-id (wb-sparql/claim-id subject-label predicate-label)]
          (println claim-id "[already]")
          (println (wb-api/create-string-object-claim wb-csrf-token (wb-sparql/pq-number subject-label) predicate-p-number object) "[new]"))))))



(defn write-population-items-in-wikibase []
  (doseq [population sg-populations]
    (let [label (str "population " (:areaLabel population) " " (:year population))]
      (println (str label "... "))
      (if-let [q-number (wb-sparql/pq-number label)]
        (println "\t" q-number "[already]")
        (println "\t" (wb-api/create-item wb-csrf-token label (str "the population of " (:areaLabel population) " in " (:year population))) "[new]")))))

(defn write-population-claims-in-wikibase []
  (doseq [src-population sg-populations]
    (let [area-label (:areaLabel src-population)
          year (:year src-population)
          quantity (:quantity src-population)
          population-label (str "population " area-label " " year)]
      (println "Writing:" population-label "has area" area-label)
      (if-let [claim-id (wb-sparql/claim-id population-label "for area")]
          (println "claim-id:" claim-id "[already]")
          (println "claim-id:" (wb-api/create-item-object-claim 
                                 wb-csrf-token 
                                 (wb-sparql/pq-number population-label) 
                                 (wb-sparql/pq-number "for area")
                                 (wb-sparql/pq-number area-label)) "[new]"))
      (println "Writing:" population-label "for time" year)
      (if-let [claim-id (wb-sparql/claim-id population-label "for time")]
          (println "claim-id:" claim-id "[already]")
          (println "claim-id:" (wb-api/create-year-object-claim 
                                 wb-csrf-token 
                                 (wb-sparql/pq-number population-label) 
                                 (wb-sparql/pq-number "for time")
                                 year) "[new]"))
      (println "Writing:" population-label "has quantity" quantity)
      (if-let [claim-id (wb-sparql/claim-id population-label "has quantity")]
          (println "claim-id:" claim-id "[already]")
          (println "claim-id:" (wb-api/create-quantity-object-claim 
                                 wb-csrf-token 
                                 (wb-sparql/pq-number population-label) 
                                 (wb-sparql/pq-number "has quantity")
                                 quantity) "[new]")))))

(defn write-co2e-items-in-wikibase []
  (doseq [co2e sepa-co2es]
    (let [label (str "carbon equivalent " (:council co2e) " " (:year co2e))]
      (println (str label "... "))
      (if-let [q-number (wb-sparql/pq-number label)]
        (println "\t" q-number "[already]")
        (println "\t" (wb-api/create-item wb-csrf-token label (str "the CO2e emitted from " (:council co2e) " household waaste in " (:year co2e))) "[new]")))))


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



