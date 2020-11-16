(ns dcs.wdt.in-repl
  (:require
    [taoensso.timbre :as log]
    [clojure.pprint :as pp]
    [dcs.wdt.misc :as misc]
    [dcs.wdt.wikibase-api :as wb-api]
    [dcs.wdt.wikibase-sparql :as wb-sparql]
    [dcs.wdt.scotgov-sparql :as sg-sparql]
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


(def properties [; common classification or composition
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

(defn write-properties-into-wikibase []
  (doseq [[label description datatype] properties]
    (println (str label "... "))
    (if-let [p-number (wb-sparql/pq-number label)]
      (println "\t" p-number "(already)")
      (println "\t" (wb-api/create-property wb-csrf-token label description datatype) "(new)"))))


(def wb-ref-items [["council area" ""]
                   ["household waste solids quantity" ""]
                   ["population quantity" ""]
                   ["household waste solids material" ""]
                   ["waste management end-state" ""]])


(defn write-area-items-in-wikibase []
  (doseq [area sg-areas]
    (let [label (:label area)]
      (println (str label "... "))
      (if-let [q-number (wb-sparql/pq-number label)]
        (println "\t" q-number "(already)")
        (println "\t" (wb-api/create-item wb-csrf-token label "a Scottish council area") "(new)")))))

(defn write-area-claims-in-wikibase []
  (let [predicate-label "has UK government code"
        predicate-p-number (wb-sparql/pq-number predicate-label)]
    (doseq [area sg-areas]
      (let [subject-label (:label area)
            object (:ukGovCode area)]
        (println (str subject-label " " predicate-label "... "))
        (if-let [claim-id (wb-sparql/claim-id subject-label predicate-label)]
          (println "\t" claim-id "(already)")
          (println "\t" (wb-api/create-value-object-claim wb-csrf-token (wb-sparql/pq-number subject-label) predicate-p-number object) "(new)"))))))



(defn write-population-items-in-wikibase []
  (doseq [population sg-populations]
    (let [label (str "population " (:areaLabel population) " " (:year population))]
      (println (str label "... "))
      (if-let [q-number (wb-sparql/pq-number label)]
        (println "\t" q-number "(already)")
        (println "\t" (wb-api/create-item wb-csrf-token label (str "the population of " (:areaLabel population) " in " (:year population))) "(new)")))))

(defn write-population-claims-in-wikibase []
  (let [predicate-label "has quantity"
        predicate-p-number (wb-sparql/pq-number predicate-label)]
    (doseq [population sg-populations]
      (let [subject-label (str "population " (:areaLabel population) " " (:year population))
            object (:population population)]
        (println (str subject-label " " predicate-label "... "))
        (println (wb-sparql/pq-number subject-label) predicate-p-number object)
        (if-let [claim-id (wb-sparql/claim-id subject-label predicate-label)]
          (println "\t" claim-id "(already)")
          (println "\t" (wb-api/create-quantity-object-claim wb-csrf-token (wb-sparql/pq-number subject-label) predicate-p-number object) "(new)"))))))



(defn write-scotland-into-wikibase []
  (let [label "Scotland"]
		  (println (str label "... "))
    (try
      (if-let [q-number (wb-sparql/pq-number label)]
        (println "\t" q-number "(already)")
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



