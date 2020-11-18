(ns dcs.wdt.predicate
  (:require
    [dcs.wdt.wikibase-api :as wb-api]
    [dcs.wdt.wikibase-sparql :as wb-sparql]))


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

(defn write-predicates-to-wikibase [wb-csrf-token]
  (let [number-of-rows (count predicates)]
    (doseq [[ix row] (map-indexed vector predicates)] ; remember that a row is really a map
      (println "Predicate row:" (inc ix) "of" number-of-rows)
      (let [label (:label row)]
        (println "Writing property:" label)
        (let [[pid status] (if-let [pid (wb-sparql/pq-number label)]
                             [pid "already"]
                             [(wb-api/create-property wb-csrf-token label (:description row) (:datatype row)) "new"])]
          (println "Property:" pid (str "[" status "]")))))))

(defn- create-item-object-claim [wb-csrf-token item-qid predicate-pid object]
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

