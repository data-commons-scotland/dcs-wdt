(ns dcs.wdt.writing
  (:require [dcs.wdt.predicate :as predicate]
            [dcs.wdt.wikibase-api :as wb-api]
            [dcs.wdt.wikibase-sparql :as wb-sparql]
            [dcs.wdt.scotgov-sparql :as sg-sparql]))
  

(defn- create-item-object-claim [wb-csrf-token item-qid predicate-pid object]
  (wb-api/create-item-object-claim wb-csrf-token item-qid predicate-pid (wb-sparql/pq-number object)))

(defn- claim-creator-fn [predicate-label]
  (->> predicate/predicates
    (filter #(= predicate-label (:label %)))
    first ; shouldbe only 1 anyway
    :datatype
    (get {"wikibase-item" create-item-object-claim
          "quantity" wb-api/create-quantity-object-claim
          "external-id" wb-api/create-string-object-claim
          "time" wb-api/create-year-object-claim})))

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


(defn write-predicates-to-wikibase [wb-csrf-token]
  (let [number-of-rows (count predicate/predicates)]
    (doseq [[ix row] (map-indexed vector predicate/predicates)] ; remember that a row is really a map
      (println "Predicate row:" (inc ix) "of" number-of-rows)
      (let [label (:label row)]
        (println "Writing property:" label)
        (let [[pid status] (if-let [pid (wb-sparql/pq-number label)]
                             [pid "already"]
                             [(wb-api/create-property wb-csrf-token label (:description row) (:datatype row)) "new"])]
          (println "Property:" pid (str "[" status "]")))))))