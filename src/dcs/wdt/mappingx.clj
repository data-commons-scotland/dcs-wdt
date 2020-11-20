(ns dcs.wdt.mappingx
  (:require [dcs.wdt.predicate :as predicate]
            [dcs.wdt.wikibase-sparql :as wb-sparql]))


(defn concepts-dataset-mapper [row]
  [(:label row)
   (:description row)
   []])


(defn areas-dataset-mapper [row]
  [(:label row)
   "a Scottish council area"
   [(let [p "has UK government code"] [(wb-sparql/pqid p) (predicate/datatype p) (:ukGovCode row)])
    (let [p "part of"] [(wb-sparql/pqid p) (predicate/datatype p) (wb-sparql/pqid "Scotland")])]])
    

(defn population-dataset-mapper [row]
  [(str "population " (:areaLabel row) " " (:year row))
   (str "the population of " (:areaLabel row) " in " (:year row))
   [(let [p "has quantity"] [(wb-sparql/pqid p) (predicate/datatype p) (:quantity row)])
    (let [p "for area"] [(wb-sparql/pqid p) (predicate/datatype p) (wb-sparql/pqid (:areaLabel row))])
    (let [p "for time"] [(wb-sparql/pqid p) (predicate/datatype p) (:year row)])
    (let [p "for concept"] [(wb-sparql/pqid p) (predicate/datatype p) (wb-sparql/pqid "population, the concept")])]])


(defn end-states-dataset-mapper [row]
  [(:endState row)
   "a waste generated end-state"
   [(let [p "for concept"] [(wb-sparql/pqid p) (predicate/datatype p) (wb-sparql/pqid "waste generated, the concept")])]])

(defn materials-dataset-mapper [row]
  [(:material row)
   "a waste generated material"
   [(let [p "for concept"] [(wb-sparql/pqid p) (predicate/datatype p) (wb-sparql/pqid "waste generated, the concept")])]])

(defn waste-generated-dataset-mapper [row]
  [(str "waste generated " (:area row) " " (:year row) " " (:endState row) " " (:material row))
   (str "the waste generated in " (:area row) " in " (:year row) " ending up as " (:endState row) " comprised of " (:material row))
   [(let [p "has quantity"] [(wb-sparql/pqid p) (predicate/datatype p) (:tonnes row)])
    (let [p "for area"] [(wb-sparql/pqid p) (predicate/datatype p) (wb-sparql/pqid (:area row))])
    (let [p "for time"] [(wb-sparql/pqid p) (predicate/datatype p) (:year row)])
    (let [p "for end-state"] [(wb-sparql/pqid p) (predicate/datatype p) (wb-sparql/pqid (:endState row))])
    (let [p "for material"] [(wb-sparql/pqid p) (predicate/datatype p) (wb-sparql/pqid (:material row))])
    (let [p "for concept"] [(wb-sparql/pqid p) (predicate/datatype p) (wb-sparql/pqid "waste generated, the concept")])]])


(defn co2e-dataset-mapper [row]
  [(str "carbon equivalent " (:council row) " " (:year row))
   (str "the CO2e emitted from " (:council row) " household waste in " (:year row))
   [(let [p "has quantity"] [(wb-sparql/pqid p) (predicate/datatype p) (:TCO2e row)])
    (let [p "for area"] [(wb-sparql/pqid p) (predicate/datatype p) (wb-sparql/pqid (:council row))])
    (let [p "for time"] [(wb-sparql/pqid p) (predicate/datatype p) (:year row)])
    (let [p "for concept"] [(wb-sparql/pqid p) (predicate/datatype p) (wb-sparql/pqid "carbon equivalent, the concept")])]])



