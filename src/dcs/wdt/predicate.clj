(ns dcs.wdt.predicate)


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




