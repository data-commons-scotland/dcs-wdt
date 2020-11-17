(ns dcs.wdt.wikibase-sparql
  (:require
  		[clojure.string :as str]
    [dcs.wdt.misc :as misc]))


(def service-url "http://strf8b46abcf478:8282/proxy/wdqs/bigdata/namespace/wdq/sparql")


(def pq-number-sparql "

SELECT 
  (strafter(str(?entity), 'http://strf8b46abcf478/entity/') as ?pqnumber)  

WHERE {  
  ?entity ?label 'LABEL'@en .
  SERVICE wikibase:label { bd:serviceParam wikibase:language 'en'. } 
}
")

(defn pq-number [label]
  (let [response (misc/exec-sparql service-url (str/replace pq-number-sparql "LABEL" label))
        n (count response)]
    (when (> n 1)
      (throw (RuntimeException. (str "Expected 0 or 1 but got " n))))
    (some-> response
        first
        :pqnumber)))


(def claim-id-sparql "
SELECT  
  (strafter(str(?statement), 'http://strf8b46abcf478/entity/statement/') as ?statementId)  
{   
  ?subjectItem ?label 'SUBJECT_LABEL'@en .
  ?subjectItem ?directProperty ?statement . 
  
  ?statement ?ps ?propertyValue .
  
  ?property ?label 'PREDICATE_LABEL'@en .
  ?property wikibase:claim ?directProperty .
  ?property wikibase:statementProperty ?ps .
}
")

(defn claim-id [subject-label predicate-label]
  (let [response (misc/exec-sparql service-url 
                                   (-> claim-id-sparql
                                     (str/replace "SUBJECT_LABEL" subject-label)
                                     (str/replace "PREDICATE_LABEL" predicate-label)))
        n (count response)]
    (when (> n 1)
      (throw (RuntimeException. (str "Expected 0 or 1 but got " n))))
    (some-> response
        first
        :statementId)))