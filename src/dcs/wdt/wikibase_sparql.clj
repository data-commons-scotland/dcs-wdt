(ns dcs.wdt.wikibase-sparql
  (:require
  		[clojure.string :as str]
    [dcs.wdt.misc :as misc]))


(def service-url "http://strf8b46abcf478:8282/proxy/wdqs/bigdata/namespace/wdq/sparql")


(def pqid-cache (atom {}))

(def pqid-sparql "

SELECT 
  (strafter(str(?entity), '/entity/') as ?id)  
WHERE {  
  ?entity rdfs:label 'LABEL'@en .
}
")

(defn pqid [label]
  (if-let [pqid (get @pqid-cache label)]
    pqid
    (let [response (misc/exec-sparql service-url (str/replace pqid-sparql "LABEL" label))
          n (count response)]
      (when (> n 1)
        (throw (RuntimeException. (str "Expected 0 or 1 but got " n))))
      (if-let [pqid (some-> response first :id)]
        (do
          (swap! pqid-cache assoc label pqid)
          pqid)
        nil))))

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


(def subject-count-sparql "
SELECT 
  (count(distinct(?subjectItem)) as ?count)
  #?subjectItem ?subjectItemLabel ?predicateLabel
WHERE {   
  VALUES ?predicateLabel { 'has quantity'@en 
                           'has UK government code'@en
                           'for area'@en
                           'for time'@en }
  
  ?subjectItem ?directProperty ?statement ; 
               rdfs:label ?subjectItemLabel .
  
  ?statement ?ps ?_ .
  
  ?property rdfs:label ?predicateLabel ;
            wikibase:claim ?directProperty ;
            wikibase:statementProperty ?ps . 
}
")

(defn subject-count []
  (->> subject-count-sparql
    (misc/exec-sparql service-url)
    first
    :count
    Integer/parseInt))