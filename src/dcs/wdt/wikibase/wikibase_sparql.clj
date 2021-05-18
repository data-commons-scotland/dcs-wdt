(ns dcs.wdt.wikibase.wikibase-sparql
  (:require
  		[clojure.string :as str]
    [dcs.wdt.wikibase.misc :as misc]))


;(def service-url "http://strf8b46abcf478:8282/proxy/wdqs/bigdata/namespace/wdq/sparql")
(def service-url "https://waste-commons-scotland.wiki.opencura.com/query/sparql?")

;(def host-for-prefixes "wikbase.svc")
(def host-for-prefixes "waste-commons-scotland.wiki.opencura.com")

(def prefixes (format (str """
prefix wd: <http://%s/entity/>
prefix wdt: <http://%s/prop/direct/>
""")
                          host-for-prefixes
                          host-for-prefixes))


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

; Strict
(defn pqid-s [label]
  (if-let [pqid (pqid label)]
    pqid
    (throw (RuntimeException. (str "Entity '" label "' not found. (Perhaps because of the delay in propogating to the SPARQL subsystem. If so, retry!)")))))

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



(defn count [sparql]
  (->> sparql
    (misc/exec-sparql service-url)
    first
    :count
    Integer/parseInt))