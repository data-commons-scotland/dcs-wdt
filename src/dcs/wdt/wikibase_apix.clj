(ns dcs.wdt.wikibase-apix
  (:require
    [taoensso.timbre :as log]
    [clj-http.client :as http]
    [clojure.string :as s]
    [clojure.data :refer [diff]]
    [clojure.data.json :as json]
    [clojure.pprint :as pp]))


(def wb-api-url "http://strf8b46abcf478/api.php")


(defn- http-call [meth-fn params]
  (let [params-type (if (= http/get meth-fn)
                      :query-params
                      :form-params)
        response (meth-fn wb-api-url
                          {params-type (merge {:format "json"}
                                              params)})]
    (log/debug response)
    (-> response
      :body
      (json/read-str :key-fn keyword))))


; Returns a value for :datavalue
(defn- datavalue [datatype value]
  (condp = datatype
    ;;TODO "external-id"
    "wikibase-item" (let [qid value]
                      {:value {:entity-type "item"
                               :numeric-id (subs qid 1)
                               :id qid}
                       :type "wikibase-entityid"})
    "time" (let [year value]
             {:value {:time (str "+" year "-00-00T00:00:00Z")
                      :timezone 0
                      :before 0
                      :after 0
                      :precision 9
                      :calendarmodel "http://www.wikidata.org/entity/Q1985727"}
              :type "time"})
    "quantity" (let [quantity value]
                 {:value {:amount (str "+" quantity)
                          :unit "1"}
                  :type "quantity"})))

; Returns a values for what is roughly a claim
(defn- snakvec [predicate-pid datatype object-value]
  [{:mainsnak {:snaktype "value"
               :property (str predicate-pid)
               :datavalue (datavalue datatype object-value)
               :datatype datatype}
    :type "statement"
    :rank "normal"}])

; Returns a value for the :claims key.
; predicate-object-threes should be structured: [[predicate-pid object-datatype object-value] ...]
(defn- claims [predicate-object-threes]
  (->> predicate-object-threes
    (map (fn [[predicate-pid object-datatype object-value]] 
           [predicate-pid (snakvec predicate-pid object-datatype object-value)]))
    (into {})))

; Returns a value for the :data key.
(defn- data 
  ([label description predicate-object-threes]
    {:labels {:en {:language "en"
                   :value label}}
     :claims (claims predicate-object-threes)
     :aliases {}
     :descriptions {:en {:language "en"
                         :value description}}
     ;:sitelinks {} ...these aren't supported for property
     })
  ([label description datatype predicate-object-threes] ; For property data
    (-> (data label description predicate-object-threes)
      (assoc :datatype datatype)))) 


; Make the HTTP call then parse its response.
(defn- write-entity [form-params]
  (let [response (http-call http/post form-params)]
    (when (contains? response :error)
      (throw (RuntimeException. (-> response :error :info))))
    (-> response
      :entity
      :id)))


; Create a new entity.
(defn- create-entity [csrf-token entity-type data]
  (write-entity {:action "wbeditentity"
               :new entity-type
               :data (json/write-str data)
               :token csrf-token}))

; Overwrite an existing entity.
(defn- overwrite-entity [csrf-token pqid data]
  (write-entity {:action "wbeditentity"
               :id pqid
               :clear true
               :data (json/write-str data)
               :token csrf-token}))


; Create a new item entity.
(defn create-item [csrf-token label description predicate-object-threes entity-type]
  (create-entity csrf-token "item" (data label description predicate-object-threes)))

; Create a new property.
(defn create-property [csrf-token label description datatype predicate-object-threes entity-type]
  (create-entity csrf-token "property" (data label description datatype predicate-object-threes)))


; Overwrite an existing item.
(defn overwrite-item [csrf-token pqid label description predicate-object-threes]
  (overwrite-entity csrf-token pqid (data label description predicate-object-threes)))

; Overwrite an existing property.
(defn overwrite-property [csrf-token pqid label description datatype predicate-object-threes]
  (overwrite-entity csrf-token pqid (data label description datatype predicate-object-threes)))



