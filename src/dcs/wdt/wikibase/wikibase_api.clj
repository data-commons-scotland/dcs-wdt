(ns dcs.wdt.wikibase.wikibase-api
  (:require
    [taoensso.timbre :as log]
    [clj-http.client :as http]
    [clj-http.cookies :as cookies]
    [clojure.string :as s]
    [clojure.data :refer [diff]]
    [clojure.data.json :as json]
    [clojure.pprint :as pp]))

(def cookie-store (cookies/cookie-store))

;(def wbi-url "http://strf8b46abcf478/api.php")
(def wbi-url "https://waste-commons-scotland.wiki.opencura.com/w/api.php")

(defn- http-call [meth-fn params]
  (let [params-type (if (= http/get meth-fn)
                      :query-params
                      :form-params)
        response (meth-fn wbi-url
                          {params-type (merge {:format "json"}
                                              params)

                           :cookie-store cookie-store})]
    (log/debug response)
    (-> response
      :body
      (json/read-str :key-fn keyword))))


; --------------------------------------------


(defn- get-login-token []
  (-> (http-call http/get
                 {:action "query"
                  :meta   "tokens"
                  :type   "login"})
      :query
      :tokens
      :logintoken))


(defn- login [login-token username password]
  (http-call http/post
             {:action     "login"
              :lgname     username
              :lgpassword password
              :lgtoken    login-token}))

(defn- get-csrf-token []
  (-> (http-call http/get
                 {:action "query"
                  :meta   "tokens"})
      :query
      :tokens
      :csrftoken))

(defn do-login-seq [username password]
  (-> (get-login-token)
      (login username password))
  (get-csrf-token))



; --------------------------------------------


(def datatype-cache (atom {}))

; Returns the predicates datatype
(defn datatype [pid]
  (if-let [datatype (get @datatype-cache pid)]
    datatype
    (let [response (http-call http/get
                              {:action "wbgetentities"
                               :ids pid
                               :props "datatype"})]
      (if-let [datatype (some-> response :entities (get (keyword pid)) :datatype)]
        (do
          (swap! datatype-cache assoc pid datatype)
          datatype)
        nil))))


; --------------------------------------------


(defn get-entities [pqids]
  (http-call http/get
             {:action "wbgetentities"
              :ids    (s/join "|" pqids)}))


(defn- create-claim [csrf-token subject-q-number property-p-number object]
  (let [response (http-call http/post
                            {:action   "wbcreateclaim"
                             :entity   subject-q-number
                             :property property-p-number
                             :value    object
                             :snaktype "value"
                             :bot      "1"
                             ;:errorformat "plaintext"
                             :token    csrf-token})]
    (when (contains? response :error)
      (throw (RuntimeException. (-> response :error :info))))
    (-> response
      :claim
      :id)))


; --------------------------------------------


; Returns a value for :datavalue
(defn- datavalue [datatype value]
  (condp = datatype
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
                  :type "quantity"})
    "external-id" (let [external-id value]
                    {:value external-id
                     :type "string"})))

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
(defn create-item [csrf-token label description predicate-object-threes]
  (create-entity csrf-token "item" (data label description predicate-object-threes)))

; Create a new property.
(defn create-property [csrf-token label description datatype predicate-object-threes]
  (create-entity csrf-token "property" (data label description datatype predicate-object-threes)))


; Overwrite an existing item.
(defn overwrite-item [csrf-token pqid label description predicate-object-threes]
  (overwrite-entity csrf-token pqid (data label description predicate-object-threes)))

; Overwrite an existing property.
(defn overwrite-property [csrf-token pqid label description datatype predicate-object-threes]
  (overwrite-entity csrf-token pqid (data label description datatype predicate-object-threes)))



