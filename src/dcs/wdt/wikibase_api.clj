(ns dcs.wdt.wikibase-api
  (:require
    [taoensso.timbre :as log]
    [clj-http.client :as http]
    [clojure.string :as s]
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


(defn get-entities [pq-numbers]
  (http-call http/get
             {:action "wbgetentities"
              :ids    (s/join "|" pq-numbers)}))


(defn- create-entity [csrf-token entity-type data]
  (let [response (http-call http/post
                            {:action "wbeditentity"
                             :new    entity-type
                             :data   (json/write-str data)
                             :token  csrf-token})]
    (when (contains? response :error)
      (throw (RuntimeException. (-> response :error :info))))
    (-> response
        :entity
        :id)))


(defn- label-desc-map [label description]
  {:labels       {:en {:language "en"
                       :value    label}}
   :descriptions {:en {:language "en"
                       :value    description}}})


(defn create-property
  ([csrf-token label description datatype]
   (create-entity csrf-token "property" (assoc (label-desc-map label description) :datatype datatype))))
  
  
(defn create-item
  ([csrf-token label description]
   (create-entity csrf-token "item" (label-desc-map label description))))


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

(defn create-item-object-claim [csrf-token subject-q-number property-p-number object-q-number]
  (create-claim csrf-token subject-q-number property-p-number 
                (json/write-str {:entity-type "item"
                                 :numeric-id  (subs object-q-number 1)})))

(defn create-string-object-claim [csrf-token subject-q-number property-p-number value-object]
  (create-claim csrf-token subject-q-number property-p-number 
                (json/write-str value-object)))

(defn create-quantity-object-claim [csrf-token subject-q-number property-p-number quantity-object]
  (create-claim csrf-token subject-q-number property-p-number 
                (json/write-str {:amount (str "+" quantity-object)
                                 :unit "1"})))

(defn create-year-object-claim [csrf-token subject-q-number property-p-number year-object]
  (create-claim csrf-token subject-q-number property-p-number 
                (json/write-str {:time (str "+" year-object "-00-00T00:00:00Z")
                                 :timezone 0
                                 :before 0
                                 :after 0
                                 :precision 9
                                 :calendarmodel "http://www.wikidata.org/entity/Q1985727"})))
