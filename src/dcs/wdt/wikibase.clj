(ns dcs.wdt.wikibase
  (:require
    [taoensso.timbre :as log]
    [clj-http.client :as http]
    [clojure.string :as s]
    [clojure.data.json :as json]
    [clojure.pprint :as pp]))

(def wb-api-url "http://strf8b46abcf478/api.php")

(defn http-call [meth-fn params]
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

(defn get-login-token []
  (-> (http-call http/get
                 {:action "query"
                  :meta   "tokens"
                  :type   "login"})
      :query
      :tokens
      :logintoken))

(defn login [login-token username password]
  (http-call http/post
             {:action     "login"
              :lgname     username
              :lgpassword password
              :lgtoken    login-token}))

(defn get-csrf-token []
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

(defn create-entity* [csrf-token entity-type data]
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

(defn build-map [label description]
  {:labels       {:en {:language "en"
                       :value    label}}
   :descriptions {:en {:language "en"
                       :value    description}}})

(defn create-entity
  ([csrf-token label description datatype]
   (create-entity* csrf-token "property" (assoc (build-map label description) :datatype datatype)))
  ([csrf-token label description]
   (create-entity* csrf-token "item" (build-map label description))))

(defn create-statement [csrf-token subject-q-number property-p-number object-q-number]
  (-> (http-call http/post
                 {:action   "wbcreateclaim"
                  :entity   subject-q-number
                  :property property-p-number
                  :value    (json/write-str {:entity-type "item"
                                             :numeric-id  (subs object-q-number 1)})
                  :snaktype "value"
                  :bot      "1"
                  :token    csrf-token})
      :claim
      :id))
