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



