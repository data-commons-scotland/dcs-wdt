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
(defn- data [item-label item-description predicate-object-threes]
  {:labels {:en {:language "en"
                 :value item-label}}
   :claims (claims predicate-object-threes)
   :aliases {}
   :descriptions {:en {:language "en"
                       :value item-description}}
   :sitelinks {}})


(defn- write-item [form-params]
  (let [response (http-call http/post form-params)]
    (when (contains? response :error)
      (throw (RuntimeException. (-> response :error :info))))
    (-> response
      :entity
      :id)))

(defn overwrite-item [csrf-token item-qid item-label item-description predicate-object-threes]
  (write-item {:action "wbeditentity"
               :id item-qid
               :clear true
               :data (json/write-str (data item-label item-description predicate-object-threes))
               :token csrf-token}))

(defn create-item [csrf-token item-label item-description predicate-object-threes]
  (write-item {:action "wbeditentity"
               :new "item"
               :data (json/write-str (data item-label item-description predicate-object-threes))
               :token csrf-token}))

  
(comment """

example invocation args

                 "carbon equivalent North Lanarkshire 2018"
                 "the CO2e quantity from North Lanarkshire in 2018"
                 [["P19" "wikibase-item" "Q41"]
                  ["P107" "time" 2018]
                  ["P110" "quantity" 353976.8102]
                  ["P25" "wikibase-item" "Q218"]
                  ["P111" "wikibase-item" "Q222"]]
""")




