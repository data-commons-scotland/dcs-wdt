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



(defn datavalue [datatype value]
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

(defn snakvec [predicate-pid datatype object-value]
  [{:mainsnak {:snaktype "value"
               :property (str predicate-pid)
               :datavalue (datavalue datatype object-value)
               :datatype datatype}
    :type "statement"
    :rank "normal"}])

; predicate-object-threes = [[predicate-pid object-datatype object-value] ...]
(defn claims [predicate-object-threes]
  (->> predicate-object-threes
    (map (fn [[predicate-pid object-datatype object-value]] 
           [predicate-pid (snakvec predicate-pid object-datatype object-value)]))
    (into {})))
  
(defn data [item-label item-description predicate-object-threes]
  {:labels {:en {:language "en"
                 :value item-label}}
   :claims (claims predicate-object-threes)
   :aliases {}
   :descriptions {:en {:language "en"
                       :value item-description}}
   :sitelinks {}})

(defn x-clear-then-write [csrf-token]
  (let [d  
        {:labels
  {:en
   {:language "en",
    :value "carbon equivalent North Lanarkshire 2018"}},
  :claims
  {:P19
   [{:mainsnak
     {:snaktype "value",
      :property "P19",
      :datavalue
      {:value {:entity-type "item", :numeric-id 41, :id "Q41"},
       :type "wikibase-entityid"},
      :datatype "wikibase-item"},
     :type "statement"
     :rank "normal"}],
   :P107
   [{:mainsnak
     {:snaktype "value",
      :property "P107",
      :datavalue
      {:value
       {:time "+2018-00-00T00:00:00Z",
        :timezone 0,
        :before 0,
        :after 0,
        :precision 9,
        :calendarmodel "http://www.wikidata.org/entity/Q1985727"},
       :type "time"},
      :datatype "time"},
     :type "statement"
     :rank "normal"}],
   :P110
   [{:mainsnak
     {:snaktype "value",
      :property "P110",
      :datavalue
      {:value {:amount "+353976.8101", :unit "1"}, :type "quantity"},
      :datatype "quantity"},
     :type "statement",
     :rank "normal"}],
   :P25
   [{:mainsnak
     {:snaktype "value",
      :property "P25",
      :datavalue
      {:value {:entity-type "item", :numeric-id 218, :id "Q218"},
       :type "wikibase-entityid"},
      :datatype "wikibase-item"},
     :type "statement",
     :rank "normal"}],
   :P111
   [{:mainsnak
     {:snaktype "value",
      :property "P111",
      :datavalue
      {:value {:entity-type "item", :numeric-id 222, :id "Q222"},
       :type "wikibase-entityid"},
      :datatype "wikibase-item"},
     :type "statement",
     :rank "normal"}]},
  :aliases {},
  :descriptions
  {:en
   {:language "en",
    :value "the CO2e quantity from North Lanarkshire in 2018"}},
  :sitelinks {}}
        
        
        d2 (data "carbon equivalent North Lanarkshire 2018"
                 "the CO2e quantity from North Lanarkshire in 2018"
                 [["P19" "wikibase-item" "Q41"]
                  ["P107" "time" 2018]
                  ["P110" "quantity" 353976.8102]
                  ["P25" "wikibase-item" "Q218"]
                  ["P111" "wikibase-item" "Q222"]])
        response (http-call http/post
                            {:action "wbeditentity"
                             :id    "Q200"
                             :clear true
                             :data   (json/write-str d2)
                             :token  csrf-token})]
    (pp/pprint response)))

(comment """


write entity
  
  if not-already
  
    create a new one with the full structure





    clear all its data and given it full new data # it might be nice to have a version of this that overwrites only some of the data, but let's keep it easy for now
    






Search for "alphabet" in English language for type property

http://strf8b46abcf478/api.php?action=wbsearchentities&search=Stirling&language=en&type=item

&format=json


http://strf8b46abcf478/api.php?action=wbgetentities&ids=Q118&languages=en

http://strf8b46abcf478/api.php?action=wbgetentities&titles=Stirling&languages=en&sites=NEEDED

http://strf8b46abcf478/api.php?action=wbeditentity&id=Q118&data={}


Creates a new claim on the item for the property P56 and a value of "ExampleString"
api.php
?action=wbeditentity
&id=Q42
&data={"claims":[{"mainsnak":{"snaktype":"value",
                              "property":"P56",
                              "datavalue":{"value":"ExampleString",
                                           "type":"string"}},
                  "type":"statement",
                  "rank":"normal"}]}

""")
