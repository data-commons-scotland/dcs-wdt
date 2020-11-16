(ns dcs.wdt.misc
	(:require [clj-http.client :as http]
    	[clojure.string :as str]
    	[clojure.data.csv :as csv])
	(:import java.net.URLEncoder))

(defn envvar [name]
  (if-let [value (System/getenv name)]
    value
    (throw (AssertionError. (str "Expected the environment variable " name " to have been defined")))))

; Convert the CSV structure to a list-of-maps structure.
(defn to-maps [csv-data]
  (map zipmap (->> (first csv-data)
                   (map keyword)
                   repeat)
       (rest csv-data)))

; Ask the service to execute the given SPARQL query
; and return its result as a list-of-maps.
(defn exec-sparql [service-url sparql]
  (->> (http/post service-url
                  {:body    (str "query=" (URLEncoder/encode sparql))
                   :headers {"Accept"       "text/csv"
                             "Content-Type" "application/x-www-form-urlencoded"}
                   :debug   false})
       :body
       csv/read-csv
       to-maps))