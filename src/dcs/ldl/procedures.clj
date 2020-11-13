(ns dcs.ldl.procedures
  (:require
    [taoensso.timbre :as log]
    [clojure.pprint :as pp]
    [dcs.ldl.wb-api :as wb-api]
    [dcs.ldl.sparql :as sparql]))

(defn envvar [name]
  (if-let [value (System/getenv name)]
    value
    (throw (AssertionError. (str "Expected the environment variable " name " to have been defined")))))

(def wb-username (envvar "WB_USERNAME"))
(def wb-password (envvar "WB_PASSWORD"))

(def properties [["is instance of"       "the class of this"]
                 ["part of"              "the containment structure of this"]
	                ["for area"             "the area of this"]
                 ["for time"             "the year of this"]
                 ["for end-state"        "the waste management end-state of this"]
                 ["for material"         "the waste management material of this"]
                 ["has quantity"         "the quantity of this"]
                 ["TEST has source"           "the source of this"]])

(def reference-items [["council area" ""]
                      ["year" ""]
                      ["household waste solids quantity" ""]
                      ["population quantity" ""]
                      ["household waste solids material" ""]
                      ["waste management end-state" ""]])

(defn create-properties-in-wikibase []
  (let [csrf-token (wb-api/do-login-seq wb-username wb-password)]
    (log/info "Logged into wikibase")
    (doseq [[label description] properties]
      (print (str label "... "))
      (try 
        (println (wb-api/create-entity csrf-token label description "wikibase-item"))
        (catch Throwable t (println (.getMessage t)))))))

(defn create-areas-in-wikibase []
  (let [areas (sparql/get-areas)]
    (log/info (count areas) "areas sourced")
    (let [csrf-token (wb-api/do-login-seq wb-username wb-password)]
      (log/info "Logged into wikibase")
      (doseq [area areas]
        (let [label (:label area)]
          (print (str label "... "))
          (try 
            (println (wb-api/create-entity csrf-token label "a Scottish council area"))
            (catch Throwable t (println (.getMessage t)))))))))

(defn create-scotland-in-wikibase []
  (let [areas (sparql/get-areas)]
   (let [csrf-token (wb-api/do-login-seq wb-username wb-password)]
      (log/info "Logged into wikibase")
      (let [label "Scotland"]
          (print (str label "... "))
          (try 
            (println (wb-api/create-entity csrf-token label "a UK country"))
            (catch Throwable t (println (.getMessage t))))))))

(defn create-part-of-statements-in-wikibase []
  (let [areas (sparql/get-areas)
        scotland (sparql/get-pq-number "Scotland")
  						part-of (sparql/get-pq-number "part of")]
   (let [csrf-token (wb-api/do-login-seq wb-username wb-password)]
     (log/info "Logged into wikibase")
     (doseq [area areas]
        (let [label (:label area)]
          (print (str label "... "))
          (try
            (println (wb-api/create-statement csrf-token (sparql/get-pq-number label) part-of scotland))
            (catch Throwable t (println (.getMessage t)))))))))



