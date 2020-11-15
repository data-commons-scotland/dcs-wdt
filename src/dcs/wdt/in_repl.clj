(ns dcs.wdt.in-repl
  (:require
    [taoensso.timbre :as log]
    [clojure.pprint :as pp]
    [dcs.wdt.misc :as misc]
    [dcs.wdt.wikibase :as wb]
    [dcs.wdt.scot-gov :as sg]))

(log/set-level! :info)

(def wb-csrf-token (atom nil))
(def wb-property-Ps (atom nil))
(def wb-area-Qs (atom nil))

(def sg-household-waste (atom nil))

(def batch-report (atom nil))

; For summarising the results from the last significant batch operation
(defn reset-batch-report! (reset! batch-report []))
(defn add-success! [main-input main-outcome] (swap! batch-report conj {:status :success :main-input main-input :main-outcome main-outcome}))
(defn add-failure! [main-input err-msg ex] (swap! batch-report conj {:status :failure :main-input main-input :err-msg err-msg :exception ex}))
(defn batch-report-summary [] (str (count (filter #(= :success (:status %)) @batch-report)) " successes; "
                                   (count (filter #(= :failure (:status %)) @batch-report)) " failures"))

(defn establish-wb-csrf-token []
  (reset! wb-csrf-token (wb/do-login-seq (misc/envvar "WB_USERNAME") (misc/envvar "WB_PASSWORD")))
  (println @wb-csrf-token))


(def wb-properties [["is instance of" "the class of this"]
                 ["part of" "the containment structure of this"]
                 ["for area" "the area of this"]
                 ["for time" "the year of this"]
                 ["for end-state" "the waste management end-state of this"]
                 ["for material" "the waste management material of this"]
                 ["has quantity" "the quantity of this"]
                 ["TEST has source" "the source of this"]])

(defn write-wb-properties []
  (reset-batch-report!)
  (doseq [[label description] wb-properties]
    (print (str "Writing " label "... "))
    (try
      (let [p-number (wb/create-entity @wb-csrf-token label description "wikibase-item")]
        (println p-number)
        (add-success! label p-number))
      (catch Throwable ex
        (let [err-msg (.getMessage ex)]
          (println err-msg)
          (add!-failure! label err-msg ex)))))
  (println (batch-report-summary)))


(def wb-ref-items [["council area" ""]
                      ["household waste solids quantity" ""]
                      ["population quantity" ""]
                      ["household waste solids material" ""]
                      ["waste management end-state" ""]])

(defn create-areas-in-wikibase []
  (let [areas (sg/get-areas)]
    (log/info (count areas) "areas sourced")
      (log/info "Logged into wikibase")
      (doseq [area areas]
        (let [label (:label area)]
          (print (str label "... "))
          (try
            (println (wb/create-entity @wb-csrf-token label "a Scottish council area"))
            (catch Throwable t (println (.getMessage t))))))))

(defn create-scotland-in-wikibase []
  (let [areas (sg/get-areas)]
      (log/info "Logged into wikibase")
      (let [label "Scotland"]
        (print (str label "... "))
        (try
          (println (wb/create-entity @wb-csrf-token label "a UK country"))
          (catch Throwable t (println (.getMessage t)))))))

(defn create-part-of-statements-in-wikibase []
  (let [areas (sg/get-areas)
        scotland (wb/get-pq-number "Scotland")
        part-of (wb/get-pq-number "part of")]
      (log/info "Logged into wikibase")
      (doseq [area areas]
        (let [label (:label area)]
          (print (str label "... "))
          (try
            (println (wb/create-statement @wb-csrf-token (wb/get-pq-number label) part-of scotland))
            (catch Throwable t (println (.getMessage t))))))))



