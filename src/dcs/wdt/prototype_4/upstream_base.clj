(ns dcs.wdt.prototype-4.upstream-base
  (:require [clojure.string :as str]
            [clojure.java.io :as io]
            [clojure.data :as data]
            [clojure.data.csv :as csv]
            [clj-http.client :as http]
            [taoensso.timbre :as log]
            [dcs.wdt.prototype-4.db-base :as db-base])
  (:import java.io.PushbackReader
           java.net.URLEncoder))

(def area-aliases {"Aberdeen"           "Aberdeen City"
                   "Dundee"             "Dundee City"
                   "Edinburgh"          "City of Edinburgh"
                   "Eilean Siar"        "Outer Hebrides"
                   "Glasgow"            "Glasgow City"
                   "Na h-Eileanan Siar" "Outer Hebrides"
                   "Orkney"             "Orkney Islands"})

(def waste-category-aliases {"Discarded equipment (excl discarded vehicles, batteries and accumulators)" "Discarded equipment (excluding discarded vehicles, batteries and accumulators wastes)"})

(def years-set (set db-base/years))
(def areas-set (set db-base/areas))
(def waste-categories-set (set db-base/waste-categories))

(defn rows-to-maps [rows]
  (map zipmap
       (repeat (first rows))
       (rest rows)))

(defn skip-byte-order-marker [reader]
  (let [pbr (PushbackReader. reader)
        c (.read pbr)]
    (when (not= 65279 c)
      (.unread pbr c))
    pbr))

(defn csv-file-to-maps [file]
  (->> file
       io/reader
       skip-byte-order-marker
       csv/read-csv
       rows-to-maps))

(defn customise-map-fn [customise-map-fn m]
  (try
    (customise-map-fn m)
    (catch Throwable t
      (do (log/errorf "%s - Bad value in: %s" (.getMessage t) m)
          (throw t)))))

(defn find-csv-files [dir]
  (->> dir
       io/file
       file-seq
       (filter #(and (.isFile %)
                     (str/ends-with? (.getName %) ".csv")))))

(defn check-year-totals [quantity-field-kw expected db]
  (let [actual (into {}
                     (for [year (distinct (map :year db))]
                       [year (->> db
                                  (filter #(= year (:year %)))
                                  (map quantity-field-kw)
                                  (apply +))]))
        diff (take 2 (data/diff expected actual))]
    (when (some some? diff)
      diff)))

(def scotgov-service-url "http://statistics.gov.scot/sparql")

(defn exec-sparql [service-url sparql]
  (:body (http/post service-url
                    {:body    (str "query=" (URLEncoder/encode sparql))
                     :headers {"Accept"       "text/csv"
                               "Content-Type" "application/x-www-form-urlencoded"}
                     :debug   false})))