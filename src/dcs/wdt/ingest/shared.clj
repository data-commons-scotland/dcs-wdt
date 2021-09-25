(ns dcs.wdt.ingest.shared
  "Forms that are used by more than one of the ingest namespaces."
  (:require [clojure.string :as str]
            [clojure.java.io :as io]
            [clojure.data :as data]
            [clojure.data.csv :as csv]
            [clj-http.client :as http]
            [taoensso.timbre :as log]
            [dcs.wdt.dimensions :as dim]
            [dcs.wdt.ingest.region :as region])
  (:import java.io.PushbackReader
           java.net.URLEncoder
           java.util.Date
           java.text.SimpleDateFormat))

(def region-aliases
  "For mapping to a consistent set of reasonable regions in the internal database."
  {"Aberdeen"            "Aberdeen City"
   "Argyll & Bute"       "Argyll and Bute"
   "Dumfries & Galloway" "Dumfries and Galloway"
   "Dundee"              "Dundee City"
   "Edinburgh"           "City of Edinburgh"
   "Eilean Siar"         "Outer Hebrides"
   "Glasgow"             "Glasgow City"
   "Na h-Eileanan Siar"  "Outer Hebrides"
   "Orkney"              "Orkney Islands"
   "Perth & Kinross"     "Perth and Kinross"
   "Shetland"            "Shetland Islands"})

(def material-aliases
  "For mapping to a consistent set of reasonable waste materials in the internal database."
  {"Discarded equipment (excl discarded vehicles, batteries and accumulators)" "Discarded equipment (excluding discarded vehicles, batteries and accumulators wastes)"})

(def business-sector-aliases {"Agriculture Forestry  Fishing"  "Agriculture, forestry and fishing"
                              "Chemical manufacture"           "Manufacture of chemicals, plastics and pharmaceuticals"
                              "Food & drink manufacture"       "Manufacture of food and beverage products"
                              "Manufacturing of wood products" "Manufacture of wood products"
                              "Mining & quarrying"             "Mining and quarrying"})

(def years-set (set dim/years))
(def regions-set (set (keys region/internal)))
(def materials-set (set dim/materials))
(def managements-set (set dim/managements))
(def business-sectors-set (set dim/business-sectors))

(defn- rows-to-maps
  [rows]
  (map zipmap
       (repeat (first rows))
       (rest rows)))

(defn- skip-byte-order-marker
  [reader]
  (let [pbr (PushbackReader. reader)
        c (.read pbr)]
    (when (not= 65279 c)
      (.unread pbr c))
    pbr))

(defn csv-file-to-maps
  "file should represent a CSV file.
  Returns a seq of maps, each map representing a data row from the CSV file."
  [file]
  (-> file
      io/reader
      skip-byte-order-marker
      csv/read-csv
      rows-to-maps))

(defn customise-map
  "Runs the given and logs any Throwable that results."
  [customise-map-fn m]
  (try
    (customise-map-fn m)
    (catch Throwable t
      (do (log/errorf "%s - Bad value in: %s" (.getMessage t) m)
          (throw t)))))

(defn find-csv-files
  "dir should be a string representing a directory.
  Returns a seq on Files, each File being a .csv ."
  [dir]
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

(def yyyy-MM-dd-format (SimpleDateFormat. "yyyy-MM-dd"))

(def supplied-date-pattern (re-pattern "(\\d{4}-\\d{2}-\\d{2})((_)(\\d{4}-\\d{2}-\\d{2}))?"))

(defn dirname-with-max-supplied-date [dir]
  (->> dir
       io/file
       .listFiles
       (filter #(.isDirectory %))
       (map #(.getName %))
       (map #(re-matches supplied-date-pattern %))
       (remove nil?)
       (map first)
       sort
       last))

(defn max-year-from-sparql-query-result [sparql-query-result-as-a-string]
  (->> sparql-query-result-as-a-string
       csv/read-csv
       (drop 1)
       (map first) ;; assume 1st column contains the year
       sort
       last))

(comment

  scotgov-service-url

  (.format yyyy-MM-dd-format (Date.))

  (re-matches supplied-date-pattern "2021-01-31_2020-12-31")
  (re-matches supplied-date-pattern "2021-01-31")
  (re-matches supplied-date-pattern "2021-01-31_")
  (re-matches supplied-date-pattern "2021-01-3")

  (dirname-with-max-supplied-date "tmp/test-dirname-logic")

  
  )