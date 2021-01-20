(ns dcs.wdt.business
  (:require [clojure.pprint :as pp]
            [clojure.string :as str]
            [clojure.data :as data]
            [clojure.data.json :as json]
            [clojure.data.csv :as csv]
            [clojure.java.io :as io]
            [taoensso.timbre :as log]
            [dcs.wdt.misc :as misc])
  (:import java.io.PushbackReader))

(def areas ["Aberdeen City"
            "Aberdeenshire"
            "Angus"
            "Argyll and Bute"
            "City of Edinburgh"
            "Clackmannanshire"
            "Dumfries and Galloway"
            "Dundee City"
            "East Ayrshire"
            "East Dunbartonshire"
            "East Lothian"
            "East Renfrewshire"
            "Falkirk"
            "Fife"
            "Glasgow City"
            "Highland"
            "Inverclyde"
            "Midlothian"
            "Moray"
            "North Ayrshire"
            "North Lanarkshire"
            "Orkney Islands"
            "Outer Hebrides"
            "Perth and Kinross"
            "Renfrewshire"
            "Scottish Borders"
            "Shetland Islands"
            "South Ayrshire"
            "South Lanarkshire"
            "Stirling"
            "West Dunbartonshire"
            "West Lothian"
            ;; Special values
            "Offshore"
            "Unknown"
            "Total"])
(def areas-set (set areas))

(def waste-categories ["Acid, alkaline or saline wastes"
                       "Animal and mixed food waste"
                       "Animal faeces, urine and manure"
                       "Batteries and accumulators wastes"
                       "Chemical wastes"
                       "Combustion wastes"
                       "Common sludges"
                       "Discarded equipment (excluding discarded vehicles, batteries and accumulators wastes)"
                       "Discarded vehicles"
                       "Dredging spoils"
                       "Glass wastes"
                       "Health care and biological wastes"
                       "Household and similar wastes"
                       "Industrial effluent sludges"
                       "Metallic wastes, ferrous"
                       "Metallic wastes, mixed ferrous and non-ferrous"
                       "Metallic wastes, non-ferrous"
                       "Mineral waste from construction and demolition"
                       "Mineral wastes from waste treatment and stabilised wastes"
                       "Mixed and undifferentiated materials"
                       "Other mineral wastes"
                       "Paper and cardboard wastes"
                       "Plastic wastes"
                       "Rubber wastes"
                       "Sludges and liquid wastes from waste treatment"
                       "Soils"
                       "Sorting residues"
                       "Spent solvents"
                       "Textile wastes"
                       "Used oils"
                       "Vegetal wastes"
                       "Waste containing PCB"
                       "Wood wastes"
                       ;; Special values
                       "Total"])
(def waste-categories-set (set waste-categories))

(def years (range 2011 2019))
(def years-set (set years))

(def area-aliases {"Aberdeen"           "Aberdeen City"
                   "Dundee"             "Dundee City"
                   "Edinburgh"          "City of Edinburgh"
                   "Eilean Siar"        "Outer Hebrides"
                   "Glasgow"            "Glasgow City"
                   "Na h-Eileanan Siar" "Outer Hebrides"
                   "Orkney"             "Orkney Islands"})

(def waste-category-aliases {"Discarded equipment (excl discarded vehicles, batteries and accumulators)" "Discarded equipment (excluding discarded vehicles, batteries and accumulators wastes)"})

(defn stats [db]
  (let []
    {:record-count (count db)
     :waste-category-count (count (distinct (map :waste-category db)))
     :area-count (count (distinct (map :area db)))
     :year-count (count (distinct (map :year db)))}))

(defn check-values [m]
  (doseq [[expected kw] [[areas-set :area]
                         [years-set :year]
                         [waste-categories-set :waste-category]]]
    (when (not (contains? expected (kw m)))
      (log/warnf "Bad %s value in: %s" (name kw) m))))

(def waste-category-column-label "waste-category") ;; Expected to have been edited into each CSV extract

(defn split-by-area [m]
  (let [waste-category (get m waste-category-column-label)
        ;; Remove the waste-category entry and any blank-keyed entry that might have been created because of blank columns in the CSV
        remaining-m (dissoc m waste-category-column-label "")]
    (for [[k v] remaining-m]
      {:waste-category waste-category
       :area k
       :tonnes v})))

(defn tidy-up-values [m]
  (assoc m
    :waste-category (let [v (-> m
                                :waste-category
                                str/trim
                                (str/replace "*" ""))]
                      (get waste-category-aliases v v))
    :area (let [v (-> m
                      :area
                      str/trim
                      (str/replace "*" ""))]
            (get area-aliases v v))
    :tonnes (let [v (-> m
                        :tonnes
                        str/trim
                        (str/replace "," ""))]
              (if (= "-" v)
                0
                (Integer/parseInt v)))))

(defn tidy-up-values-with-error-logging [m]
  (try
    (tidy-up-values m)
    (catch Throwable t
      (do (log/errorf "%s - Bad value in: %s" (.getMessage t) m)
          (throw t)))))

(defn rows-to-maps [rows]
  (map zipmap
       (repeat (first rows))
       (rest rows)))

(defn skip-byte-order-marker [reader]
  (let [pbr (PushbackReader. reader)
        c (.read pbr)]
    (when (not= 65279 c)
      (.unread pbr c)))
  reader)

(defn csv-file-to-maps [file]
  (let [year (Integer/parseInt (re-find #"\d{4}" (.getName file)))]
    (->> file
         io/reader
         skip-byte-order-marker
         csv/read-csv
         rows-to-maps
         (map split-by-area)
         flatten
         (map tidy-up-values-with-error-logging)
         (map #(assoc % :year year)))))

(defn find-csv-files []
  (->> "src/business-generated/csv-extracts/by-area/"
       io/file
       file-seq
       (filter #(.isFile %))))

(defn gen [_]
  (let [db (->> (find-csv-files)
                (map csv-file-to-maps)
                flatten
                (map #(assoc % :name "business-generated (by area)")))]
    (pp/pprint (stats db))))

