(ns dcs.wdt.ingest.waste-site-io
  (:require [clojure.string :as str]
            [clojure.java.io :as io]
            [taoensso.timbre :as log]
            [geocoordinates.core :as geo]
            [dcs.wdt.ingest.shared :as shared]))

(def expected-year-totals {2019 1254})

(def csv-dir "data/ingesting/waste-site-io")

(defn bigdec' [s]
  (bigdec (if (str/blank? s) "0" s)))

(defn parse-slash-separated-list [s]
  (when-not (nil? s)
    (->> (str/split s #"/")
         (map str/trim)
         (remove str/blank?)
         (apply sorted-set))))

(defn customise-map
  "Converts an externally-oriented map to an internally-oriented map."
  [m]
  (let [region (let [v (get m "Local Authority of site")]
                 (get shared/region-aliases v v))]
    (if (contains? shared/regions-set region)
      (let [{:keys [latitude longitude]} (geo/easting-northing->latitude-longitude
                                           {:easting  (Integer/parseInt (get m "Easting"))
                                            :northing (Integer/parseInt (get m "Northing"))}
                                           :national-grid)]
        {:region                   region
         :year                     (Integer/parseInt (get m "Year"))
         :site-name                (get m "Site Name and or Address")
         :permit                   (get m "Permit or Licence Number")
         :status                   (get m "Operational Status of Site")
         :operator                 (get m "Operator Organisation")
         :latitude                 latitude
         :longitude                longitude
         :activities               (parse-slash-separated-list (get m "Waste Site Activity"))
         :accepts                  (parse-slash-separated-list (get m "Waste Type"))
         :tonnes-input             (bigdec' (get m "Waste inputs to site (Table B)"))
         :tonnes-treated-recovered (bigdec' (get m "Waste treated/ recovered on site (Table C)"))
         :tonnes-output            (bigdec' (get m "Waste outputs from site (Table D)"))})
      (do (log/debugf "Ignoring: %s" m)
          nil))))

(defn csv-file-to-maps
  "Parses a waste-site-io CSV file
  to return a seq of :waste-site-io maps (internal DB records)."
  [file]
  (let [customise-map (partial shared/customise-map customise-map)]
    (->> file
         (#(do (log/infof "Reading CSV file: %s" (.getAbsolutePath %)) %))
         shared/csv-file-to-maps
         (#(do (log/infof "CSV data rows: %s" (count %)) %))
         (#(do (log/infof "Candidate records: %s" (count %)) %))
         (map customise-map)
         (remove nil?)
         (#(do (log/infof "Accepted records: %s" (count %)) %)))))

(defn db-from-csv-file []
  (let [db (->> (str csv-dir "/" (shared/dirname-with-max-supplied-date csv-dir) "/extract.csv")
                io/file
                csv-file-to-maps
                (map #(assoc % :record-type :waste-site-io)))]
    (when-let [error (shared/check-year-totals (fn [_] 1) expected-year-totals db)]
      (throw (RuntimeException. (format "waste-site-io has year-totals error...\nExpected: %s\nActual: %s" (first error) (second error)))))
    db))