(ns dcs.wdt.prototype-4.ingest.waste-site-io
  (:require [clojure.string :as str]
            [clojure.java.io :as io]
            [taoensso.timbre :as log]
            [dcs.wdt.prototype-4.ingest.shared :as shared]))

(def expected-year-totals
  (assoc
    (zipmap (range 2007 2020)                               ;; 2007-2019 each have
            (repeat (* 4 50338)))                           ;; 4 quarters of data
    2020 50338))                                            ;; 2020 has just the 1 quarter of data

(def csv-file-str "data/ingesting/waste-site-io/csv-extract/2007-Q1-onwards.csv")

(def non-year-quarter-keys ["Permit / Licence Number"
                            "Operator Organisation"
                            "Input / Output Table"
                            "EWC Code"
                            "EWC Description"])

(defn split-by-year-quarter
  "Convert the given map with multiple year-quarters to a seq of maps, each with a single year-quarter."
  [m]
  (let [common-m (select-keys m non-year-quarter-keys)
        ;; Remove the non-year-quarter-key entries
        remaining-m (apply (partial dissoc m) non-year-quarter-keys)]
    (for [[k v] remaining-m]
      (merge common-m
             {:year    (subs k 0 4)
              :quarter (subs k 5)
              :tonnes  v}))))

(defn bigdec' [s]
  (bigdec (if (str/blank? s) "0" s)))

(defn customise-map
  "Converts an externally-oriented map to an internally-oriented map."
  [m]
  {:year            (Integer/parseInt (:year m))
   :quarter         (:quarter m)
   :permit          (get m "Permit / Licence Number")
   :operator        (get m "Operator Organisation")
   :io              (get m "Input / Output Table")
   :ewc-code        (get m "EWC Code")
   :ewc-description (get m "EWC Description")
   :tonnes          (bigdec' (:tonnes m))})

(defn csv-file-to-maps
  "Parses a waste-site CSV file
  to return a seq of :waste-site-io maps (internal DB records)."
  [file]
  (let [customise-map (partial shared/customise-map customise-map)]
    (->> file
         (#(do (log/infof "Reading CSV file: %s" (.getAbsolutePath %)) %))
         shared/csv-file-to-maps
         (#(do (log/infof "CSV data rows: %s" (count %)) %))
         (map split-by-year-quarter)
         flatten
         (#(do (log/infof "Candidate records: %s" (count %)) %))
         (map customise-map)
         (remove nil?)
         (#(do (log/infof "Accepted records: %s" (count %)) %)))))

(defn db-from-csv-file []
  (let [db (->> csv-file-str
                io/file
                csv-file-to-maps
                (map #(assoc % :record-type :waste-site-io)))]
    #_(when-let [error (shared/check-year-totals (fn [_] 1) expected-year-totals db)]
      (throw (RuntimeException. (format "waste-site has year-totals error...\nExpected: %s\nActual: %s" (first error) (second error)))))
    db))