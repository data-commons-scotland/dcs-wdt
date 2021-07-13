(ns dcs.wdt.export.general-use
  (:require [clojure.java.io :as io]
            [clojure.data.csv :as csv]
            [clojure.data.json :as json]
            [clojure.pprint :as pp]
            [taoensso.timbre :as log]
            [dcs.wdt.dimensions :as dims]
            [dcs.wdt.ingest.meta :as meta]
            [dcs.wdt.export.shared :as shared]
            [dcs.wdt.export.general-use-html :as html])
  (:import java.io.FileWriter))


(def trunk-dir "data/exporting/general-use/")


(def do-not-json #{:waste-site-io})


(defn- datasets-metadata [db]
  (for [rtype dims/record-types]
    (let [n (count (filter #(= rtype (:record-type %)) db))
          source (rtype meta/sources)]
      [(name rtype) (:description source) n
       (:creator source) (:created-when source)
       (:supplier source) (:supply-url source)
       (:licence source) (:licence-url source)
       (:notes source)])))

(defn- dimensions-metadata [db]
  (sort-by (comp dims/ord keyword first)                    ;; sort by dimension (with the ordering defined by ord)
           (apply concat                                    ;; flatten one level
                  (for [rtype dims/record-types]
                    (let [sub-db (filter #(= rtype (:record-type %)) db)
                          record (first sub-db)
                          dims (sort-by dims/ord (keys (dissoc record :record-type)))]
                      (for [dim dims]
                        (let [dim-vals (sort-by dims/ord (distinct (map dim sub-db)))]
                          [(name dim) (dim dims/descriptions)
                           (name rtype) (shared/stringify-if-collection (dim record))
                           (when (dims/count-useful? dim) (count dim-vals))
                           (when (dims/min-max-useful? dim) (apply min dim-vals)) (when (dims/min-max-useful? dim) (apply max dim-vals))])))))))


(defn- generate-metadata-csv-files [db]
  (let [metadata-dir (str trunk-dir "metadata/")]
    (let [data-rows (datasets-metadata db)
          header-row ["dataset" "description" "number of records"
                      "creator of source data" "creation date of source data"
                      "supplier of source data" "supply URL of source data"
                      "licence of source data" "licence URL of source data"
                      "sourcing notes"]
          file (io/file (str metadata-dir "datasets.csv"))]
      (log/infof "Writing %s records to: %s" (count data-rows) (.getAbsolutePath file))
      (io/make-parents file)
      (with-open [wtr (io/writer file)]
        (csv/write-csv wtr (cons header-row data-rows))))
    (let [data-rows (dimensions-metadata db)
          header-row ["dimension" "description" "dataset" "example value of dimension"
                      "count of values of dimension, when useful"
                      "min value of dimension, when useful" "max value of dimension, when useful"]
          file (io/file (str metadata-dir "dimensions.csv"))]
      (log/infof "Writing %s records to: %s" (count data-rows) (.getAbsolutePath file))
      (io/make-parents file)
      (with-open [wtr (io/writer file)]
        (csv/write-csv wtr (cons header-row data-rows))))))


(defn- generate-data-csv-files [db]
  (let [data-dir (str trunk-dir "data/")]
    (doseq [rtype dims/record-types]
      (let [sub-db (filter #(= rtype (:record-type %)) db)
            headers (sort-by dims/ord (keys (dissoc (first sub-db) :record-type)))
            header-row (map name headers)
            data-rows (->> sub-db
                           (map (fn [record] (map record headers))) ;; Get the values as specified by 'headers'
                           (map (fn [values] (map shared/stringify-if-collection values))))
            file (io/file (str data-dir (name rtype) ".csv"))]
        (log/infof "Writing %s %s records to: %s" (count data-rows) rtype (.getAbsolutePath file))
        (io/make-parents file)
        (with-open [wtr (io/writer file)]
          (csv/write-csv wtr (cons header-row data-rows)))))))


(defn- generate-data-json-files [db]
  (let [data-dir (str trunk-dir "data/")]
    (doseq [rtype dims/record-types]
      (when (not (contains? do-not-json rtype))
        (let [sub-db (->> db
                          (filter #(= rtype (:record-type %)))
                          (map #(dissoc % :record-type)))
              file (io/file (str data-dir (name rtype) ".json"))]
          (log/infof "Writing %s %s records to: %s" (count sub-db) rtype (.getAbsolutePath file))
          (io/make-parents file)
          (binding [*out* (FileWriter. file)]
            (json/pprint sub-db)))))))


(defn- print-describing-tables-for-the-metadata [db]
  (let [data-rows (dimensions-metadata db)
        ks [:dimension :description :record-type :example :count-distincts :min :max]
        data-maps (map #(zipmap ks %) data-rows)]
    (pp/print-table data-maps))
  (let [data-rows (datasets-metadata db)
        ks [:record-type :description :record-count :creator :created-when :supplier :supply-url :licence :licence-url :notes]
        data-maps (map #(zipmap ks %) data-rows)]
    (pp/print-table data-maps)))


(defn- print-tables-of-sample-of-the-data [db]
  (doseq [rtype dims/record-types]
    (let [sub-db (filter #(= rtype (:record-type %)) db)
          sub-db-sampled (repeatedly 5 #(rand-nth sub-db))
          sub-db-sampled-and-stringified (map (fn [record] (zipmap (keys record)
                                                                   (map shared/stringify-if-collection (vals record))))
                                              sub-db-sampled)
          ks (sort-by dims/ord (keys (first sub-db-sampled-and-stringified)))]
      (pp/print-table ks sub-db-sampled-and-stringified))))


(defn generate-csv-files [db]
  (generate-data-csv-files db)
  (generate-data-json-files db)
  (generate-metadata-csv-files db)
  (html/generate-readme-file do-not-json trunk-dir (datasets-metadata db) (dimensions-metadata db))
  (html/generate-webapp-file do-not-json trunk-dir (datasets-metadata db) (dimensions-metadata db)))

(defn print-describing-tables [db]
  (print-tables-of-sample-of-the-data db)
  (print-describing-tables-for-the-metadata db))