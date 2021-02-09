(ns dcs.wdt.prototype-4.export.general-use
  (:require [clojure.java.io :as io]
            [clojure.data.csv :as csv]
            [clojure.pprint :as pp]
            [taoensso.timbre :as log]
            [dcs.wdt.prototype-4.dimensions :as dims]
            [dcs.wdt.prototype-4.ingest.meta :as meta]))


(def trunk-dir "data/exporting/general-use/")


(defn- datasets-metadata [db]
  (for [rtype dims/record-types]
    (let [n (count (filter #(= rtype (:record-type %)) db))
          source (rtype meta/sources)]
      [(name rtype) n
       (:creator source) (:created-when source)
       (:supplier source) (:supply-url source)
       (:licence source ) (:notes source)])))

(defn- dimensions-metadata [db]
  (apply concat                                             ;; flatten one level
         (for [rtype dims/record-types]
           (let [sub-db (filter #(= rtype (:record-type %)) db)
                 record (first sub-db)
                 dims (sort-by dims/ord (keys (dissoc record :record-type)))]
             (for [dim dims]
               (let [dim-vals (distinct (map dim sub-db))]
                 [(name rtype) (name dim) (dim record)
                  (when (dims/count-useful? dim) (count dim-vals))
                  (when (dims/min-max-useful? dim) (apply min dim-vals)) (when (dims/min-max-useful? dim) (apply max dim-vals))]))))))


(defn- generate-metadata-csv-files [db]
  (let [metadata-dir (str trunk-dir "metadata/")]
    (let [data-rows (datasets-metadata db)
          header-row ["dataset" "number of records"
                      "creator of source data" "creation date of source data"
                      "supplier of source data" "supply URL of source data"
                      "licence of source data" "sourcing notes"]
          file (io/file (str metadata-dir "datasets.csv"))]
      (log/infof "Writing %s records to: %s" (count data-rows) (.getAbsolutePath file))
      (io/make-parents file)
      (with-open [wtr (io/writer file)]
        (csv/write-csv wtr (cons header-row data-rows))))
    (let [data-rows (dimensions-metadata db)
          header-row ["dataset" "dimension" "example value of dimension"
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
            data-rows (map #(map % headers) sub-db)
            file (io/file (str data-dir (name rtype) ".csv"))]
        (log/infof "Writing %s %s records to: %s" (count data-rows) rtype (.getAbsolutePath file))
        (io/make-parents file)
        (with-open [wtr (io/writer file)]
          (csv/write-csv wtr (cons header-row data-rows)))))))


(defn- print-describing-tables-for-the-metadata [db]
  (let [data-rows (dimensions-metadata db)
        ks [:record-type :dimension :example :count-distincts :min :max]
        data-maps (map #(zipmap ks %) data-rows)]
    (pp/print-table data-maps))
  (let [data-rows (datasets-metadata db)
        ks [:record-type :record-count :creator :created-when :supplier :supply-url :licence :notes]
        data-maps (map #(zipmap ks %) data-rows)]
    (pp/print-table data-maps)))


(defn- print-tables-of-sample-of-the-data [db]
  (doseq [rtype dims/record-types]
    (let [sub-db (filter #(= rtype (:record-type %)) db)
          sub-db-sampled (repeatedly 5 #(rand-nth sub-db))
          ks (sort-by dims/ord (keys (first sub-db-sampled)))]
      (pp/print-table ks sub-db-sampled))))


(defn generate-csv-files [db]
  (generate-data-csv-files db)
  (generate-metadata-csv-files db))

(defn print-describing-tables [db]
  (print-tables-of-sample-of-the-data db)
  (print-describing-tables-for-the-metadata db))
