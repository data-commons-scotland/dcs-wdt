(ns dcs.wdt.export.general-use
  (:require [clojure.java.io :as io]
            [clojure.data.csv :as csv]
            [clojure.data.json :as json]
            [clojure.pprint :as pp]
            [taoensso.timbre :as log]
            [dcs.wdt.dimensions :as dims]
            [dcs.wdt.export.shared :as shared]
            [dcs.wdt.export.general-use-markup :as markup])
  (:import java.io.FileWriter))


(def trunk-dir "data/exporting/general-use/")


(def do-not-csv #{:ace-furniture-count :ace-furniture-avg-weight})
(def do-not-json #{:ace-furniture-count :ace-furniture-avg-weight})


(defn- datasets-metadata [db]
  (sort-by :name (filter #(= :meta (:record-type %)) db)))
  

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


(defn- generate-data-csv-files [db]
  (let [data-dir (str trunk-dir "data/")]
    (doseq [rtype dims/record-types]
      (when (not (contains? do-not-csv rtype))
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
          (csv/write-csv wtr (cons header-row data-rows))))))))


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
        ks        [:dimension :description :record-type :example :count-distincts :min :max]
        data-maps (map #(zipmap ks %) data-rows)]
    (pp/print-table data-maps))
  (pp/print-table (datasets-metadata db)))


(defn- print-tables-of-sample-of-the-data [db]
  (doseq [rtype dims/record-types]
    (let [sub-db (filter #(= rtype (:record-type %)) db)
          sub-db-sampled (repeatedly 5 #(rand-nth sub-db))
          sub-db-sampled-and-stringified (map (fn [record] (zipmap (keys record)
                                                                   (map shared/stringify-if-collection (vals record))))
                                              sub-db-sampled)
          ks (sort-by dims/ord (keys (first sub-db-sampled-and-stringified)))]
      (pp/print-table ks sub-db-sampled-and-stringified))))


(defn generate-data-files [db]
  (generate-data-csv-files db)
  (generate-data-json-files db))

(defn generate-readme-files [db]
  (let [metas (datasets-metadata db)]
    (markup/generate-top-level-readme-file trunk-dir metas)
    (markup/generate-data-level-readme-file trunk-dir metas (set (map name do-not-csv)))))

(defn print-describing-tables [db]
  (print-tables-of-sample-of-the-data db)
  (print-describing-tables-for-the-metadata db))