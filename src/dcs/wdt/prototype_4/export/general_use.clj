(ns dcs.wdt.prototype-4.export.general-use
  (:require [clojure.string :as str]
            [clojure.java.io :as io]
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
                           (name rtype) (dim record)
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
            data-rows (map #(map % headers) sub-db)
            file (io/file (str data-dir (name rtype) ".csv"))]
        (log/infof "Writing %s %s records to: %s" (count data-rows) rtype (.getAbsolutePath file))
        (io/make-parents file)
        (with-open [wtr (io/writer file)]
          (csv/write-csv wtr (cons header-row data-rows)))))))


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
          ks (sort-by dims/ord (keys (first sub-db-sampled)))]
      (pp/print-table ks sub-db-sampled))))


(defn- generate-readme-file [db]
  (let [content-template "
== _Easier_ open data about waste in Scotland

=== Objective

Several organisations are doing a good job of curating & publishing _open data_ about waste in Scotland but,
the published data is not always \"easy to use\" for non-experts.
We have see several references to this at open data conference events and on social media platforms:
[quote]
Whilst statisticians/coders may think that it is reasonably simple to knead together these
somewhat diverse datasets into a coherent knowledge, the interested layman doesn't find it so easy.

One of the objectives of the Data Commons Scotland project is to address
the \"ease of use\" issue over open data.
The contents of this repository are the result of us _re-working_ some of the existing
_source_ open data
so that it is *_easier_* to use, understand, consume, parse, and all in one place.
It may not be as detailed or have all the nuances as the source data - but aims to be
better for the purposes of making the information accessible to non-experts.

We have processed the source data just enough to:

* provide value-based cross-referencing between datasets
* add a few fields whose values are generally useful but not easily derivable by a simple calculation (such as latitude & longitude)
* make it available as simple CSV and JSON files in a Git repository.

We have not augmented the data with derived values that can be simply calculated,
such as per-population amounts, averages, trends, totals, etc.

=== The _easier_ datasets

[width=\"100%\",cols=\"<,<,<,>,<,<,<\"]

|=========================================================

4+^h|dataset ^(generated&nbsp;February&nbsp;2021)^
3+^h|source data ^(sourced&nbsp;January&nbsp;2021)^

1+<h| name
1+<h| description
1+<h| file
1+<h| number of records
1+<h| creator
1+<h| supplier
1+<h| licence

datasets-str

|=========================================================

(The fuller, link:metadata/datasets.csv[CSV version of the table] above.)

=== The dimensions of the _easier_ datasets

One of the things that makes these datasets _easier_ to use,
is that they use consistent dimensions values/controlled code-lists.
This makes it easier to join/link datasets.

So we have tried to rectify the inconsistencies that occur in the source data
(in particular, the inconsistent labelling of waste materials and regions).
However, this is still \"work-in-progress\" and we yet to tease out & make consistent further useful dimensions.

[width=\"100%\",cols=\"7\",options=\"header\"]

|=========================================================

| dimension
| description
| dataset
| example value of dimension
| count of values of dimension
| min value of dimension
| max value of dimension

dimensions-str

|=========================================================

(The link:metadata/dimensions.csv[CSV version of the table] above.)
"

        datasets-str (str/join "\n\n"
                               (map (fn [columns] (format
                                                    "| anchor:%s[] %s | %s | link:data/%s.csv[CSV] | %s | %s | %s^&nbsp;%s[URL]^ | %s[%s]"
                                                    (nth columns 0) (nth columns 0) (nth columns 1) (nth columns 0) (nth columns 2) (nth columns 3) (nth columns 5) (nth columns 6) (nth columns 8) (nth columns 7)))
                                    (datasets-metadata db)))
        dimensions-str (str/join "\n\n"
                                 (flatten
                                   (for [rows (partition-by first (dimensions-metadata db))] ;; partition by dimension value
                                     (cons
                                       (let [columns (first rows)] ;; 1st row for the particular dimension value
                                         (format
                                           ".%s+| %s .%s+| %s | xref:%s[%s] | %s | %s | %s | %s"
                                           (count rows) (nth columns 0) (count rows) (nth columns 1) (nth columns 2) (nth columns 2) (nth columns 3) (if-let [v (nth columns 4)] v "") (if-let [v (nth columns 5)] v "") (if-let [v (nth columns 6)] v "")))
                                       (for [columns (rest rows)] ;; the remaining rows for the particular dimension value
                                         (format
                                           "| xref:%s[%s] | %s | %s | %s | %s"
                                           (nth columns 2) (nth columns 2) (nth columns 3) (if-let [v (nth columns 4)] v "") (if-let [v (nth columns 5)] v "") (if-let [v (nth columns 6)] v "")))))))
        content (-> content-template
                    (str/replace "datasets-str" datasets-str)
                    (str/replace "dimensions-str" dimensions-str))
        file (io/file (str trunk-dir "README.adoc"))]
    (log/infof "Writing: %s" (.getAbsolutePath file))
    (io/make-parents file)
    (spit file content)))


(defn generate-csv-files [db]
  (generate-data-csv-files db)
  (generate-metadata-csv-files db)
  (generate-readme-file db))

(defn print-describing-tables [db]
  (print-tables-of-sample-of-the-data db)
  (print-describing-tables-for-the-metadata db))