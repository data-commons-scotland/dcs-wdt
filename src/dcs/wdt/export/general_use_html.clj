(ns dcs.wdt.export.general-use-html
  (:require [clojure.string :as str]
            [clojure.java.io :as io]
            [taoensso.timbre :as log]))


(defn generate-readme-file [do-not-json trunk-dir datasets-metadata dimensions-metadata]
  (let [content-template "
== _Easier_ open data about waste in Scotland

=== Objective

Several organisations are doing a very good job of curating & publishing _open data_ about waste in Scotland but,
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

=== The DATASETS-COUNT _easier_ datasets

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

DATASETS-ROWS

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

DIMENSIONS-ROWS

|=========================================================

(The link:metadata/dimensions.csv[CSV version of the table] above.)
"

        datasets-count (str (count datasets-metadata))
        datasets-str (str/join "\n\n"
                               (map (fn [columns] (format
                                                    "| anchor:%s[] %s | %s | link:data/%s.csv[CSV]%s | %s | %s | %s^&nbsp;%s[URL]^ | %s[%s]"
                                                    (nth columns 0) (nth columns 0)
                                                    (nth columns 1)
                                                    (nth columns 0) (let [s (nth columns 0)] (if (contains? do-not-json (keyword s)) "" (str " link:data/" s ".json[JSON]")))
                                                    (nth columns 2)
                                                    (nth columns 3)
                                                    (nth columns 5) (nth columns 6)
                                                    (nth columns 8) (nth columns 7)))
                                    datasets-metadata))
        dimensions-str (str/join "\n\n"
                                 (flatten
                                   (for [rows (partition-by first dimensions-metadata)] ;; partition by dimension value
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
                    (str/replace "DATASETS-COUNT" datasets-count)
                    (str/replace "DATASETS-ROWS" datasets-str)
                    (str/replace "DIMENSIONS-ROWS" dimensions-str))
        file (io/file (str trunk-dir "README.adoc"))]
    (log/infof "Writing: %s" (.getAbsolutePath file))
    (io/make-parents file)
    (spit file content)))


(defn generate-webapp-file [do-not-json trunk-dir datasets-metadata dimensions-metadata]
  (let [datasets-str (str/join "\n\n"
                               (map (fn [columns] (format
                                                    "(dataset-row \"%s\" \"%s\" \"%s\" \"%s.csv\" %s \"%s\" \"%s\" \"%s\" \"%s\" \"%s\" \"%s\")"
                                                    (nth columns 0) (nth columns 0)
                                                    (nth columns 1)
                                                    (nth columns 0) (let [s (nth columns 0)] (if (contains? do-not-json (keyword s))
                                                                                               "nil"
                                                                                               (str "\"" s ".json\"")))
                                                    (nth columns 2)
                                                    (nth columns 3)
                                                    (nth columns 5) (nth columns 6)
                                                    (nth columns 8) (nth columns 7)))
                                    datasets-metadata))
        dimensions-str (str/join "\n\n"
                                 (flatten
                                   (for [rows (partition-by first dimensions-metadata)] ;; partition by dimension value
                                     (cons
                                       (let [columns (first rows)] ;; 1st row for the particular dimension value
                                         (format
                                           "(dimension-row \"%s\" \"%s\" \"%s\" \"%s\" \"%s\" \"%s\" \"%s\" \"%s\" \"%s\" \"%s\")"
                                           (count rows) (nth columns 0) (count rows) (nth columns 1) (nth columns 2) (nth columns 2) (nth columns 3) (if-let [v (nth columns 4)] v "") (if-let [v (nth columns 5)] v "") (if-let [v (nth columns 6)] v "")))
                                       (for [columns (rest rows)] ;; the remaining rows for the particular dimension value
                                         (format
                                           "(dimension-row \"%s\" \"%s\" \"%s\" \"%s\" \"%s\" \"%s\")"
                                           (nth columns 2) (nth columns 2) (nth columns 3) (if-let [v (nth columns 4)] v "") (if-let [v (nth columns 5)] v "") (if-let [v (nth columns 6)] v "")))))))
        content (-> ";; -------- datasets ---------\n\n"
                    (str datasets-str)
                    (str "\n\n;; -------- dimensions ---------\n\n")
                    (str dimensions-str))
        file (io/file (str trunk-dir "webapp-fragment.clj"))]
    (log/infof "Writing: %s" (.getAbsolutePath file))
    (io/make-parents file)
    (spit file content)))