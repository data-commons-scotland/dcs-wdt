(ns dcs.wdt.export.general-use-markup
  (:require [clojure.string :as str]
            [clojure.java.io :as io]
            [taoensso.timbre :as log]))


(defn generate-top-level-readme-file [trunk-dir metas]
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
                          
=== Approach

We have processed the source data just enough to:

* provide intuitive value-based cross-referencing between datasets
* add a few fields whose values are generally useful but not easily derivable by a simple calculation (such as latitude & longitude)
* make it available as simple CSV and JSON files in a Git repository.

We have not augmented the data with derived values that can be simply calculated,
such as per-population amounts, averages, trends, totals, etc.

We have tried to rectify the inconsistencies that occur in the source data 
(in particular, the inconsistent labelling of waste materials and regions). 
However, this is still \"work-in-progress\" and we yet to tease out & make consistent further 
useful dimensions.

==== The DATASETS-COUNT _easier_ datasets

[width=\"100%\",cols=\"<,<,^,<,<,<,<\",stripes=\"hover\"]

|=========================================================

4+^h|dataset ^(generated&nbsp;September&nbsp;2021)^
3+^h|source data ^(sourced&nbsp;January&nbsp;2021)^

1+<h| name
1+<h| description
1+<h| rows x cols
1+<h| data files
1+<h| creator
1+<h| supplier
1+<h| licence

DATASETS-ROWS

|=========================================================

"

        datasets-count (str (count metas))
        datasets-str (str/join "\n\n"
                               (for [meta metas]
                                 (format "| anchor:%s[] %s | %s |  %s x %s | link:data/README#%s[files] | %s | %s[%s] | %s[%s]"
                                         (:name meta) (:name meta)
                                         (:description meta)
                                         (:record-count meta) (:attribute-count meta)
                                         (:name meta)
                                         (:creator meta)
                                         (:supply-url meta) (:supplier meta)
                                         (:licence-url meta) (:licence meta))))
        content (-> content-template
                    (str/replace "DATASETS-COUNT" datasets-count)
                    (str/replace "DATASETS-ROWS" datasets-str))
        file (io/file (str trunk-dir "README.adoc"))]
    (log/infof "Writing: %s" (.getAbsolutePath file))
    (io/make-parents file)
    (spit file content)))




(defn generate-data-level-readme-file [trunk-dir metas]
  (let [content-template "
== _Easier_ open data about waste in Scotland
                          
==== The data files

[width=\"100%\",cols=\"<,<,<,<,<\",stripes=\"hover\"]

|=========================================================

1.2+^h|name
3.1+^h|the actual data +
(in various formats)
1.2+^h|the data's specification +
(in CSVW format)


1+^h| CSV +
(table oriented format)
1+^h| JSON +
(Javascript oriented format)
1+^h| Turtle +
(linked data format)

DATASETS-ROWS

|=========================================================

"
        datasets-str (str/join "\n\n"
                               (for [meta metas]
                                (format "| anchor:%s[] %s | link:%s.csv[%s.csv] | link:%s.json[%s.json] | link:%s.ttl[%s.ttl] | link:%s-metadata.json[%s-metadata.json]"
                                        (:name meta) (:name meta)
                                        (:name meta) (:name meta)
                                        (:name meta) (:name meta)
                                        (:name meta) (:name meta)
                                        (:name meta) (:name meta))))
        content (-> content-template
                    (str/replace "DATASETS-ROWS" datasets-str))
        file (io/file (str trunk-dir "data/README.adoc"))]
    (log/infof "Writing: %s" (.getAbsolutePath file))
    (io/make-parents file)
    (spit file content)))