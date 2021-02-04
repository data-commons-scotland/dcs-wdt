(ns dcs.wdt.prototype-4.export.general-use
  (:require [clojure.java.io :as io]
            [clojure.data.csv :as csv]
            [taoensso.timbre :as log]
            [dcs.wdt.prototype-4.dimensions :as dims]))

(def output-dir "data/exporting/general-use/")

(defn generate-csv-files [db]
  (doseq [rtype dims/record-types]
    (let [sub-db (filter #(= rtype (:record-type %)) db)
          headers (keys (first sub-db))
          header-row (map name headers)
          data-rows (map #(map % headers) sub-db)
          file (io/file (str output-dir (name rtype) ".csv"))]
      (log/infof "Writing %s %s records to: %s" (count sub-db) rtype (.getAbsolutePath file))
      (io/make-parents file)
      (with-open [wtr (io/writer file)]
        (csv/write-csv wtr (cons header-row data-rows))))))