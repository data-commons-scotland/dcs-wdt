(ns dcs.wdt.ingest.ewc-code
  (:require [clojure.string :as str]
            [clojure.java.io :as io]
            [clojure.pprint :as pp]
            [taoensso.timbre :as log]
            [dcs.wdt.ingest.shared :as shared]))

(def expected-ewc-codes-count 973)

(def txt-dir "data/ingesting/ewc-code")

(defn customise-list
  "Converts an externally-oriented list to an internally-oriented map."
  [[code-line description-line]]
  {:ewc-code code-line
   :ewc-description description-line})

(defn txt-file-to-maps
  "Parses an EWC TXT file
  to return a seq of :ewc-code maps (internal DB records)."
  [file]
  (->> file
       (#(do (log/infof "Reading TXT file: %s" (.getAbsolutePath %)) %))
       slurp
       (#(str/split % #"\r?\n" -1))
       (#(do (log/infof "TXT lines: %s" (count %)) %))
       (drop-while #(not= "01" %))                          ;; The first data line of the nn-only codes section
       (drop 1)
       (drop-while #(not= "01" %))                          ;; The first data line of the section that we're interested in
       (take-while #(not (str/starts-with? % "(1)")))       ;; The first non-data line after the section that we're interested in
       (remove str/blank?)
       (#(do (log/infof "Candidate TXT lines: %s" (count %)) %))
       (partition 2)
       (#(do (log/infof "Candidate records: %s" (count %)) %))
       (map customise-list)
       (#(do (log/infof "Accepted records: %s" (count %)) %))))

(defn db-from-txt-file []
  (let [db (->> (str txt-dir "/" (shared/dirname-with-max-supplied-date txt-dir) "/extract.txt")
                io/file
                txt-file-to-maps
                (map #(assoc % :record-type :ewc-code)))]
    (let [x (->> db
                 (group-by :ewc-code)
                 (filter (fn [[_k v]] (> (count v) 1))))]
      (pp/pprint x))
    (let [actual-ewc-codes-count (->> db (map :ewc-code) distinct count)]
      (when (not= expected-ewc-codes-count actual-ewc-codes-count)
        (throw (RuntimeException. (format "ewc-code has a count error...\nExpected: %s\nActual: %s" expected-ewc-codes-count actual-ewc-codes-count)))))
    db))