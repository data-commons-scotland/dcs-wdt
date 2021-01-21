(ns dcs.wdt.prototype-4.export.time-series-map
  (:require [clojure.java.io :as io]
            [clojure.data.json :as json]
            [taoensso.timbre :as log]))

(def output-dir "data/exporting/time-series-map/")

(defn generate-js-file-for-household-waste [lookup-population db file]
  (let [household-waste (->> db
                             (filter #(= :household-waste (:record-type %)))
                             ;; calculate the tonnes for each (area, year, end-state) triple
                             (group-by (juxt :area :year :end-state))
                             (map (fn [[[area year end-state] coll]] {:area      area
                                                                      :year      year
                                                                      :end-state end-state
                                                                      :tonnes    (->> coll
                                                                                      (map :tonnes)
                                                                                      (apply +))})))
        output-records (map (fn [{:keys [area year end-state tonnes]}] {:area     area
                                                                        :year     year
                                                                        :endState end-state
                                                                        :tonnes   (double (/ tonnes (lookup-population area year)))})
                            household-waste)
        min-year (->> output-records
                      (map :year)
                      (apply min))
        max-year (->> output-records
                      (map :year)
                      (apply max))
        max-tonnes-per-year (->> output-records
                                 (group-by (juxt :area :year))
                                 vals
                                 (map (fn [vec-of-3] (->> vec-of-3 ;; i.e. the 3 end-states
                                                          (map :tonnes)
                                                          (apply +))))
                                 (apply max))
        content (str "/* Generated  at " (java.time.LocalDateTime/now) " from SEPA, NRS and Scottish Government data. */\n"
                     "const records = " (json/write-str output-records) ";\n"
                     "const minYear = " min-year ";\n"
                     "const maxYear = " max-year ";\n"
                     "const maxTonnesPerYear = " max-tonnes-per-year ";\n"
                     "export { records, minYear, maxYear, maxTonnesPerYear };")]
    (io/make-parents file)
    (spit file content)
    (log/infof "Wrote: %s" (.getAbsolutePath file))))

(defn generate-js-file-for-household-co2e [lookup-population db file]
  (let [household-co2e (filter #(= :household-co2e (:record-type %)) db)
        output-records (map (fn [{:keys [area year end-state tonnes]}] {:area   area
                                                                        :year   year
                                                                        :tonnes (double (/ tonnes (lookup-population area year)))})
                            household-co2e)
        min-year (->> output-records
                      (map :year)
                      (apply min))
        max-year (->> output-records
                      (map :year)
                      (apply max))
        max-tonnes-per-year (->> output-records
                                 (map :tonnes)
                                 (apply max))
        content (str "/* Generated  at " (java.time.LocalDateTime/now) " from SEPA, NRS and Scottish Government data. */\n"
                     "const records = " (json/write-str output-records) ";\n"
                     "const minYear = " min-year ";\n"
                     "const maxYear = " max-year ";\n"
                     "const maxTonnesPerYear = " max-tonnes-per-year ";\n"
                     "export { records, minYear, maxYear, maxTonnesPerYear };")]
    (io/make-parents file)
    (spit file content)
    (log/infof "Wrote: %s" (.getAbsolutePath file))))

(defn generate-js-files [db]
  (let [population (filter #(= :population (:record-type %)) db)
        population-for-lookup (group-by (juxt :area :year) population)
        lookup-population (fn [area year] (-> population-for-lookup (get [area year]) first :population))]
    (generate-js-file-for-household-waste lookup-population db (io/file (str output-dir "hw-mgmt-data.js")))
    (generate-js-file-for-household-co2e lookup-population db (io/file (str output-dir "hw-co2e-data.js")))))