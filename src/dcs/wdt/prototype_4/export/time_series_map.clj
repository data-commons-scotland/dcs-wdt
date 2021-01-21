(ns dcs.wdt.prototype-4.export.time-series-map
  (:require [clojure.java.io :as io]
            [clojure.data.json :as json]
            [taoensso.timbre :as log]))

(def dir "data/exporting/time-series-map/")
(def file-mgmt (io/file (str dir "hw-mgmt-data.js")))
(def file-co2e (io/file (str dir "hw-co2e-data.js")))

(defn generate-js-files [db]

  (let [population (filter #(= :population (:record-type %)) db)
        household-waste (->> db
                             (filter #(= :household-waste (:record-type %)))
                             ;; calculate the tonnes for each (area, year, end-state) triple
                             (group-by (juxt :area :year :end-state))
                             (map (fn [[[area year end-state] coll]] {:area      area
                                                                      :year      year
                                                                      :end-state end-state
                                                                      :tonnes    (->> coll
                                                                                      (map :tonnes)
                                                                                      (apply +))})))
        household-co2e (filter #(= :household-co2e (:record-type %)) db)

        population-for-lookup (group-by (juxt :area :year) population)
        lookup-population (fn [area year] (-> population-for-lookup (get [area year]) first :population))]

    (let [output-dataset (map (fn [{:keys [area year end-state tonnes]}] {:area     area
                                                                          :year     year
                                                                          :endState end-state
                                                                          :tonnes   (double (/ tonnes (lookup-population area year)))})
                              household-waste)
          min-year (->> output-dataset
                        (map :year)
                        (apply min))
          max-year (->> output-dataset
                        (map :year)
                        (apply max))
          max-tonnes-per-year (->> output-dataset
                                   (group-by (juxt :area :year))
                                   vals
                                   (map (fn [vec-of-3] (->> vec-of-3 ;; i.e. the 3 end-states
                                                            (map :tonnes)
                                                            (apply +))))
                                   (apply max))]

      (let [content (str "/* Generated  at " (java.time.LocalDateTime/now) " from SEPA, NRS and Scottish Government data. */\n"
                         "const records = " (json/write-str output-dataset) ";\n"
                         "const minYear = " min-year ";\n"
                         "const maxYear = " max-year ";\n"
                         "const maxTonnesPerYear = " max-tonnes-per-year ";\n"
                         "export { records, minYear, maxYear, maxTonnesPerYear };")]
        (io/make-parents file-mgmt)
        (spit file-mgmt content)
        (log/infof "Wrote: %s" (.getAbsolutePath file-mgmt))))



    (let [output-dataset (map (fn [{:keys [area year end-state tonnes]}] {:area   area
                                                                          :year   year
                                                                          :tonnes (double (/ tonnes (lookup-population area year)))})
                              household-co2e)
          min-year (->> output-dataset
                        (map :year)
                        (apply min))
          max-year (->> output-dataset
                        (map :year)
                        (apply max))
          max-tonnes-per-year (->> output-dataset
                                   (map :tonnes)
                                   (apply max))]

      (let [content (str "/* Generated  at " (java.time.LocalDateTime/now) " from SEPA, NRS and Scottish Government data. */\n"
                         "const records = " (json/write-str output-dataset) ";\n"
                         "const minYear = " min-year ";\n"
                         "const maxYear = " max-year ";\n"
                         "const maxTonnesPerYear = " max-tonnes-per-year ";\n"
                         "export { records, minYear, maxYear, maxTonnesPerYear };")]
        (io/make-parents file-co2e)
        (spit file-co2e content)
        (log/infof "Wrote: %s" (.getAbsolutePath file-co2e))))))



