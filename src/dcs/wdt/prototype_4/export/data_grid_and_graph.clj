(ns dcs.wdt.prototype-4.export.data_grid_and_graph
  (:require [clojure.java.io :as io]
            [clojure.data.json :as json]
            [taoensso.timbre :as log]
            [dcs.wdt.prototype-4.dimensions :as dims])
  (:import java.io.FileWriter))


(def filename "data/exporting/data-grid-and-graph/dx-data.json")


(defn generate-json-file [db]
  (let [ignored-areas #{"Offshore" "Unknown"}

        household-waste (->> db
                             (remove #(contains? ignored-areas (:area %)))
                             (filter #(= :household-waste (:record-type %)))
                             ;; Calculate the tonnes roll-up for each (area, year, waste-category) triple
                             (group-by (juxt :area :year :waste-category))
                             (map (fn [[[area year waste-category] coll]] {:record-type    :household-waste
                                                                           :area           area
                                                                           :year           year
                                                                           :waste-category waste-category
                                                                           :tonnes         (->> coll
                                                                                                (map :tonnes)
                                                                                                (apply +))})))
        household-waste-areas-count (count (distinct (map :area household-waste)))
        household-waste-averages (->> household-waste
                                      (group-by (juxt :year :waste-category))
                                      (map (fn [[[year waste-category] coll]] {:record-type    :household-waste
                                                                               :area           "average"
                                                                               :year           year
                                                                               :waste-category waste-category
                                                                               :tonnes         (double (/ (->> coll
                                                                                                               (map :tonnes)
                                                                                                               (apply +))
                                                                                                          household-waste-areas-count))})))

        business-waste (->> db
                            (remove #(contains? ignored-areas (:area %)))
                            (filter #(= :business-waste-by-area (:record-type %)))
                            (map #(assoc % :record-type :business-waste)))
        business-waste-areas-count (count (distinct (map :area business-waste)))
        business-waste-averages (->> business-waste
                                     (group-by (juxt :year :waste-category))
                                     (map (fn [[[year waste-category] coll]] {:record-type    :business-waste
                                                                              :area           "average"
                                                                              :year           year
                                                                              :waste-category waste-category
                                                                              :tonnes         (double (/ (->> coll
                                                                                                              (map :tonnes)
                                                                                                              (apply +))
                                                                                                         business-waste-areas-count))})))

        data-for-output (concat household-waste business-waste household-waste-averages business-waste-averages)
        file (io/file filename)]

    (log/infof "Writing %s records" (count data-for-output))
    (io/make-parents file)
    (binding [*out* (FileWriter. file)]
      (json/pprint data-for-output))
    (log/infof "Wrote: %s" (.getAbsolutePath file))))