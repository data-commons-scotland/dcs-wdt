(ns dcs.wdt.prototype-4.export.data_grid_and_graph
  (:require [clojure.java.io :as io]
            [clojure.data.json :as json]
            [taoensso.timbre :as log]
            [dcs.wdt.prototype-4.dimensions :as dims])
  (:import java.io.FileWriter))


(def filename "data/exporting/data-grid-and-graph/dx-data.json")


(defn generate-json-file [db]
  (let [
        ;; --------- household and business waste by area ---------

        ignored-areas #{"Offshore" "Unknown"}

        household-waste (->> db
                             (remove #(contains? ignored-areas (:area %)))
                             (filter #(= :household-waste (:record-type %)))
                             ;; Calculate the tonnes roll-up for each (area, year, waste-category) triple
                             (group-by (juxt :area :year :waste-category))
                             (map (fn [[[area year waste-category] coll]] {:generator     :household
                                                                           :area           area
                                                                           :year           year
                                                                           :waste-category waste-category
                                                                           :tonnes         (->> coll
                                                                                                (map :tonnes)
                                                                                                (apply +))})))
        household-waste-areas-count (count (distinct (map :area household-waste)))
        household-waste-averages (->> household-waste
                                      (group-by (juxt :year :waste-category))
                                      (map (fn [[[year waste-category] coll]] {:generator      :household
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
                            (map #(assoc % :generator :business)))
        business-waste-areas-count (count (distinct (map :area business-waste)))
        business-waste-averages (->> business-waste
                                     (group-by (juxt :year :waste-category))
                                     (map (fn [[[year waste-category] coll]] {:generator      :business
                                                                              :area           "average"
                                                                              :year           year
                                                                              :waste-category waste-category
                                                                              :tonnes         (double (/ (->> coll
                                                                                                              (map :tonnes)
                                                                                                              (apply +))
                                                                                                         business-waste-areas-count))})))

        by-area (->> [household-waste business-waste household-waste-averages business-waste-averages]
                      (apply concat)
                      (map #(assoc % :top-selector :by-area)))

        ;; --------- business waste by economic sector ---------

        by-economic-sector (->> db
                                (filter #(= :business-waste-by-sector (:record-type %)))
                                (map #(assoc % :generator :business
                                               :top-selector :by-economic-sector)))

        ;; --------- into a JSON file ---------

        data-for-output (concat by-area by-economic-sector)
        file (io/file filename)]

    (log/infof "Writing %s records" (count data-for-output))
    (io/make-parents file)
    (binding [*out* (FileWriter. file)]
      (json/pprint data-for-output))
    (log/infof "Wrote: %s" (.getAbsolutePath file))))