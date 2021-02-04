(ns dcs.wdt.prototype-4.export.data_grid_and_graph
  (:require [clojure.java.io :as io]
            [clojure.data.json :as json]
            [taoensso.timbre :as log]
            [dcs.wdt.prototype-4.dimensions :as dims])
  (:import java.io.FileWriter))


(def filename "data/exporting/data-grid-and-graph/dx-data.json")


(defn generate-json-file [db]
  (let [
        ;; --------- household and business waste by region ---------

        ignored-regions #{"Offshore" "Unknown"}

        household-waste (->> db
                             (remove #(contains? ignored-regions (:region %)))
                             (filter #(= :household-waste (:record-type %)))
                             ;; Calculate the tonnes roll-up for each (region, year, type) triple
                             (group-by (juxt :region :year :type))
                             (map (fn [[[region year type] coll]] {:generator :household
                                                                   :region    region
                                                                   :year      year
                                                                   :type      type
                                                                   :tonnes    (->> coll
                                                                                   (map :tonnes)
                                                                                   (apply +))})))
        household-waste-regions-count (count (distinct (map :region household-waste)))
        household-waste-averages (->> household-waste
                                      (group-by (juxt :year :type))
                                      (map (fn [[[year type] coll]] {:generator :household
                                                                     :region    "average"
                                                                     :year      year
                                                                     :type      type
                                                                     :tonnes    (double (/ (->> coll
                                                                                                (map :tonnes)
                                                                                                (apply +))
                                                                                           household-waste-regions-count))})))

        business-waste (->> db
                            (remove #(contains? ignored-regions (:region %)))
                            (filter #(= :business-waste-by-region (:record-type %)))
                            (map #(assoc % :generator :business)))
        business-waste-regions-count (count (distinct (map :region business-waste)))
        business-waste-averages (->> business-waste
                                     (group-by (juxt :year :type))
                                     (map (fn [[[year type] coll]] {:generator :business
                                                                    :region    "average"
                                                                    :year      year
                                                                    :type      type
                                                                    :tonnes    (double (/ (->> coll
                                                                                               (map :tonnes)
                                                                                               (apply +))
                                                                                          business-waste-regions-count))})))

        by-region (->> [household-waste business-waste household-waste-averages business-waste-averages]
                       (apply concat)
                       (map #(assoc % :top-selector :by-region)))

        ;; --------- business waste by economic sector ---------

        by-business-sector (->> db
                                (filter #(= :business-waste-by-sector (:record-type %)))
                                (map #(assoc % :generator :business
                                               :top-selector :by-business-sector)))

        ;; --------- into a JSON file ---------

        data-for-output (concat by-region by-business-sector)
        file (io/file filename)]

    (log/infof "Writing %s records" (count data-for-output))
    (io/make-parents file)
    (binding [*out* (FileWriter. file)]
      (json/pprint data-for-output))
    (log/infof "Wrote: %s" (.getAbsolutePath file))))