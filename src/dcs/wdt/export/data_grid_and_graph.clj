(ns dcs.wdt.export.data_grid_and_graph
  (:require [clojure.java.io :as io]
            [clojure.data.json :as json]
            [taoensso.timbre :as log])
  (:import java.io.FileWriter))


(def filename "data/exporting/data-grid-and-graph/dx-data.json")


(defn generate-json-file [db]
  (let [
        ;; --------- household and business waste by region ---------
        
        ignored-regions #{"Offshore" "Unknown"}

        household-waste (->> db
                             (remove #(contains? ignored-regions (:region %)))
                             (filter #(= :household-waste (:record-type %)))
                             ;; Calculate the tonnes roll-up for each (region, year, material) triple
                             (group-by (juxt :region :year :material))
                             (map (fn [[[region year material] coll]] {:generator      :household
                                                                       :area           region
                                                                       :year           year
                                                                       :waste-category material
                                                                       :tonnes         (->> coll
                                                                                            (map :tonnes)
                                                                                            (apply +))})))
        household-waste-regions-count (count (distinct (map :area household-waste)))
        household-waste-averages (->> household-waste
                                      (group-by (juxt :year :waste-category))
                                      (map (fn [[[year waste-category] coll]] {:generator      :household
                                                                               :area           "average"
                                                                               :year           year
                                                                               :waste-category waste-category
                                                                               :tonnes         (double (/ (->> coll
                                                                                                               (map :tonnes)
                                                                                                               (apply +))
                                                                                                          household-waste-regions-count))})))

        business-waste (->> db
                            (remove #(contains? ignored-regions (:region %)))
                            (filter #(= :business-waste-by-region (:record-type %)))
                            (map #(assoc %
                                         :area (:region %)
                                         :waste-category (:material %) 
                                         :generator :business))
                            (map #(dissoc %
                                          :region
                                          :material
                                          :record-type)))
        business-waste-regions-count (count (distinct (map :area business-waste)))
        business-waste-averages (->> business-waste
                                     (group-by (juxt :year :waste-category))
                                     (map (fn [[[year waste-category] coll]] {:generator      :business
                                                                              :area           "average"
                                                                              :year           year
                                                                              :waste-category waste-category
                                                                              :tonnes         (double (/ (->> coll
                                                                                                              (map :tonnes)
                                                                                                              (apply +))
                                                                                                         business-waste-regions-count))})))

        by-region (->> [household-waste business-waste household-waste-averages business-waste-averages]
                       (apply concat)
                       (map #(assoc % :top-selector :by-area)))

        ;; --------- business waste by economic sector ---------
        
        by-business-sector (->> db
                                (filter #(= :business-waste-by-sector (:record-type %)))
                                (map #(assoc %
                                             :waste-category (:material %)
                                             :economic-sector (:business-sector %)
                                             :generator :business))
                                (map #(dissoc %
                                              :material
                                              :business-sector
                                              :record-type))
                                (map #(assoc % 
                                             :generator :business
                                             :top-selector :by-economic-sector)))

        ;; --------- into a JSON file ---------
        
        data-for-output (concat by-region by-business-sector)
        file (io/file filename)]

    (log/infof "Writing %s records" (count data-for-output))
    (io/make-parents file)
    (binding [*out* (FileWriter. file)]
      (json/pprint data-for-output))
    (log/infof "Wrote: %s" (.getAbsolutePath file))))