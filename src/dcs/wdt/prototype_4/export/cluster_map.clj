(ns dcs.wdt.prototype-4.export.cluster-map
  (:require [clojure.java.io :as io]
            [clojure.data.json :as json]
            [taoensso.timbre :as log])
  (:import java.io.FileWriter))

(def file (io/file "data/exporting/cluster-map/waste-sites.geojson"))

(def mock-template {:type       "FeatureCollection"
                    :features   []
                    :properties {:fields      {"5065" {:lookup {"1" "Pedestrian"
                                                                "3" "Motorcycle"
                                                                "2" "Bicycle"
                                                                "4" "Car"}
                                                       :name   "Site type"}
                                               "5055" {:name "Date"}
                                               "5074" {:lookup {"1" "red"
                                                                "3" "orange"
                                                                "2" "yellow"
                                                                "5" "green"
                                                                "4" "blue"
                                                                "6" "grey"}
                                                       :name   "Materials"}
                                               "9990" {:name "Region"}
                                               "9993" {:name "Site name"}
                                               "9991" {:name "Permit"}
                                               "9992" {:name "Status"}
                                               "9994" {:name "Site activity"}
                                               "9995" {:name "Site sector"}
                                               "9996" {:name "Tonnes input"}}
                                 :attribution "SEPA, Bard, etc. (TODO)",
                                 :description "Waste sites (WIP)"}})

(defn generate-json-file [db]
  (let [sub-db (filter #(= :waste-site (:record-type %)) db)
        features (map #(hash-map :geometry {:type        "Point"
                                            :coordinates [(:longitude %) (:latitude %)]}
                                 :type "Feature"
                                 :properties {"5065"           (str (inc (rand-int 4)))
                                              "5055"           "2021-02-08"
                                              "5074"           (str (inc (rand-int 6)))
                                              "9990"           (:region %)
                                              "9991"           (:permit %)
                                              "9992"           (:status %)
                                              "9993"           (:site-name %)
                                              "9994"           (:activity %)
                                              "9995"           (:sector %)
                                              "9996"           (:tonnes-input %)
                                              "tonnesIncoming" (let [materials (random-sample 0.5 (range 1 7))
                                                                     tonnes (repeatedly (count materials) (fn [] inc (rand-int 100)))]
                                                                 (zipmap materials tonnes))})
                      sub-db)
        mock (assoc mock-template :features features)]
    (log/infof "Writing %s records" (count features))
    (io/make-parents file)
    (binding [*out* (FileWriter. file)]
      (json/pprint mock))
    (log/infof "Wrote: %s" (.getAbsolutePath file))))