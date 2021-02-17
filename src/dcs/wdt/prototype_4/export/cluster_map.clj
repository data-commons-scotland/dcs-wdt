(ns dcs.wdt.prototype-4.export.cluster-map
  (:require [clojure.java.io :as io]
            [clojure.pprint :as pp]
            [clojure.data.json :as json]
            [taoensso.timbre :as log]
            [dcs.wdt.prototype-4.dimensions :as dims])
  (:import java.io.FileWriter))

(def file (io/file "data/exporting/cluster-map/waste-sites.geojson"))

(def geojson-template {:type       "FeatureCollection"
                       :features   []
                       :properties {:fields      {"n" {:name "Site name"}
                                                  "r" {:name "Region"}
                                                  "p" {:name "Permit"}
                                                  "s" {:name "Status"}
                                                  "a" {:name "Activity"}
                                                  "k" {:name "Sector"}
                                                  "t" {:lookup (apply array-map
                                                                      (interleave (map #(str "m" %) (range))
                                                                                  dims/materials))
                                                       :name   "Materials"}
                                                  "z" {:name "Total incoming tonnes"}}
                                    :attribution "SEPA",
                                    :description "Waste site locations and the quantities of incoming materials (2019)"}})


(defn generate-json-file [db]
  (let [;; Prep for looking up a material by an EWC code
        material-coding (filter #(= :material-coding (:record-type %)) db)
        material-coding-lookup-map (group-by :ewc-code material-coding)
        lookup-material (fn [ewc-code] (->> ewc-code
                                            (get material-coding-lookup-map)
                                            first
                                            :material))

        ;; Prep for looking up incoming tonnes (in 2019) by waste site's permit and a specified material
        waste-site-io (->> db
                           (filter #(= :waste-site-io (:record-type %)))
                           (filter #(= 2019 (:year %)))     ;; 2019 only
                           (filter #(= "Waste Inputs (Table B)" (:io %))) ;; inputs only
                           (map #(assoc % :material (lookup-material (:ewc-code %))))) ;; lookup and associate the material
        waste-site-io-lookup-map (group-by (juxt :permit :material) waste-site-io)
        lookup-tonnes (fn [permit material] (->> [permit material]
                                                 (get waste-site-io-lookup-map)
                                                 (map :tonnes)
                                                 (apply +)))

        ;; Add to each waste site's record, the incoming tonnes (in 2019) of each material
        waste-sites0 (for [m (filter #(= :waste-site (:record-type %)) db)]
                      (assoc m :tonnes-incoming
                               (apply array-map
                                      (flatten
                                        (filter #(> (second %) 0) ;; keep only pairs whose value is > 0
                                                (partition 2 ;; to look at each key-value pair
                                                           (interleave (map #(str "m" %) (range))
                                                                       (map #(lookup-tonnes (:permit m) %) dims/materials))))))))

        ;; Add to each waste site's record, the total incoming tonnes (in 2019)
        waste-sites (map (fn [m] (assoc m :tonnes-incoming-total (apply + (vals (:tonnes-incoming m)))))
                         waste-sites0)

        ;; Encode as GeoJSON oriented feature records
        features (map #(hash-map :geometry {:type        "Point"
                                            :coordinates [(:longitude %) (:latitude %)]}
                                 :type "Feature"
                                 :properties {"n" (:site-name %)
                                              "r" (:region %)
                                              "p" (:permit %)
                                              "s" (:status %)
                                              "a" (:activity %)
                                              "k" (:sector %)
                                              "t" (:tonnes-incoming %)
                                              "z" (:tonnes-incoming-total %)})
                      waste-sites)

        geojson (assoc geojson-template :features features)]

    (log/infof "Writing %s records" (count features))
    (io/make-parents file)
    (binding [*out* (FileWriter. file)]
      (json/pprint geojson))
    (log/infof "Wrote: %s" (.getAbsolutePath file))))

