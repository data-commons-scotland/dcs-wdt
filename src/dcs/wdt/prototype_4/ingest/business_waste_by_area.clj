(ns dcs.wdt.prototype-4.ingest.business-waste-by-area
  (:require [clojure.string :as str]
            [taoensso.timbre :as log]
            [dcs.wdt.prototype-4.ingest.shared :as shared]))

(def expected-year-totals {2011 4025748   ;; Upstream provided value is 4025733
                           2012 3638689   ;; Upstream provided value is 3638683
                           2013 3662427   ;; Upstream provided value is 3662432
                           2014 3310130   ;; Upstream provided value is 3310131
                           2015 3599066   ;; Upstream provided value is 3599063
                           2016 3263468
                           2017 3335634   ;; Upstream provided value is 3335638
                           2018 3236531}) ;; Upstream provided value is 3236534})

(def waste-category-column-label "waste-category") ;; Expected to have been edited into each CSV extract

(defn split-by-area [m]
  (let [waste-category (get m waste-category-column-label)
        ;; Remove the waste-category entry
        remaining-m (dissoc m waste-category-column-label)]
    (for [[k v] remaining-m]
      {:waste-category waste-category
       :area k
       :tonnes v})))

(defn customise-map [m]
  (let [area (-> m
                 :area
                 str/trim
                 (str/replace "*" "")
                 (as-> v0
                       (get shared/area-aliases v0 v0)))
        waste-category (-> m
                           :waste-category
                           (str/replace "*" "")
                           str/trim
                           (as-> v0
                                 (get shared/waste-category-aliases v0 v0)))]
    (if (and (contains? shared/areas-set area)
             (contains? shared/waste-categories-set waste-category))
      {:area           area
       :waste-category waste-category
       :tonnes         (-> m
                           :tonnes
                           (str/replace "," "")
                           (str/replace "-" "0")
                           str/trim
                           Integer/parseInt)}
      (do (log/debugf "Ignoring: %s" m)
          nil))))

(defn csv-file-to-maps [file]
  (let [year (Integer/parseInt (re-find #"\d{4}" (.getName file)))
        customise-map (partial shared/customise-map-fn customise-map)]
    (->> file
         (#(do (log/infof "Reading CSV file: %s" (.getAbsolutePath %)) %))
         shared/csv-file-to-maps
         (#(do (log/infof "CSV data rows: %s" (count %)) %))
         (map split-by-area)
         flatten
         (#(do (log/infof "Candidate records: %s" (count %)) %))
         (map customise-map)
         (remove nil?)
         (#(do (log/infof "Accepted records: %s" (count %)) %))
         (map #(assoc % :year year)))))

(defn db-from-csv-files []
  (let [db (->> "data/ingesting/business-waste/csv-extracts/by-area/"
                shared/find-csv-files
                (map csv-file-to-maps)
                flatten
                (map #(assoc % :record-type :business-waste-by-area)))]
    (when-let [error (shared/check-year-totals :tonnes expected-year-totals db)]
      (throw (RuntimeException. (format "business-waste-by-area has year-totals error...\nExpected: %s\nActual: %s" (first error) (second error)))))
    (log/infof "business-waste-by-area records: %s" (count db))
    db))

