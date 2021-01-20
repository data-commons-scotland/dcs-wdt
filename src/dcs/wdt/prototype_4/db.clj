(ns dcs.wdt.prototype-4.db
  (:require [clojure.pprint :as pp]
            [taoensso.timbre :as log]
            [dcs.wdt.prototype-4.business-waste-by-area :as business-waste-by-area]
            [dcs.wdt.prototype-4.household-co2e :as household-co2e]))

(defn stats [db]
    {:record-count (count db)
     :year-count (count (distinct (map :year db)))
     :waste-category-count (count (distinct (map :waste-category db)))})

(defn glimpse [db]
  (doseq [record-type [:household-co2e :business-waste-by-area]]
    (let [sub-db (filter #(= record-type (:record-type %)) db)]
      (println "------------------")
      (println (name record-type) "records")
      (pp/pprint (stats sub-db))
      (pp/print-table (repeatedly 5 #(rand-nth sub-db ))))))

(defn db-from-csv-files []
  (concat (business-waste-by-area/db-from-csv-files)
          (household-co2e/db-from-csv-files)))

(defn glimpse-db [_]
  (log/set-level! :info)
  (let [db (db-from-csv-files)]
    (glimpse db)))

