(ns dcs.wdt.prototype-4.db
  (:require [clojure.pprint :as pp]
            [taoensso.timbre :as log]
            [dcs.wdt.prototype-4.population :as population]
            [dcs.wdt.prototype-4.household-waste :as household-waste]
            [dcs.wdt.prototype-4.household-co2e :as household-co2e]
            [dcs.wdt.prototype-4.business-waste-by-area :as business-waste-by-area]))

(defn counts [db]
  (into {}
        (cons [:all (count db)]
              (map (fn [type] [type (count (filter #(= type (:record-type %)) db))])
                   [:population :household-waste :household-co2e :business-waste-by-area]))))

(defn print-samples [db]
  (doseq [record-type [:population :household-waste :household-co2e :business-waste-by-area]]
    (let [sub-db (filter #(= record-type (:record-type %)) db)]
      (pp/print-table (repeatedly 5 #(rand-nth sub-db ))))))

(defn db-from-csv-files []
  (concat (population/db-from-csv-files)
          (household-waste/db-from-csv-files)
          (household-co2e/db-from-csv-files)
          (business-waste-by-area/db-from-csv-files)))

(defn glimpse-db [_]
  (log/set-level! :info)
  (let [db (db-from-csv-files)]
    (print-samples db)
    (pp/pprint (counts db))))

(defn csv-files-from-sparql[_]
  (log/set-level! :info)
  (population/csv-file-from-sparql)
  (household-waste/csv-file-from-sparql))