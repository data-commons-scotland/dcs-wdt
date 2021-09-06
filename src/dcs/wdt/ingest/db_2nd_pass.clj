(ns dcs.wdt.ingest.db-2nd-pass
  "Modifies the data in some types of records, using data from other types of records."
  (:require [taoensso.timbre :as log]))


(defn rollup-quarters-of-waste-site-material-io
  "Over waste-site-material-io records...
   Roll-up quarters (and operators) into their years.
   N.B. At Aug 2021, year 2020 had data for only 1 of its quarters."
  [db]
  (let [sub-db-toRemainAsIs (filter #(not= :waste-site-material-io (:record-type %)) db)
        sub-db-toBeModified (filter #(= :waste-site-material-io (:record-type %)) db)
        sub-db-modified     (->> sub-db-toBeModified
                                 (group-by (juxt :record-type :year :permit :io-direction :ewc-code))
                                 (map (fn [[[record-type year permit io-direction ewc-code] coll]] {:record-type  record-type
                                                                                                    :year         year
                                                                                                    :permit       permit
                                                                                                    :io-direction io-direction
                                                                                                    :ewc-code     ewc-code
                                                                                                    :tonnes       (->> coll
                                                                                                                       (map :tonnes)
                                                                                                                       (apply +))})))]
    (concat sub-db-toRemainAsIs sub-db-modified)))


(defn rollup-ewc-codes-of-waste-site-material-io
  "Over waste-site-material-io records...
   Roll-up the ewc-codes into their sepa-material 'parents'
   and remove 0-tonnes records."
  [db]
  (let [sub-db-toRemainAsIs (filter #(not= :waste-site-material-io (:record-type %)) db)
        sub-db-toBeModified (filter #(= :waste-site-material-io (:record-type %)) db)

        ;; Prep for looking up a sepa-material by an EWC code
        sepa-material (filter #(= :sepa-material (:record-type %)) db)
        sepa-material-lookup-map (group-by :ewc-code sepa-material)
        lookup-material (fn [ewc-code] (->> ewc-code
                                            (get sepa-material-lookup-map)
                                            first
                                            :material))

        sub-db-modified     (->> sub-db-toBeModified
                                 (map (fn [{:keys [ewc-code] :as m}] (assoc m :material (lookup-material ewc-code))))
                                 (group-by (juxt :record-type :year :permit :io-direction :material))
                                 (map (fn [[[record-type year permit io-direction material] coll]] {:record-type  record-type
                                                                                                    :year         year
                                                                                                    :permit       permit
                                                                                                    :io-direction io-direction
                                                                                                    :material     material
                                                                                                    :tonnes       (->> coll
                                                                                                                       (map :tonnes)
                                                                                                                       (apply +))}))
                                 (remove #(= 0M (:tonnes %))))]
    (concat sub-db-toRemainAsIs sub-db-modified)))


(defn add-sepa-material-into-ewc-code
  "Over ewc-code records...
   Associate sepa-materials with ewc-codes."
  [db]
  (let [sub-db-toRemainAsIs (filter #(not= :ewc-code (:record-type %)) db)
        sub-db-toBeModified (filter #(= :ewc-code (:record-type %)) db)

        ;; Prep for looking up a sepa-material by an EWC code
        sepa-material (filter #(= :sepa-material (:record-type %)) db)
        sepa-material-lookup-map (group-by :ewc-code sepa-material)
        lookup-material (fn [ewc-code] (->> ewc-code
                                            (get sepa-material-lookup-map)
                                            first
                                            :material))
        
        sub-db-modified     (->> sub-db-toBeModified
                                 (map (fn [{:keys [ewc-code]
                                            :as   m}] (assoc m :material (lookup-material ewc-code)))))]
    (concat sub-db-toRemainAsIs sub-db-modified)))


(defn remove-ewc-code-from-sepa-material
  "Over sepa-material records...
   Remove ewc-codes (leaving just a single column)." 
  [db]
  (let [sub-db-toRemainAsIs (filter #(not= :sepa-material (:record-type %)) db)
        sub-db-toBeModified (filter #(= :sepa-material (:record-type %)) db)

        sub-db-modified     (->> sub-db-toBeModified
                                 (map #(dissoc % :ewc-code))
                                 distinct)]
    (concat sub-db-toRemainAsIs sub-db-modified)))