(ns dcs.wdt.household-vs-business
  (:require [clojure.pprint :as pp]))

(def categories
  {:household ["Animal and mixed food waste"
               "Batteries and accumulators wastes"
               "Chemical wastes"
               "Combustion wastes"
               "Discarded equipment (excluding discarded vehicles, batteries and accumulators wastes)"
               "Discarded vehicles"
               "Glass wastes"
               "Health care and biological wastes"
               "Household and similar wastes"
               "Metallic wastes, ferrous"
               "Metallic wastes, mixed ferrous and non-ferrous"
               "Metallic wastes, non-ferrous"
               "Mineral waste from construction and demolition"
               "Mixed and undifferentiated materials"
               "Paper and cardboard wastes"
               "Plastic wastes"
               "Rubber wastes"
               "Soils"
               "Textile wastes"
               "Total Waste"
               "Used oils"
               "Vegetal wastes"
               "Wood wastes"]
   :business  ["Acid, alkaline or saline wastes"
               "Animal and mixed food waste"
               "Animal faeces, urine and manure"
               "Batteries and accumulators wastes"
               "Chemical wastes"
               "Combustion wastes"
               "Common sludges"
               "Discarded equipment (excluding discarded vehicles, batteries and accumulators wastes)"
               "Discarded vehicles"
               "Dredging spoils"
               "Glass wastes"
               "Health care and biological wastes"
               "Household and similar wastes"
               "Industrial effluent sludges"
               "Metallic wastes, ferrous"
               "Metallic wastes, mixed ferrous and non-ferrous"
               "Metallic wastes, non-ferrous"
               "Mineral waste from construction and demolition"
               "Mineral wastes from waste treatment and stabilised wastes"
               "Mixed and undifferentiated materials"
               "Other mineral wastes"
               "Paper and cardboard wastes"
               "Plastic wastes"
               "Rubber wastes"
               "Sludges and liquid wastes from waste treatment"
               "Soils"
               "Sorting residues"
               "Spent solvents"
               "Textile wastes"
               "Total"
               "Used oils"
               "Vegetal wastes"
               "Waste containing PCB"
               "Wood wastes"]
   :business-2011 ["Acid, alkaline or saline wastes"
                   "Animal and mixed food waste"
                   "Animal faeces, urine and manure"
                   "Batteries and accumulators wastes"
                   "Chemical wastes"
                   "Combustion wastes"
                   "Common sludges"
                   "Discarded equipment (excl discarded vehicles, batteries and accumulators)"
                   "Discarded vehicles"
                   "Dredging spoils"
                   "Glass wastes"
                   "Health care and biological wastes"
                   "Household and similar wastes"
                   "Industrial effluent sludges"
                   "Metallic wastes, ferrous"
                   "Metallic wastes, mixed ferrous and non-ferrous"
                   "Metallic wastes, non-ferrous"
                   "Mineral waste from construction and demolition"
                   "Mineral wastes from waste treatment and stabilised wastes"
                   "Mixed and undifferentiated materials"
                   "Other mineral wastes"
                   "Paper and cardboard wastes"
                   "Plastic wastes"
                   "Rubber wastes"
                   "Sludges and liquid wastes from waste treatment"
                   "Soils"
                   "Sorting residues"
                   "Spent solvents"
                   "Textile wastes"
                   "Total"
                   "Used oils"
                   "Vegetal wastes"
                   "Waste containing PCB"
                   "Wood wastes"]})

(def economic-sectors ["Agriculture, forestry and fishing"
                       "Commerce"
                       "Manufacture of chemicals, plastics and pharmaceuticals"
                       "Manufacture of food and beverage products"
                       "Manufacture of wood products"
                       "Mining and quarrying"
                       "Other manufacturing"
                       "Power industry"
                       "Waste management"
                       "Water industry"])


(defn quick-sizing [_]

  (let [diff (data/diff (set (:household categories)) (set (:business categories)))]
    (println (count (:household categories)) "household categories")
    (println (count (:business categories)) "business categories")
    (println (count (first diff)) "in household but not in business")
    (pp/print-table (map #(hash-map "in household but not in business" %) (first diff)))
    (println (count (second diff)) "in business but not in household")
    (pp/print-table (map #(hash-map "in business but not in household" %) (second diff))))

  (let [diff (data/diff (set (:business categories)) (set (:business-2011 categories)))]
    (println (count (:business categories)) "business-2018 categories")
    (println (count (:business-2011 categories)) "business-2011 categories")
    (println (count (first diff)) "in business-2018 but not in business-2011")
    (pp/print-table (map #(hash-map "in business-2018 but not in business-2011" %) (first diff)))
    (println (count (second diff)) "in business-2011 but not in business-2018")
    (pp/print-table (map #(hash-map "in business-2011 but not in business-2018" %) (second diff)))))