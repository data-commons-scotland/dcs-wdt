(ns dcs.wdt.prototype-4.dimensions
  "Dimension stuff... ordering, controlled values, etc.")

(def years
  "2011-2019 inclusive."
  (range 2011 2020))

(def regions
  "Scottish council areas and a few specials."
  ["Aberdeen City"
   "Aberdeenshire"
   "Angus"
   "Argyll and Bute"
   "City of Edinburgh"
   "Clackmannanshire"
   "Dumfries and Galloway"
   "Dundee City"
   "East Ayrshire"
   "East Dunbartonshire"
   "East Lothian"
   "East Renfrewshire"
   "Falkirk"
   "Fife"
   "Glasgow City"
   "Highland"
   "Inverclyde"
   "Midlothian"
   "Moray"
   "North Ayrshire"
   "North Lanarkshire"
   "Orkney Islands"
   "Outer Hebrides"
   "Perth and Kinross"
   "Renfrewshire"
   "Scottish Borders"
   "Shetland Islands"
   "South Ayrshire"
   "South Lanarkshire"
   "Stirling"
   "West Dunbartonshire"
   "West Lothian"
   ;; Specials
   "Offshore"
   "Unknown"])

(def materials
  "Waste material materials/categories."
  ["Acid, alkaline or saline wastes"
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
   "Used oils"
   "Vegetal wastes"
   "Waste containing PCB"
   "Wood wastes"])

(def managements
  "Waste management."
  ["Landfilled"
   "Other Diversion"
   "Recycled"])

;; TODO Is this used to cross-reference?
(def business-sectors
  "Business/economic sector classification."
  ["Agriculture, forestry and fishing"
   "Commerce"
   "Manufacture of chemicals, plastics and pharmaceuticals"
   "Manufacture of food and beverage products"
   "Manufacture of wood products"
   "Mining and quarrying"
   "Other manufacturing"
   "Power industry"
   "Waste management"
   "Water industry"])

(def sortable-dims [:record-type
                    :region :business-sector
                    :year :quarter
                    :site-name :permit :status :latitude :longitude
                    :io :material :management :ewc-code :ewc-description :operator :activity :sector
                    :population :tonnes :tonnes-input :tonnes-treated-recovered :tonnes-output])
(def place-it-last (count sortable-dims))

(defn ord
  "Associates the dimension with an ordinal value - useful for sorting."
  [dimension]
  (let [v (.indexOf sortable-dims dimension)]
    (if (= -1 v)
      place-it-last
      v)))

(defn min-max-useful?
  [dimension]
  (contains? #{:year :population :tonnes :tonnes-input :tonnes-treated-recovered :tonnes-output} dimension))

(defn count-useful?
  [dimension]
  (not (contains? #{:population :tonnes :tonnes-input :tonnes-treated-recovered :tonnes-output} dimension)))

(def record-types
  "Record types of the internal database."
  [:population
   :household-waste
   :household-co2e
   :business-waste-by-region
   :business-waste-by-sector
   :waste-site
   :waste-site-io])
