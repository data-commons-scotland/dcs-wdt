(ns dcs.wdt.dimensions
  "Dimension stuff... ordering, controlled values, etc.")

(def years
  "2011-2019 inclusive."
  (range 2011 2020))

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
   "Wood wastes"
   ;; Special
   "Uncategorised"])

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
                    :year :quarter :yyyy-MM-dd
                    :site-name :permit :status :latitude :longitude
                    :ewc-code :ewc-description :io-direction :material :material-L1 :material-L2 :management :recycling? :missed-bin? :operator :activities :accepts
                    :phase :stream :idealStream :stratum
                    :code :qid
                    :name :description :creator :created-when :supplier :supply-url :licence :licence-url :notes
                    :multiplier :count :tonnes :tonnes-input :tonnes-treated-recovered :tonnes-output :tonnes-weight :tonnes-co2e :kgPerHhPerWk])
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
  (contains? #{:year :multiplier :count :tonnes :tonnes-input :tonnes-treated-recovered :tonnes-output :tonnes-weight :tonnes-co2e :kgPerHhPerWk} dimension))

(defn count-useful?
  [dimension]
  (not (contains? #{:count :tonnes :tonnes-input :tonnes-treated-recovered :tonnes-output :tonnes-weight :tonnes-co2e :kgPerHhPerWk} dimension)))

(def descriptions
  {:record-type              "Indicates the dataset of the record."
   :region                   "The name of a council area."
   :business-sector          "The label representing the business/economic sector."
   :year                     "The integer representation of a year."
   :quarter                  "The integer representation of the year's quarter."
   :yyyy-MM-dd               "The date."
   :site-name                "The name of the waste site."
   :permit                   "The waste site operator's official permit or licence."
   :status                   "The label indicating the open/closed status of the waste site in the record's timeframe. "
   :latitude                 "The signed decimal representing a latitude."
   :longitude                "The signed decimal representing a longitude."
   :io-direction             "The label indicating the direction of travel of the waste from the PoV of a waste site."
   :material                 "The name of a waste material/stream in SEPA's classification."
   :material-L1              "The name of a waste material/stream in ZWS' high-level classification."
   :material-L2              "The name of a waste material/stream in ZWS' detailed-level classification."
   :management               "The label indicating how the waste was managed/processed (i.e. what its end-state was)."
   :recycling?               "True if the waste was categorised as 'for recycling' when collected."
   :missed-bin?              "True if the waste was in a missed bin."
   :ewc-code                 "The code from the European Waste Classification hierarchy."
   :ewc-description          "The description from the European Waste Classification hierarchy."
   :operator                 "The name of the waste site operator."
   :activities               "The waste processing activities supported by the waste site."
   :accepts                  "The kinds of clients/wastes accepted by the waste site."
   :multiplier               "The value to multiply a weight by to calculate the C02e amount."
   :phase                    "The sample period (1 = late Nov/early Dec 2013; early Mar 2014)"
   :stream                   "The household's disposal decision"
   :idealStream              "The ideal disposal decision"
   :stratum                  "The household type (location + CTax band)"
   :code                     "The UK government code."
   :qid                      "The Wikidata entity ID."
   :count                    "The population/household count as an integer."
   :tonnes                   "The waste related quantity as a decimal."
   :tonnes-input             "The quantity of incoming waste as a decimal."
   :tonnes-treated-recovered "The quantity of waste treated or recovered as a decimal."
   :tonnes-output            "The quantity of outgoing waste as a decimal."
   :tonnes-weight            "The waste related quantity as a decimal."
   :tonnes-co2e              "The waste related quantity as a decimal."
   :kgPerHhPerWk             "The waste related quantity as a decimal."})

(def record-types
  "Record types of the internal database."
  [:meta
   :region
   :household-waste
   :household-co2e
   :business-waste-by-region
   :business-waste-by-sector
   :waste-site-io
   :waste-site-material-io
   :bin-collection
   :sepa-material
   :ewc-code
   :co2e-multiplier
   :household
   :population
   :fairshare
   :household-waste-analysis])
