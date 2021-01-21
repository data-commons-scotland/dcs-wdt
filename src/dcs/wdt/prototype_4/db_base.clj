(ns dcs.wdt.prototype-4.db-base)

(def record-types [:population
                   :household-waste
                   :household-co2e
                   :business-waste-by-area
                   :business-waste-by-sector])

(def years (range 2011 2020))

(def areas ["Aberdeen City"
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

(def waste-categories ["Acid, alkaline or saline wastes"
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

(def end-states ["Landfilled"
                 "Other Diversion"
                 "Recycled"])

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