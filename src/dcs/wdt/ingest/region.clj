(ns dcs.wdt.ingest.region)


(def internal
  "Scottish council areas (and a few specials) with their UK gov codes and Wikidata IDs."

  {"Aberdeen City"         {:code "S12000033" :qid  "Q62274582"}
   "Aberdeenshire"         {}
   "Angus"                 {} 
   "Argyll and Bute"       {}
   "City of Edinburgh"     {:code "S12000036" :qid  "Q2379199"}
   "Clackmannanshire"      {}
   "Dumfries and Galloway" {}
   "Dundee City"           {:code "S12000042" :qid  "Q2357511"}
   "East Ayrshire"         {}
   "East Dunbartonshire"   {}
   "East Lothian"          {}
   "East Renfrewshire"     {}
   "Falkirk"               {}
   "Fife"                  {:code "S12000047" :qid  "Q201149"}
   "Glasgow City"          {}
   "Highland"              {}
   "Inverclyde"            {}
   "Midlothian"            {}
   "Moray"                 {}
   "North Ayrshire"        {}
   "North Lanarkshire"     {}
   "Orkney Islands"        {}
   "Outer Hebrides"        {:code "S12000013" :qid  "Q80967"}
   "Perth and Kinross"     {}
   "Renfrewshire"          {}
   "Scottish Borders"      {}
   "Shetland Islands"      {}
   "South Ayrshire"        {}
   "South Lanarkshire"     {}
   "Stirling"              {:code "S12000030" :qid  "Q217838"}
   "West Dunbartonshire"   {}
   "West Lothian"          {}
   ;; Special
   "Offshore"              {}
   "Unknown"               {}})


(defn db-from-internal
  []
  (map (fn [[region m]] {:record-type :region
                         :region      region
                         :code        (:code m)
                         :qid         (:qid m)})
       internal))