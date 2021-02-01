(ns dcs.wdt.prototype-4.ingest.api
  (:require [dcs.wdt.prototype-4.ingest.population :as population]
            [dcs.wdt.prototype-4.ingest.household-waste :as household-waste]
            [dcs.wdt.prototype-4.ingest.household-co2e :as household-co2e]
            [dcs.wdt.prototype-4.ingest.business-waste-by-area :as business-waste-by-area]
            [dcs.wdt.prototype-4.ingest.business-waste-by-sector :as business-waste-by-sector]))

(defn db-from-csv-files []
  (concat (population/db-from-csv-file)
          (household-waste/db-from-csv-file)
          (household-co2e/db-from-csv-file)
          (business-waste-by-area/db-from-csv-files)
          (business-waste-by-sector/db-from-csv-files)))

(defn csv-files-from-sparql []
  (population/csv-file-from-sparql)
  (household-waste/csv-file-from-sparql))