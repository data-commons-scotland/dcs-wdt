(ns dcs.wdt.prototype-4.ingest.api
  (:require [dcs.wdt.prototype-4.ingest.meta :as meta]
            [dcs.wdt.prototype-4.ingest.population :as population]
            [dcs.wdt.prototype-4.ingest.household-waste :as household-waste]
            [dcs.wdt.prototype-4.ingest.household-co2e :as household-co2e]
            [dcs.wdt.prototype-4.ingest.business-waste-by-region :as business-waste-by-region]
            [dcs.wdt.prototype-4.ingest.business-waste-by-sector :as business-waste-by-sector]
            [dcs.wdt.prototype-4.ingest.waste-site :as waste-site]))

(defn csv-files-from-sparql []
  (population/csv-file-from-sparql)
  (household-waste/csv-file-from-sparql))

(defn db-from-csv-files []
  (concat (population/db-from-csv-file)
          (household-waste/db-from-csv-file)
          (household-co2e/db-from-csv-file)
          (business-waste-by-region/db-from-csv-files)
          (business-waste-by-sector/db-from-csv-files)
          (waste-site/db-from-csv-file)))

(defn describe-source
  "Returns a string that describes the sourcing of the specified type of data."
  [record-type]
  (record-type meta/sources))