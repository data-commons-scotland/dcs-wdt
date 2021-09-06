(ns dcs.wdt.ingest.api
  (:require [dcs.wdt.ingest.meta :as meta]
            [dcs.wdt.ingest.household-waste :as household-waste]
            [dcs.wdt.ingest.household-co2e :as household-co2e]
            [dcs.wdt.ingest.business-waste-by-region :as business-waste-by-region]
            [dcs.wdt.ingest.business-waste-by-sector :as business-waste-by-sector]
            [dcs.wdt.ingest.waste-site-io :as waste-site-io]
            [dcs.wdt.ingest.waste-site-material-io :as waste-site-material-io]
            [dcs.wdt.ingest.stirling-bin-collection :as stirling-bin-collection]
            [dcs.wdt.ingest.material-coding :as material-coding]
            [dcs.wdt.ingest.ewc-coding :as ewc-coding]
            [dcs.wdt.ingest.co2e-multiplier :as co2e-multiplier]
            [dcs.wdt.ingest.households :as households]
            [dcs.wdt.ingest.population :as population]
            [dcs.wdt.ingest.db-2nd-pass :as db-2nd-pass]))

(defn csv-files-from-sparql []
  (household-waste/csv-file-from-sparql)
  (households/csv-file-from-sparql)
  (population/csv-file-from-sparql))

(defn db-from-csv-files []
  (concat (household-waste/db-from-csv-file)
          (household-co2e/db-from-csv-file)
          (business-waste-by-region/db-from-csv-files)
          (business-waste-by-sector/db-from-csv-files)
          (waste-site-io/db-from-csv-file)
          (waste-site-material-io/db-from-csv-file)
          (stirling-bin-collection/db-from-csv-files)
          (material-coding/db-from-csv-file)
          (ewc-coding/db-from-txt-file)
          (co2e-multiplier/db-from-csv-file)
          (households/db-from-csv-file)
          (population/db-from-csv-file)))

(defn db-from-2nd-pass 
  "Modify the data in some types of records, using data from other types of records."
  [db]
  (->> db
       db-2nd-pass/rollup-quarters-of-waste-site-material-io
       db-2nd-pass/rollup-ewc-codes-of-waste-site-material-io
       db-2nd-pass/add-sepa-material-into-ewc-code))

(defn describe-source
  "Returns a string that describes the sourcing of the specified type of data."
  [record-type]
  (record-type meta/sources))