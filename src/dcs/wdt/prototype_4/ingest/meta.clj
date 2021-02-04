(ns dcs.wdt.prototype-4.ingest.meta
  (:require [clojure.string :as str]))

(def sources
  {:population               (str/join " "
                                       ["The underlying data was authored by NRS in 2020,"
                                        "then curated by statistics.gov.scot into the RDF data cube http://statistics.gov.scot/data/population-estimates-current-geographic-boundaries,"
                                        "then converted by DCS into this form."])
   :household-waste          (str/join " "
                                       ["The underlying data was authored by SEPA in 2020,"
                                        "then curated by statistics.gov.scot into the RDF data cube http://statistics.gov.scot/data/household-waste,"
                                        "then converted by DCS into this form."])
   :household-co2e           (str/join " "
                                       ["The underlying data was authored by SEPA in 2020,"
                                        "then curated by SEPA into the data tool https://www.environment.gov.scot/data/data-analysis/household-waste,"
                                        "then converted by DCS into this form."])
   :business-waste-by-region (str/join " "
                                       ["The underlying data was authored by SEPA in 2020,"
                                        "then curated by SEPA into the series of Excel files https://www.sepa.org.uk/environment/waste/waste-data/waste-data-reporting/business-waste-data,"
                                        "then converted by DCS into this form."])
   :business-waste-by-sector (str/join " "
                                       ["The underlying data was authored by SEPA in 2020,"
                                        "then curated by SEPA into the series of Excel files https://www.sepa.org.uk/environment/waste/waste-data/waste-data-reporting/business-waste-data,"
                                        "then converted by DCS into this form."])
   :waste-site               (str/join " "
                                       ["The underlying data was authored by SEPA in 2020,"
                                        "then curated by SEPA into the data tool https://www.sepa.org.uk/data-visualisation/waste-sites-and-capacity-tool,"
                                        "then converted by DCS into this form."])
   :waste-site-io            (str/join " "
                                       ["The underlying data was authored by SEPA in 2020,"
                                        "then curated by SEPA into the data tool https://www.sepa.org.uk/data-visualisation/waste-sites-and-capacity-tool,"
                                        "then converted by DCS into this form."])})


