(ns dcs.wdt.prototype-4.ingest.meta)

(def sources
  {:population               {:creator      "NRS"
                              :created-when 2020
                              :supplier     "statistics.gov.scot"
                              :supply-url   "http://statistics.gov.scot/data/population-estimates-current-geographic-boundaries"
                              :licence      "OGL v3.0 http://www.nationalarchives.gov.uk/doc/open-government-licence/version/3/"
                              :notes        "Published as an RDF data cube, the data can be accessed via SPARQL."}
   :household-waste          {:creator      "SEPA"
                              :created-when 2020
                              :supplier     "statistics.gov.scot"
                              :supply-url   "http://statistics.gov.scot/data/household-waste"
                              :licence      "OGL v3.0 http://www.nationalarchives.gov.uk/doc/open-government-licence/version/3/"
                              :notes        "Published as an RDF data cube, the data can be accessed via SPARQL."}
   :household-co2e           {:creator      "SEPA"
                              :created-when 2020
                              :supplier     "SEPA"
                              :supply-url   "https://www.environment.gov.scot/data/data-analysis/household-waste"
                              :licence      "OGL v2.0 http://www.nationalarchives.gov.uk/doc/open-government-licence/version/2/"
                              :notes        "Published through a web-based data tool, the data can be downloaded as a CSV file."}
   :business-waste-by-region {:creator      "SEPA"
                              :created-when 2020
                              :supplier     "SEPA"
                              :supply-url   "https://www.sepa.org.uk/environment/waste/waste-data/waste-data-reporting/business-waste-data"
                              :licence      "OGL v2.0 http://www.nationalarchives.gov.uk/doc/open-government-licence/version/2/"
                              :notes        "Published as a series of downloadable Excel files."}
   :business-waste-by-sector {:creator      "SEPA"
                              :created-when 2020
                              :supplier     "SEPA"
                              :supply-url   "https://www.sepa.org.uk/environment/waste/waste-data/waste-data-reporting/business-waste-data"
                              :licence      "OGL v2.0 http://www.nationalarchives.gov.uk/doc/open-government-licence/version/2/"
                              :notes        "Published as a series of downloadable Excel files."}
   :waste-site               {:creator      "SEPA"
                              :created-when 2020
                              :supplier     "SEPA"
                              :supply-url   "https://www.sepa.org.uk/data-visualisation/waste-sites-and-capacity-tool"
                              :licence      "OGL v2.0 http://www.nationalarchives.gov.uk/doc/open-government-licence/version/2/"
                              :notes        "Published through a web-based data tool, the data can be downloaded as a CSV file."}
   :waste-site-io            {:creator      "SEPA"
                              :created-when 2020
                              :supplier     "SEPA"
                              :supply-url   "https://www.sepa.org.uk/data-visualisation/waste-sites-and-capacity-tool"
                              :licence      "OGL v2.0 http://www.nationalarchives.gov.uk/doc/open-government-licence/version/2/"
                              :notes        "Published through a web-based data tool, the data can be downloaded as a CSV file."}})
