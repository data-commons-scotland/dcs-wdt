(ns dcs.wdt.ingest.meta)

(def sources
  {:household-waste          {:description  "The categorised quantities of the ('managed') waste generated by household."
                              :creator      "SEPA"
                              :created-when 2020
                              :supplier     "statistics.gov.scot"
                              :supply-url   "http://statistics.gov.scot/data/household-waste"
                              :licence      "OGL v3.0"
                              :licence-url  "http://www.nationalarchives.gov.uk/doc/open-government-licence/version/3/"
                              :notes        "Published as an RDF data cube, the data can be accessed via SPARQL."}
   :household-co2e           {:description  "The carbon impact of the waste generated by household."
                              :creator      "SEPA"
                              :created-when 2020
                              :supplier     "SEPA"
                              :supply-url   "https://www.environment.gov.scot/data/data-analysis/household-waste"
                              :licence      "OGL v2.0"
                              :licence-url  "http://www.nationalarchives.gov.uk/doc/open-government-licence/version/2/"
                              :notes        "Published through a web-based data tool, the data can be downloaded as a CSV file."}
   :business-waste-by-region {:description  "The categorised quantities of the waste generated by industry & commerce."
                              :creator      "SEPA"
                              :created-when 2020
                              :supplier     "SEPA"
                              :supply-url   "https://www.sepa.org.uk/environment/waste/waste-data/waste-data-reporting/business-waste-data"
                              :licence      "OGL v2.0"
                              :licence-url  "http://www.nationalarchives.gov.uk/doc/open-government-licence/version/2/"
                              :notes        "Published as a series of downloadable Excel files."}
   :business-waste-by-sector {:description  "The categorised quantities of the waste generated by industry & commerce."
                              :creator      "SEPA"
                              :created-when 2020
                              :supplier     "SEPA"
                              :supply-url   "https://www.sepa.org.uk/environment/waste/waste-data/waste-data-reporting/business-waste-data"
                              :licence      "OGL v2.0"
                              :licence-url  "http://www.nationalarchives.gov.uk/doc/open-government-licence/version/2/"
                              :notes        "Published as a series of downloadable Excel files."}
   :waste-site-io            {:description  "The locations, services & capacities of waste sites."
                              :creator      "SEPA"
                              :created-when 2020
                              :supplier     "SEPA"
                              :supply-url   "https://www.sepa.org.uk/data-visualisation/waste-sites-and-capacity-tool"
                              :licence      "OGL v2.0"
                              :licence-url  "http://www.nationalarchives.gov.uk/doc/open-government-licence/version/2/"
                              :notes        "Published through a web-based data tool, the data can be downloaded as a CSV file."}
   :waste-site-material-io   {:description  "The categorised quantities of waste going in and out of waste sites."
                              :creator      "SEPA"
                              :created-when 2020
                              :supplier     "SEPA"
                              :supply-url   "https://www.sepa.org.uk/data-visualisation/waste-sites-and-capacity-tool"
                              :licence      "OGL v2.0"
                              :licence-url  "http://www.nationalarchives.gov.uk/doc/open-government-licence/version/2/"
                              :notes        "Published through a web-based data tool, the data can be downloaded as a CSV file."}
   :bin-collection           {:description  "The categorised quantities of the waste collected from household bins."
                              :creator      "Stirling council"
                              :created-when 2021
                              :supplier     "Stirling council"
                              :supply-url   "https://data.stirling.gov.uk/dataset/waste-management"
                              :licence      "OGL v3.0"
                              :licence-url  "http://www.nationalarchives.gov.uk/doc/open-government-licence/version/3/"
                              :notes        "Published through a web-based data tool (CKAN), the data can be downloaded as CSV files."}
   :sepa-material            {:description  "A mapping between the EWC codes and SEPA's materials classification (as used in these datasets)."
                              :creator      "SEPA"
                              :created-when 2020
                              :supplier     "SEPA"
                              :supply-url   "https://www.sepa.org.uk/data-visualisation/waste-sites-and-capacity-tool"
                              :licence      "OGL v2.0"
                              :licence-url  "http://www.nationalarchives.gov.uk/doc/open-government-licence/version/2/"
                              :notes        "Published through a web-based data tool, the data can be downloaded as CSV files."}
   :ewc-code                 {:description  "EWC (European Waste Classification) codes and descriptions."
                              :creator      "European Commission of the EU"
                              :created-when 2000
                              :supplier     "Publications Office of the EU"
                              :supply-url   "https://eur-lex.europa.eu/legal-content/EN/TXT/HTML/?uri=CELEX:02000D0532-20150601&from=EN#tocId7"
                              :licence      "CC BY 4.0"
                              :licence-url  "https://creativecommons.org/licenses/by/4.0/"
                              :notes        "Published as a web page."}
   :co2e-multiplier          {:description  "Per-material weight-multipliers to calaculate CO2e amounts. This data has been copied from section 6.2 of The Scottish Carbon Metric."
                              :creator      "Zero Waste Scotland"
                              :created-when 2012
                              :supplier     "Zero Waste Scotland"
                              :supply-url   "https://www.zerowastescotland.org.uk/sites/default/files/The%20Scottish%20Carbon%20Metric.pdf"
                              :licence      "OGL v3.0"
                              :licence-url  "http://www.nationalarchives.gov.uk/doc/open-government-licence/version/3/"
                              :notes        "Published as a PDF document."}
   :household                {:description  "Occupied residential dwelling counts. Useful for calculating per-household amounts."
                              :creator      "NRS"
                              :created-when 2020
                              :supplier     "statistics.gov.scot"
                              :supply-url   "http://statistics.gov.scot/data/household-estimates"
                              :licence      "OGL v3.0"
                              :licence-url  "http://www.nationalarchives.gov.uk/doc/open-government-licence/version/3/"
                              :notes        "Published as an RDF data cube, the data can be accessed via SPARQL."}
   :population               {:description  "People counts. Useful for calculating per-citizen amounts."
                              :creator      "NRS"
                              :created-when 2020
                              :supplier     "statistics.gov.scot"
                              :supply-url   "http://statistics.gov.scot/data/population-estimates-current-geographic-boundaries"
                              :licence      "OGL v3.0"
                              :licence-url  "http://www.nationalarchives.gov.uk/doc/open-government-licence/version/3/"
                              :notes        "Published as an RDF data cube, the data can be accessed via SPARQL."}})