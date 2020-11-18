(ns dcs.wdt.sepa-file
  (:require [clojure.java.io :as io]
            [clojure.data.csv :as csv]
            [dcs.wdt.misc :as misc]))

(defn co2es []
  (with-open [reader (io/reader "sepa-CO2e.csv")]
    (doall
      (->> reader
        csv/read-csv
        misc/to-maps
        (misc/patch :council)))))