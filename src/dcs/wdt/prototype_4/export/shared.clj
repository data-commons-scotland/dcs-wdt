(ns dcs.wdt.prototype-4.export.shared
  "Forms that are used by more than one of the export namespaces."
  (:require [clojure.string :as str]))


(defn stringify-if-collection
  "If the value is a collection then convert it into a string."
  [v]
  (if (coll? v)
    (str/join " / " v)
    v))