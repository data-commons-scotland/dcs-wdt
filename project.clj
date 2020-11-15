(defproject dcs-ldl "0.1.0-SNAPSHOT"
  :description "The Data Commons Scotland (DCS) linked data loader (LDL) - a tool and utility functions."
  :dependencies [[org.clojure/clojure "1.10.0"]
                 [org.clojure/data.csv "1.0.0"]
                 [org.clojure/data.json "1.0.0"]
                 [clj-http "3.10.3"]
                 [com.taoensso/timbre "5.1.0"]]
  :main ^:skip-aot dcs.ldl.procedures
  :target-path "target/%s"
  :profiles {:uberjar {:aot :all}})