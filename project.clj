(defproject dcs-wdt "0.1.0-SNAPSHOT"
  :description "The Data Commons Scotland (DCS) waste data tool (wdt) reads/writes linked data about waste from SPARQL/Wikibase."
  :dependencies [[org.clojure/clojure "1.10.0"]
                 [org.clojure/data.csv "1.0.0"]
                 [org.clojure/data.json "1.0.0"]
                 [clj-http "3.10.3"]
                 [com.taoensso/timbre "5.1.0"]
                 [org.clojure/tools.namespace "1.0.0"]]
  :main ^:skip-aot dcs.wdt.in-repl
  :target-path "target/%s"
  :profiles {:uberjar {:aot :all}})