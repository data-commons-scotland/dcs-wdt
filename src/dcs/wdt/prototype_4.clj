(ns dcs.wdt.prototype-4
  (:require [clojure.pprint :as pp]
            [clojure.data :as data]
            [clojure.data.json :as json]
            [dcs.wdt.dataset.population :as population]
            [dcs.wdt.dataset.household-waste :as household-waste]
            [dcs.wdt.dataset.household-co2e :as household-co2e]))

(def expected-areas ["Aberdeen City" "Aberdeenshire" "Angus" "Argyll and Bute"
                     "City of Edinburgh" "Clackmannanshire"
                     "Dumfries and Galloway" "Dundee City"
                     "East Ayrshire" "East Dunbartonshire" "East Lothian" "East Renfrewshire"
                     "Falkirk" "Fife"
                     "Glasgow City"
                     "Highland"
                     "Inverclyde"
                     "Midlothian" "Moray"
                     "North Ayrshire" "North Lanarkshire"
                     "Orkney Islands" "Outer Hebrides"
                     "Perth and Kinross"
                     "Renfrewshire"
                     "Scotland" "Scottish Borders" "Shetland Islands" "South Ayrshire" "South Lanarkshire" "Stirling"
                     "West Dunbartonshire" "West Lothian"])
(def expected-end-states ["Landfilled" "Other Diversion" "Recycled"])

(def expected-years-pop [2011 2012 2013 2014 2015 2016 2017 2018 2019])
(def expected-record-count-pop (* (count expected-years-pop) (count expected-areas)))

(def expected-years-mgmt expected-years-pop)
(def expected-record-count-mgmt (* (count expected-years-mgmt) (count expected-end-states) (count expected-areas)))

(def expected-areas-co2e (remove #(= "Scotland" %) expected-areas))
(def expected-years-co2e [2017 2018 2019])
(def expected-record-count-co2e (* (count expected-years-co2e) (count expected-areas-co2e)))

(def filename-mgmt "hw-mgmt-data.js")
(def filename-co2e "hw-co2e-data.js")



(defn load-dataset-pop []

  (println "Loading the pop dataset...")
  (let [dataset (->> (population/dataset)
                     (map #(assoc % :year (Integer/parseInt (:year %))))
                     (filter #(>= (:year %) 2011))
                     (map #(assoc % :population (bigdec (:population %)))))]
    (println "Loaded")
    ;(pp/pprint (take 10 dataset))

    (println "Checking the dataset's dimensions...")
    (let [years (sort (keys (group-by :year dataset)))
          areas (sort (keys (group-by :area dataset)))
          records-count (count dataset)

          years-diff (data/diff expected-years-pop years)
          areas-diff (data/diff expected-areas areas)]

      (if (and (= 0 (count (first years-diff)))
               (= 0 (count (second years-diff))))
        (do (println "Year dimension is good")
            (println "\tValues:" years))
        (do (println "Year dimension is bad")
            (println "\tMissing values:" (first years-diff))
            (println "\tUnexpected values:" (second years-diff))))

      (if (and (= 0 (count (first areas-diff)))
               (= 0 (count (second areas-diff))))
        (do (println "Area dimension is good")
            (println "\tValues:" areas))
        (do (println "Area dimension is bad")
            (println "\tMissing values:" (first areas-diff))
            (println "\tUnexpected values:" (second areas-diff))))

      (if (= expected-record-count-pop records-count)
        (do (println "Tonnes dimension is good")
            (println (str "\t" records-count " records")))
        (do (println "Tonnes dimension is bad")
            (println (str "\t" records-count " records but expected " expected-record-count-pop)))))

    dataset))


(defn load-dataset-mgmt []

  (println "Loading the mgmt dataset...")
  (let [dataset (->> (household-waste/dataset)
                     (map #(assoc % :year (Integer/parseInt (:year %))
                                    :tonnes (bigdec (:tonnes %)))))]
    (println "Loaded")
    ;(pp/pprint (take 10 dataset))

    (println "Checking the dataset's dimensions...")
    (let [years (sort (keys (group-by :year dataset)))
          areas (sort (keys (group-by :area dataset)))
          end-states (sort (keys (group-by :endState dataset)))
          records-count (count dataset)

          years-diff (data/diff expected-years-mgmt years)
          end-states-diff (data/diff expected-end-states end-states)
          areas-diff (data/diff expected-areas areas)]

      (if (and (= 0 (count (first years-diff)))
               (= 0 (count (second years-diff))))
        (do (println "Year dimension is good")
            (println "\tValues:" years))
        (do (println "Year dimension is bad")
            (println "\tMissing values:" (first years-diff))
            (println "\tUnexpected values:" (second years-diff))))

      (if (and (= 0 (count (first end-states-diff)))
               (= 0 (count (second end-states-diff))))
        (do (println "End-state dimension is good")
            (println "\tValues:" end-states))
        (do (println "End-state dimension is bad")
            (println "\tMissing values:" (first end-states-diff))
            (println "\tUnexpected values:" (second end-states-diff))))

      (if (and (= 0 (count (first areas-diff)))
               (= 0 (count (second areas-diff))))
        (do (println "Area dimension is good")
            (println "\tValues:" areas))
        (do (println "Area dimension is bad")
            (println "\tMissing values:" (first areas-diff))
            (println "\tUnexpected values:" (second areas-diff))))

      (if (= expected-record-count-mgmt records-count)
        (do (println "Tonnes dimension is good")
            (println (str "\t" records-count " records")))
        (do (println "Tonnes dimension is bad")
            (println (str "\t" records-count " records but expected " expected-record-count-mgmt)))))

    dataset))


(defn load-dataset-co2e []

  (println "Loading the co2e dataset...")
  (let [dataset (->> (household-co2e/dataset)
                     (map #(assoc % :area (if (= "Na h-Eileanan Siar" (:council %)) "Outer Hebrides" (:council %))
                                    :year (Integer/parseInt (:year %))
                                    :tonnes (bigdec (:TCO2e %)))))]
    (println "Loaded")
    ;(pp/pprint (take 10 dataset))

    (println "Checking the dataset's dimensions...")
    (let [years (sort (keys (group-by :year dataset)))
          areas (sort (keys (group-by :area dataset)))
          records-count (count dataset)

          years-diff (data/diff expected-years-co2e years)
          areas-diff (data/diff expected-areas-co2e areas)]

      (if (and (= 0 (count (first years-diff)))
               (= 0 (count (second years-diff))))
        (do (println "Year dimension is good")
            (println "\tValues:" years))
        (do (println "Year dimension is bad")
            (println "\tMissing values:" (first years-diff))
            (println "\tUnexpected values:" (second years-diff))))

      (if (and (= 0 (count (first areas-diff)))
               (= 0 (count (second areas-diff))))
        (do (println "Area dimension is good")
            (println "\tValues:" areas))
        (do (println "Area dimension is bad")
            (println "\tMissing values:" (first areas-diff))
            (println "\tUnexpected values:" (second areas-diff))))

      (if (= expected-record-count-co2e records-count)
        (do (println "Tonnes dimension is good")
            (println (str "\t" records-count " records")))
        (do (println "Tonnes dimension is bad")
            (println (str "\t" records-count " records but expected " expected-record-count-co2e)))))

    dataset))


(defn gen-files [_]

  (let [dataset-pop (load-dataset-pop)
        dataset-pop-for-lookup (group-by (juxt :area :year) dataset-pop)
        dataset-mgmt (load-dataset-mgmt)
        dataset-co2e (load-dataset-co2e)]

    (letfn [(pop-lookup [area year] (-> dataset-pop-for-lookup (get [area year]) first :population))]

      (let [dataset-mgmt-for-output (map #(hash-map :areaLabel (:area %)
                                                    :year (:year %)
                                                    :endStateLabel (:endState %)
                                                    :quantity (with-precision 8 (/ (:tonnes %) (pop-lookup (:area %) (:year %)))))
                                         dataset-mgmt)]

        (println (str "Writing to the file " filename-mgmt "..."))
        (let [content (str "/* Generated  at " (java.time.LocalDateTime/now) " */"
                           "const quantities = " (json/write-str dataset-mgmt-for-output) ";\n"
                           "export { quantities };")]
          (spit filename-mgmt content)
          (println "Wrote")))

      (let [dataset-co2e-for-output (map #(hash-map :areaLabel (:area %)
                                                    :year (:year %)
                                                    :quantity (with-precision 8 (/ (:tonnes %) (pop-lookup (:area %) (:year %)))))
                                         dataset-co2e)]

        (println (str "Writing to the file " filename-co2e "..."))
        (let [content (str "/* Generated  at " (java.time.LocalDateTime/now) " */\n"
                           "const quantities = " (json/write-str dataset-co2e-for-output) ";\n"
                           "export { quantities };")]
          (spit filename-co2e content)
          (println "Wrote"))))))
