(ns dcs.wdt.ingest.household-waste-analysis
  (:require [clojure.string :as str]
            [clojure.pprint :as pp]
            [clojure.java.io :as io]
            [clojure.data.json :as json]
            [taoensso.timbre :as log]
            [dk.ative.docjure.spreadsheet :as xls]))


;; ------------------------------------------------------
;; xls helpers

(defn assoc-dims
  [ix kgPerHhPerWk]
  {:stratum (inc (rem ix 7))
   :stream (inc (quot ix 7))
   :kgPerHhPerWk kgPerHhPerWk})

(defn read-cell-as-bigdec
  [cell]
  (let [v (xls/read-cell cell)]
    (bigdec
     (if (double? v)
       v
       0))))

(defn read-kgPerHhPerWk-fromPhaseCells
  [number-to-skip cells]
  (->> cells
       (drop number-to-skip) ;; skip to the first cell of interest
       (take 14) ;; expected number of data cells, 2 (phases) x 7 (stratum)
       (map read-cell-as-bigdec)
       (map-indexed assoc-dims)))

(defn read-kgPerHhPerWk-fromRow
  [row]
  (let [cells (xls/cell-seq row)
        [material-L1 material-L2] (->> cells
                                       (drop 1) ;; skip the Order cell
                                       (take 2)
                                       (map #(str/trim (xls/read-cell %))))
        kgPerHhPerWk-phase1 (->> cells
                                 (read-kgPerHhPerWk-fromPhaseCells 120) ;; phase 1 cells start at column DQ
                                 (map #(assoc %
                                              :material-L1 material-L1
                                              :material-L2 material-L2
                                              :phase 1)))
        kgPerHhPerWk-phase2 (->> cells
                                 (read-kgPerHhPerWk-fromPhaseCells 233) ;; phase 2 cells start at column HZ
                                 (map #(assoc %
                                              :material-L1 material-L1
                                              :material-L2 material-L2
                                              :phase 2)))]
    (concat kgPerHhPerWk-phase1
            kgPerHhPerWk-phase2)))

(defn read-kgPerHhPerWk-fromWorkbook
  [workbook]
  (->> workbook
       (xls/select-sheet "data")
       xls/row-seq
       (drop 6) ;; skip the header rows
       (take 72) ;; expected number of data rows
       (map read-kgPerHhPerWk-fromRow)
       flatten
       (remove #(contains? #{6 7} (:stratum %))))) ;; no info in stratum 6 & 7 so just remove them

(defn read-idealStream-fromRow
  [row]
  (let [cells (xls/cell-seq row)]
    {:material-L2 (-> cells
                      (nth 2)
                      xls/read-cell
                      str/trim)
     :idealStream (-> cells
                      (nth 294) ;; column KI, the 'mode' value 
                      xls/read-cell
                      int)}))

(defn read-idealStream-fromWorkbook
  [workbook]
  (->> workbook
       (xls/select-sheet "data")
       xls/row-seq
       (drop 6) ;; skip the header rows
       (take 72) ;; expected number of data rows
       (map read-idealStream-fromRow)))


;; ------------------------------------------------------
;; (re)labelling helpers

(defn stream-label
  [stream]
  (condp = stream
    1 "grey bin"
    2 "recycling bin"
    6 "food&garden ?"
    8 "recycling site"
    (str stream)))

(def stratum-labels ["urban £" "urban ££" "urban £££" "rural £/££" "rural £££"])
(def stratum-label (zipmap (range 1 6) stratum-labels))


;; ------------------------------------------------------
;; entry point

(defn db-from-xls-file
  "TODO Create a seq of DB records from the Excel workbook that was supplied to us by ZWS."
  []
  (let [;; read the workbookio/file 
        filename "data/ingesting/household-waste-analysis/originals/Waste Comp anonymous.xlsx"
        _        (log/infof "Reading CSV file: %s" filename)
        workbook (xls/load-workbook filename)

        ;; parse the weight data
        kgPerHhPerWk (read-kgPerHhPerWk-fromWorkbook workbook)
        
        ;; parse the ideal stream data
        idealStream (let [coll (read-idealStream-fromWorkbook workbook)]
                      (zipmap (map :material-L2 coll)
                              (map :idealStream coll)))

        ;; do some better labelling 
        kgPerHhPerWk_labelled (map #(assoc %
                                           :stratum (stratum-label (:stratum %))
                                           :stream (stream-label (:stream %))
                                           :idealStream (stream-label (get idealStream (:material-L2 %))))
                                   kgPerHhPerWk)
        
        ;; add record type
        kgPerHhPerWk_db (map #(assoc % :record-type :household-waste-analysis) kgPerHhPerWk_labelled)]
    
    (log/infof "Accepted records: %s" (count kgPerHhPerWk_db))
    kgPerHhPerWk_db))


;; ------------------------------------------------------
;; for REPL use

(comment

  ;; read the workbook
  (def workbook (xls/load-workbook "data/ingesting/household-waste-analysis/originals/Waste Comp anonymous.xlsx"))

  ;; parse the weight data
  (def kgPerHhPerWk (read-kgPerHhPerWk-fromWorkbook workbook))

  ;; have a look at it
  (pp/print-table [:material-L1 :material-L2 :phase :stream :stratum :kgPerHhPerWk]
                  (concat (take 5 kgPerHhPerWk)
                          (take-last 5 kgPerHhPerWk)))

  ;; check some data points
  (assert (some #(= {:material-L1  "Glass waste"
                     :material-L2  "Green container glass"
                     :phase        1
                     :stream       1
                     :stratum      1
                     :kgPerHhPerWk (bigdec 0.068)}
                    %)
                kgPerHhPerWk))
  (assert (some #(= {:material-L1  "Fines (<10mm)"
                     :material-L2  "Fines (<10mm)"
                     :phase        2
                     :stream       2
                     :stratum      4
                     :kgPerHhPerWk (bigdec 0.0326530612244898)}
                    %)
                kgPerHhPerWk))
  ;; expect that the following (stratum 7) record will have been removed
  (assert (not-any? #(= {:material-L1  "Fines (<10mm)"
                         :material-L2  "Fines (<10mm)"
                         :phase        2
                         :stream       2
                         :stratum      7
                         :kgPerHhPerWk (bigdec 0)}
                        %)
                    kgPerHhPerWk))

  ;; parse the ideal stream data
  (def idealStream (let [coll (read-idealStream-fromWorkbook workbook)]
                     (zipmap (map :material-L2 coll)
                             (map :idealStream coll))))

  ;; have a look at it
  (pp/pprint idealStream)

  ;; check a couple of data points
  (assert (= 8 (get idealStream "Green container glass")))
  (assert (= 1 (get idealStream "Fines (<10mm)")))

  ;; prep for chart files
  (io/make-parents "tmp/household-waste-analysis/placeholder")
  
  ;; is there an interesting difference between the phases?
  ;;   There are 2 phases: 
  ;;     * #1 is late Nov/early Dec 2013
  ;;     * #2 is early Mar 2014
  ;;   For each stratum/material-L2 combo, plot its phase quantities beside each other.
  (def compare-phases-chart-template
    {:schema     "https://vega.github.io/schema/vega/v5.json"
     :width      120
   :height     {:step 10}
   :spacing    3
   :background "#f2dfce"
   :data       {:values :PLACEHOLDER}
   :transform  [{:aggregate [{:op "sum" :field "kgPerHhPerWk" :as "kg"}]
                 :groupby ["stratum" "material-L2" "phase"]}]
   :mark       {:type                 "bar"
                :cornerRadiusTopLeft  3
                :cornerRadiusTopRight 3}
   :encoding   {:x       {:field "kg" :type "quantitative"}
                :y       {:field "phase" :type "nominal" :axis {:title ""}}
                :color {:field "material-L2" :type "nominal"
                        :scale {:scheme "tableau20"}
                        :legend nil #_{:orient "bottom-left" :columns 5 :titleOrient "top"}}
                :row   {:field "material-L2" :type "nominal"
                        :sort {:field "kg" :op "max" :order "descending"}
                        :header {:title "" :labelAngle 0 :labelAlign "left"}}
                :column  {:field "stratum" :type "nominal"}
                :tooltip [{:field "stratum"
                           :type  "nominal"}
                          {:field "material-L2"
                           :type  "nominal"}
                          {:field "phase"
                           :type  "nominal"}
                          {:field "kg"
                           :type  "quantitative"}]}})
  (binding [*out* (io/writer "tmp/household-waste-analysis/chart-1-compare-phases.vl.json")]
    (json/pprint
     (-> compare-phases-chart-template
         (assoc-in [:data :values] kgPerHhPerWk))))

  ;; ...from the chart above, it looks like there isn't an interesting difference between the 2 phases.
  ;;   So, below, we won't surface phase differences.

  ;; is there an interesting difference between the strata (i.e. household types)? 
  
  ;; first, do some better labelling
  (def kgPerHhPerWk_labelled (map #(assoc %
                                          :stratum (stratum-label (:stratum %))
                                          :stream (stream-label (:stream %))
                                          :idealStream (stream-label (get idealStream (:material-L2 %))))
                                  kgPerHhPerWk))
  
  ;; have a look at the result of that
  (pp/print-table [:material-L1 :material-L2 :phase :stream :idealStream :stratum :kgPerHhPerWk]
                  (concat (take 5 kgPerHhPerWk_labelled)
                          (take-last 5 kgPerHhPerWk_labelled)))

  ;; for each stratum (household type), plot its material-L1 quantities
  (def compare-strata-chart-template
    {:schema     "https://vega.github.io/schema/vega/v5.json"
     :background "#f2dfce"
     :width      400
     :height     250
     :data       {:values :PLACEHOLDER}
     :transform  [{:aggregate [{:op    "sum"
                                :field "kgPerHhPerWk"
                                :as    "kgPer2Wks"}]
                   :groupby   ["stratum" "material-L1"]}
                  {:calculate "datum.kgPer2Wks / 2"
                   :as        "kg"}] ;; to average over the 2 phases
     :mark       {:type                 "bar"
                  :cornerRadiusTopLeft  3
                  :cornerRadiusTopRight 3}
     :encoding   {:x       {:field "stratum"
                            :type  "nominal"
                            :sort  stratum-labels
                            :axis  {:title      "household type (location type & council tax band)"
                                    :labelAngle 0}}
                  :y       {:field "kg"
                            :type  "quantitative"
                            :axis  {:title "average kg/hh/wk"}}
                  :color   {:field  "material-L1"
                            :type   "nominal"
                            :legend {:title "material (high level)"}}
                  :tooltip [{:field "stratum"
                             :type  "nominal"
                             :title "house type (location & CTax)"}
                            {:field "material-L1"
                             :type  "nominal"
                             :title "material (high level)"}
                            {:field "kg"
                             :type  "quantitative"
                             :title "avg kg per household per wk"}]}})
  (binding [*out* (io/writer "tmp/household-waste-analysis/chart-2-compare-strata.vl.json")]
    (json/pprint
     (-> compare-strata-chart-template
         (assoc-in [:data :values] kgPerHhPerWk_labelled))))

  ;; an alternative calculation that produces values that are to be cross checked against the plotted chart's values
  (doseq [[stratum material-L1] [["urban £" "Food wastes"]
                               ["urban £" "Glass waste"]
                               ["rural £/££" "Fines (<10mm)"]]]
  (println (format "%s %s -> %s"
                   stratum
                   material-L1
                   (/ (->> kgPerHhPerWk_labelled
                           (filter #(= stratum (:stratum %)))
                           (filter #(= material-L1 (:material-L1 %)))
                           (map :kgPerHhPerWk)
                           (apply +))
                      2))))

  ;; look at more detail
  ;;   * For each stratum/material-L2 combo, plot its stream quantities beside each other.
  ;;   * Also superimpose info about stream appropriateness.
  ;;   * Use a non-linear scale to make the smaller amounts more evident.
  (def detailed-disposal-appropriateness-chart-template
    {:schema     "https://vega.github.io/schema/vega/v5.json"
     :background "floralwhite"
     :spacing    3
     :data       {:values :PLACEHOLDER}
     :transform  [{:aggregate [{:op    "mean"
                                :field "kgPerHhPerWk"
                                :as    "kg"}]
                   :groupby   ["stratum" "material-L1" "material-L2" "stream" "idealStream"]}]
     :facet      {:column {:field  "stratum"
                           :type   "nominal"
                           :sort   stratum-labels
                           :header {:title "household type (location type & council tax band)"}}
                  :row    {:field  "material-L2"
                           :type   "nominal"
                           :header {:title      "material (in detail)"
                                    :labelAngle 0
                                    :labelAlign "left"}
                           :sort   {:field "kg"
                                    :op    "max"
                                    :order "descending"}}}
     :spec       {:width  120
                  :height 20
                  :layer  [{:mark     "bar"
                            :encoding {:x           {:field "kg"
                                                     :type  "quantitative"
                                                     :scale {:type "sqrt"}
                                                     :axis  {:title "avg kg/hh/wk"}}
                                       :y           {:field "stream"
                                                     :type  "nominal"
                                                     :axis  {:title ""}}
                                       :color       {:field  "material-L2"
                                                     :type   "nominal"
                                                     :scale  {:scheme "tableau20"}
                                                     :legend nil}
                                       :fillOpacity {:value 0.5}
                                       :tooltip     [{:field "stratum"
                                                      :type  "nominal"
                                                      :title "house type (location & CTax)"}
                                                     {:field "material-L1"
                                                      :type  "nominal"
                                                      :title "material (high level)"}
                                                     {:field "material-L2"
                                                      :type  "nominal"
                                                      :title "material (in detail)"}
                                                     {:field "stream"
                                                      :type  "nominal"
                                                      :title "(actual) disposal"}
                                                     {:field "idealStream"
                                                      :type  "nominal"
                                                      :title "ideal disposal"}
                                                     {:field "kg"
                                                      :type  "quantitative"
                                                      :title "kg per household per week"}]}}
                           {:mark      "bar"
                            :transform [{:filter "(datum.kg > 0) && (datum.stream != datum.idealStream)"}]
                            :encoding  {:x           {:field "kg"
                                                      :type  "quantitative"
                                                      :scale {:type "sqrt"}
                                                      :axis  {:title "avg kg/hh/wk"}}
                                        :y           {:field "stream"
                                                      :type  "nominal"
                                                      :axis  {:title ""}}
                                        :color       {:field  "material-L2"
                                                      :type   "nominal"
                                                      :scale  {:scheme "tableau20"}
                                                      :legend nil}
                                        :fillOpacity {:value 0}
                                        :stroke      {:value "red"}
                                        :strokeWidth {:value 1}
                                        :tooltip     [{:field "stratum"
                                                       :type  "nominal"
                                                       :title "house type (location & CTax)"}
                                                      {:field "material-L1"
                                                       :type  "nominal"
                                                       :title "material (high level)"}
                                                      {:field "material-L2"
                                                       :type  "nominal"
                                                       :title "material (detailed level)"}
                                                      {:field "stream"
                                                       :type  "nominal"
                                                       :title "(actual) disposal"}
                                                      {:field "idealStream"
                                                       :type  "nominal"
                                                       :title "ideal disposal"}
                                                      {:field "kg"
                                                       :type  "quantitative"
                                                       :title "avg kg per household per wk"}]}}]}})
    (binding [*out* (io/writer "tmp/household-waste-analysis/chart-3-detailed-disposal-appropriateness.vl.json")]
      (json/pprint
       (-> detailed-disposal-appropriateness-chart-template
           (assoc-in [:data :values] kgPerHhPerWk_labelled))))

    ;; how appropriate are the disposal choices that are made by households?
(def summary-disposal-appropriateness-chart-template
  {:schema     "https://vega.github.io/schema/vega/v5.json"
   :background "#fff1e5"
   :data       {:values :PLACEHOLDER}
   :transform  [{:calculate "datum.stream + (datum.stream == datum.idealStream ? ' - appropriate' : ' - inappropriate')" :as "stream"}
                {:aggregate [{:op "sum" :field "kgPerHhPerWk" :as "kgPer2Wks"}]
                 :groupby   ["stratum" "stream"]}
                {:calculate "datum.kgPer2Wks / 2" :as "kg"}] ;; to average over the 2 phases
   :facet      {:column {:field  "stratum" :type "nominal"
                         :sort   stratum-labels
                         :header {:title "household type (location type & council tax band)"
                                  :titleOrient "bottom"
                                  :labelPadding -170}}} ;; hack to workaround labelOrient problem
   :spec       {:width  100
                :height 150
                :mark     {:type "bar"}
                :encoding {:x           {:field "stream" :type "nominal"
                                         :axis  nil}
                           :y           {:field "kg" :type "quantitative"
                                         :axis  {:title "average kg/hh/wk"}}
                           :color {:field "stream" :type "nominal"
                                   :scale {:domain ["grey bin - appropriate"
                                                    "grey bin - inappropriate"
                                                    "recycling bin - appropriate"
                                                    "recycling bin - inappropriate"]
                                           :range ["#928E85"
                                                   "#BF5748"
                                                   "#7FD1AE"
                                                   "#FD8D58"]}
                                   :legend {:title "disposal"}}
                           :tooltip     [{:field "stratum" :type "nominal" :title "house type (location & CTax)"}
                                         {:field "stream" :type "nominal" :title "disposal"}
                                         {:field "kg" :type "quantitative" :title "avg kg per household per wk"}]}}})
      (binding [*out* (io/writer "tmp/household-waste-analysis/chart-4-summary-disposal-appropriateness.vl.json")]
        (json/pprint
         (-> summary-disposal-appropriateness-chart-template
             (assoc-in [:data :values] kgPerHhPerWk_labelled))))
  
  )
  

