(ns dcs.wdt.ingest.fairshare
  (:require [clojure.string :as str]
            [clojure.pprint :as pp]
            [clojure.java.io :as io]
            [clojure.data.json :as json]
            [taoensso.timbre :as log]
            [dk.ative.docjure.spreadsheet :as xls]))


;; ------------------------------------------------------
;; xls helpers

(defn col-ix
  "A->0, B->1, Z->25, AA->26, AB->27, BA->52, AAA->702
   I.e. convert a spreadsheet column id to an index."
  [^String col-id]
  (->> col-id
       seq
       (reduce (fn [acc c] (+ (* 26 acc) (- (int c) (dec (int \A))))) 0)
       dec))

(defn row-ix
  " 1->0, 2->1, 3->2
    I.e. convert a spreadsheet row number to an index."
  [^Integer row-number]
  (dec row-number))

(defn string [cell]
  (some-> cell
          xls/read-cell
          str/trim))

(defn number [cell]
  (let [v (xls/read-cell cell)]
    (bigdec
     (if (double? v)
       v
       0))))

(defn block-spec
  [first-row-number last-row-number material-col-id weight-col-id co2e-col-id]
  {:first (row-ix first-row-number)
   :last (row-ix last-row-number)
   :material (fn [cell-seq] (or ;; use cell at (material-col-id - 1) if the material-col-id cell is empty.
                                  ;; Useful only for sheet WC 2013-14 cell B25. 
                             (-> cell-seq (nth (col-ix material-col-id)) string)
                             (-> cell-seq (nth (dec (col-ix material-col-id))) string)))
   :weight (fn [cell-seq] (-> cell-seq (nth (col-ix weight-col-id)) number (/ 1000)))
   :co2e (fn [cell-seq] (-> cell-seq (nth (col-ix co2e-col-id)) number (/ 1000)))})

(def sheet-specs
  {"WC 2012" {:year 2013
              :quarter 1
              :block-specs [(block-spec 2 18 "A" "B" "G")]}
   "SC 13" {:year 2013
            :quarter 2
            :block-specs [(block-spec 2 18 "A" "B" "G")]}
   "WC 2013-14" {:year 2014
                 :quarter 1
                 :block-specs [(block-spec 2 18 "A" "B" "G")
                               (block-spec 25 30 "B" "C" "J")]}
   "Spring 14" {:year 2014
                :quarter 2
                :block-specs [(block-spec 2 18 "A" "B" "G")]}
   "SC 14" {:year 2014
            :quarter 3
            :block-specs [(block-spec 2 18 "A" "B" "G")
                          (block-spec 26 30 "A" "B" "I")]}
   "Autumn 2014" {:year 2014
                  :quarter 4
                  :block-specs [(block-spec 2 18 "A" "B" "G")]}
   "WC 2014" {:year 2015
              :quarter 1
              :block-specs [(block-spec 2 18 "A" "B" "G")]}
   "Spring 2015" {:year 2015
                  :quarter 2
                  :block-specs [(block-spec 2 18 "A" "B" "G")]}
   "SC 2015" {:year 2015
              :quarter 3
              :block-specs [(block-spec 2 18 "A" "B" "G")]}
   "Autumn 2015" {:year 2015
                  :quarter 4
                  :block-specs [(block-spec 2 18 "A" "B" "G")]}
   "WC2015" {:year 2016
             :quarter 1
             :block-specs [(block-spec 2 18 "A" "B" "G")]}
   "Spring 2016" {:year 2016
                  :quarter 2
                  :block-specs [(block-spec 2 18 "A" "B" "G")]}
   "SC 2016" {:year 2016
              :quarter 3
              :block-specs [(block-spec 2 16 "A" "B" "G")]}
   "Autumn 2016" {:year 2016
                  :quarter 4
                  :block-specs [(block-spec 2 16 "A" "B" "G")]}
   "WC 2016" {:year 2017
              :quarter 1
              :block-specs [(block-spec 2 16 "A" "B" "G")]}
   "Spring 2017" {:year 2017
                  :quarter 2
                  :block-specs [(block-spec 2 16 "A" "B" "G")]}
   "SC 2017" {:year 2017
              :quarter 3
              :block-specs [(block-spec 2 16 "A" "B" "G")]}
   "Autumn 2017" {:year 2017
                  :quarter 4
                  :block-specs [(block-spec 2 16 "A" "B" "G")]}
   "WC 2017" {:year 2018
              :quarter 1
              :block-specs [(block-spec 2 16 "A" "B" "G")]}
   "Spring 2018" {:year 2018
                  :quarter 2
                  :block-specs [(block-spec 2 16 "A" "B" "G")]}
   "SC 2018" {:year 2018
              :quarter 3
              :block-specs [(block-spec 2 16 "A" "B" "G")]}
   "Autumn 2018" {:year 2018
                  :quarter 4
                  :block-specs [(block-spec 2 16 "A" "B" "G")]}
   "WC 2018" {:year 2019
              :quarter 1
              :block-specs [(block-spec 2 16 "A" "B" "G")]}
   "Spring 2019" {:year 2019
                  :quarter 2
                  :block-specs [(block-spec 2 16 "A" "B" "G")]}
   "SC 2019 " {:year 2019
               :quarter 3
               :block-specs [(block-spec 2 16 "A" "B" "G")]}
   "Autumn 2019" {:year 2019
                  :quarter 4
                  :block-specs [(block-spec 2 16 "A" "B" "G")]}})

(defn read-row [block-spec row]
  (let [cell-seq (xls/cell-seq row)]
    {:material ((:material block-spec) cell-seq)
     :weight ((:weight block-spec) cell-seq)
     :co2e ((:co2e block-spec) cell-seq)}))

(defn read-sheet [sheet]
  (when-let [sheet-spec (get sheet-specs (xls/sheet-name sheet))]
    (->> (for [block-spec (:block-specs sheet-spec)]
           (let [first-row-ix (:first block-spec)
                 read-row' (partial read-row block-spec)]
             (->> sheet
                  xls/row-seq
                  (drop first-row-ix)
                  (take (- (:last block-spec) (dec first-row-ix)))
                  (map read-row'))))
         flatten
         (map #(assoc %
                      :year (:year sheet-spec)
                      :quarter (:quarter sheet-spec))))))


;; ------------------------------------------------------
;; material helpers

(def aliases
  {"Alluminium"                             "Aluminium"
   "Food"                                   "Food & Drink"
   "Paper"                                  "Paper & Cardboard"
   "Card"                                   "Paper & Cardboard"
   "Glass"                                  "Glass (Mixed - assumed go to aggregates)"
   "Scrap metal"                            "Scrap Metal"
   "Steel"                                  "Scrap Metal"
   "Duvet covers"                           "Textiles & Footwear"
   "Pillow cases"                           "Textiles & Footwear"
   "Sheets"                                 "Textiles & Footwear"
   "Textiles & footwear"                    "Textiles & Footwear"
   "Textiles and footwear"                  "Textiles & Footwear"
   "Textiles and footwear (assuming reuse)" "Textiles & Footwear"
   "Copier"                                 "Large WEEE (excluding fluorescent tubes)"
   "Office WEEE recycling"                  "Large WEEE (excluding fluorescent tubes)"
   "Mixed cans"                             "Mixed Cans"
   "Multifunctional printer"                "Large WEEE (excluding fluorescent tubes)"
   "Aggregates (toners and cartridges)"     "Small WEEE"
   "Wee Waste"                              "Large WEEE (excluding fluorescent tubes)"})

(defn maybe-alias [{:keys [material] :as m}]
  (if-let [alias (get aliases material)]
    (assoc m :material alias)
    m))


;; ------------------------------------------------------
;; quarter end date helpers

(def quarter-ends ["-03-31" "-06-30" "-09-30" "-12-31"])

(defn yyyy-MM-dd [year quarter]
  (str year (get quarter-ends (dec quarter))))


;; ------------------------------------------------------
;; entry point

(defn db-from-xls-file
  "Create a seq of DB records from the Excel workbook that was supplied to us by The fair Share."
  []
  (let [;; read the workbookio/file 
        filename "data/ingesting/fairshare/originals/Fairshare Donation Weights and Carbon Saving.xlsx"
        _        (log/infof "Reading CSV file: %s" filename)
        workbook (xls/load-workbook filename)

        ;; extract the data
        data0    (->> workbook
                      xls/sheet-seq
                      (map read-sheet)
                      flatten
                      (remove nil?))
        
        ;; alias some of the material names
        data1    (map maybe-alias data0)

        ;; roll-up to get values for (year quarter material)
        data2    (->> data1
                      (group-by (juxt :year :quarter :material))
                      (map (fn [[[year quarter material] coll]] {:year     year
                                                                 :quarter  quarter
                                                                 :material material
                                                                 :weight   (->> coll
                                                                                (map :weight)
                                                                                (apply +))
                                                                 :co2e     (->> coll
                                                                                (map :co2e)
                                                                                (apply +))})))

        ;; add quarter end dates for convenience, also do minor reformating
        data3    (map (fn [{:keys [year quarter material weight co2e]}]
                        {:region        "Stirling"
                         :year          year
                         :quarter       quarter
                         :yyyy-MM-dd    (yyyy-MM-dd year quarter)
                         :material      material
                         :tonnes-weight weight
                         :tonnes-co2e   co2e})
                      data2)
        
        ;; add record type
        data4    (map #(assoc % :record-type :fairshare) data3)]
    
    (log/infof "Accepted records: %s" (count data4))
    data4))


;; ------------------------------------------------------
;; for REPL use

(comment

  ;; read the workbook
  (def workbook (xls/load-workbook "data/ingesting/fairshare/originals/Fairshare Donation Weights and Carbon Saving.xlsx"))

  ;; extract the data
  (def data0 (->> workbook
                  xls/sheet-seq
                  (map read-sheet)
                  flatten
                  (remove nil?)))

  ;; -> data0 ≈ raw data
  
  ;; have a look at it
  (pp/print-table [:quarter :year :material :weight :co2e]
                  (concat (take 5 data0)
                          (take-last 5 data0)))

  ;; check a couple of rows 
  (assert (some #(= {:year     2013
                     :quarter  1
                     :material "Small WEEE"
                     :weight   (bigdec 0.0133)
                     :co2e     (bigdec 0.0236341)}
                    %)
                data0))
  (assert (some #(= {:year     2019
                     :quarter  4
                     :material "Food & Drink"
                     :weight   (bigdec 0.0187)
                     :co2e     (bigdec 0.07631775932000001)}
                    %)
                data0))

  ;; examine the material values 
  (pp/print-table (->> data0
                       (map :material)
                       frequencies
                       (into [])
                       (map (fn [[a b]] {:material  a
                                         :frequency b}))
                       (sort-by :frequency)))

  ;; alias some of the material names
  (def data1 (map maybe-alias data0))

  ;; -> data1 ≈ data with aliased material names
  
  ;; have a look at the result
  (pp/print-table (->> data1
                       (map :material)
                       frequencies
                       (into [])
                       (map (fn [[a b]] {:material  a
                                         :frequency b}))
                       (sort-by :frequency)))
  
  ;; roll-up to get values for (year quarter material)
  (def data2 (->> data1
                  (group-by (juxt :year :quarter :material))
                  (map (fn [[[year quarter material] coll]] {:year     year
                                                             :quarter  quarter
                                                             :material material
                                                             :weight   (->> coll
                                                                            (map :weight)
                                                                            (apply +))
                                                             :co2e     (->> coll
                                                                            (map :co2e)
                                                                            (apply +))}))))
  
  ;; -> data2 ≈ data with rolled-up per-material values
  
  ;; add quarter end dates for convenience, also do minor reformating
  (def data3 (map (fn [{:keys [year quarter material weight co2e]}]
                    {:region        "Stirling"
                     :year          year
                     :quarter       quarter
                     :yyyy-MM-dd    (yyyy-MM-dd year quarter)
                     :material      material
                     :tonnes-weight weight
                     :tonnes-co2e   co2e})
                  data2))

  ;; -> data3 ≈ data with quarter end dates and minor reformatting
  
  ;; have a look at the result
  (pp/print-table [:region :year :quarter :yyyy-MM-dd :material :tonnes-weight :tonnes-co2e]
                  (concat (take 5 data3)
                          (take-last 5 data3)))

  ;; prep for chart files
  (io/make-parents "tmp/fairshare/placeholder")

  ;; plot the weight of donated items per material per month
  (def tonnes-per-material-per-month-chart-template
    {:schema     "https://vega.github.io/schema/vega/v5.json"
     :width      400
     :height     350
     :background "#fff1e5"
     :data       {:values :PLACEHOLDER}
     :transform  [{:timeUnit "yearmonth"
                   :field    "yyyy-MM-dd"
                   :as       "month"}
                  {:aggregate [{:op    "sum"
                                :field :PLACEHOLDER
                                :as    "tonnes"}]
                   :groupby   ["month" "material"]}]
     :mark       {:type  "line"
                  :point {:filled false
                          :fill   "#fff1e5"}}
     :encoding   {:x       {:field "month"
                            :type  "temporal"
                            :axis  {:format     "%b %y"
                                    :labelAngle 60
                                    :labelBound 65
                                    :tickCount  {:interval "month"
                                                 :step     3
                                                 :start    0}}}
                  :y       {:field "tonnes"
                            :type  "quantitative"}
                  :color   {:field "material"
                            :type  "nominal"
                            :scale {:scheme "tableau20"}
                            ;:legend {:orient "bottom" :columns 4}
                            }
                  :tooltip [{:field "material"
                             :type  "nominal"}
                            {:field  "month"
                             :type   "temporal"
                             :format "%b %y"}
                            {:field "tonnes"
                             :type  "quantitative"}]}})
  (binding [*out* (io/writer "tmp/fairshare/chart-1-weight-per-material-per-month.vl.json")]
    (json/pprint
     (-> tonnes-per-material-per-month-chart-template
         (assoc-in [:transform 1 :aggregate 0 :field] "tonnes-weight")
         (assoc-in [:data :values] data3))))

  ;; -> tmp/fairshare/chart-1-weight-per-material-per-month.vl.json ...look at it with a a Vega viewer
  
  ;; plot the logarithmic weight of donated items per material per month
  (binding [*out* (io/writer "tmp/fairshare/chart-2-logarithmic-weight-per-material-per-month.vl.json")]
    (json/pprint
     (-> tonnes-per-material-per-month-chart-template
         (assoc-in [:encoding :y :scale] {:type     "pow"
                                          :exponent 0.25})
         (assoc-in [:transform 1 :aggregate 0 :field] "tonnes-weight")
         (assoc-in [:data :values] data3))))

  ;; -> tmp/fairshare/chart-2-logarithmic-weight-per-material-per-month.vl.json ...look at it with a a Vega viewer
  
  ;; plot the co2e of donated items per material per month
  (binding [*out* (io/writer "tmp/fairshare/chart-3-co2e-per-material-per-month.vl.json")]
    (json/pprint
     (-> tonnes-per-material-per-month-chart-template
         (assoc-in [:transform 1 :aggregate 0 :field] "tonnes-co2e")
         (assoc-in [:data :values] data3))))

  ;; -> tmp/fairshare/chart-3-co2e-per-material-per-month.vl.json ...look at it with a a Vega viewer
  
  ;; calc the CO2e avoided per year in terms of cars
  (def data4 (->> data3
                  ;; roll-up to per-year
                  (group-by :year)
                  (map (fn [[year coll]]
                         {:year        year
                          :tonnes-co2e (apply + (map :tonnes-co2e coll))}))
                     ;; and calcuate the avoided CO2e in terms of cars
                     ;; 4.9 = average tonnes of CO2e per car per year (incorporates exhaust emissions, fuel supply chain, car material)
                  (map (fn [{:keys [year tonnes-co2e]}]
                         {:year        year
                          :tonnes-co2e tonnes-co2e
                          :cars        (int (Math/round (/ tonnes-co2e 4.9)))}))
                     ;; for a Vega emoji representation, create a record per car
                  (map (fn [{:keys [year tonnes-co2e cars]}]
                         (repeat cars
                                 {:date             (str year "-01-01")
                                  :year-tonnes-co2e tonnes-co2e
                                  :year-cars        cars
                                  :car              1})))
                  flatten
                     ;; workaround a Vega rendering issue by delimiting the year range with empty valued records 
                  (cons {:date             "2012-01-01"
                         :year-tonnes-co2e 0
                         :year-cars        0
                         :car              0})
                  (cons {:date             "2020-01-01"
                         :year-tonnes-co2e 0
                         :year-cars        0
                         :car              0})))

  ;; -> data4 ≈ data describing the CO2e avoided per year in terms of cars, also Vega-lite oriented
  
  ;; have a look at it
  (pp/print-table [:date :year-tonnes-co2e :year-cars :car]
                  (concat (take 5 data4)
                          (take-last 5 data4)))

  ;; plot the CO2e avoided per year in terms of cars
  (def cars-worth-chart-template
    {:schema     "https://vega.github.io/schema/vega/v5.json"
     :width      300
     :height     350
     :background "#f2dfce" ;  "#980f3d"  "#fff1e5"
     :data       {:values :PLACEHOLDER}
     :transform  [{:calculate "datum.car==1 ? '🚗' : ''"
                   :as        "emoji"}
                  {:window  [{:op    "sum"
                              :field "car"
                              :as    "cars"}]
                   :groupby ["date"]}]
     :mark       {:type  "text"
                  :align "center"}
     :encoding   {:x       {:title "year"
                            :field "date"
                            :type  "temporal"
                            :axis  {:format     "%Y"
                                    :labelAngle 45
                                    :labelBound 50}}
                  :y       {:field "cars"
                            :type  "quantitative"
                            :axis  {:title "equivalent number of cars"}}
                  :text    {:field "emoji"
                            :type  "nominal"}
                  :size    {:value 15}
                  :tooltip [{:title  "year"
                             :field  "date"
                             :type   "temporal"
                             :format "%Y"}
                            {:title "tonnes of CO2e"
                             :field "year-tonnes-co2e"
                             :type  "quantitative"}
                            {:title "equivalent number of cars"
                             :field "year-cars"
                             :type  "quantitative"}]}
     :config     {:axisX {:grid false}}})
  (binding [*out* (io/writer "tmp/fairshare/chart-4-cars-worth.vl.json")]
    (json/pprint
     (-> cars-worth-chart-template
         (assoc-in [:data :values] data4))))

    ;; -> tmp/fairshare/chart-4-cars-worth.vl.json ...look at it with a a Vega viewer
  
    ;; plot the average weight per material per year
  (def average-tonnes-per-material-per-year-chart-template
    {:schema     "https://vega.github.io/schema/vega/v5.json"
     :width      370
     :height     200
     :background "#fff1e5"
     :data       {:values :PLACEHOLDER}
     :transform  [{:timeUnit "year"
                   :field    "yyyy-MM-dd"
                   :as       "year"}
                  {:aggregate [{:op    "average"
                                :field :PLACEHOLDER
                                :as    "avg-tonnes"}]
                   :groupby   ["material"]}]
     :mark       {:type                 "bar"
                  :cornerRadiusTopLeft  3
                  :cornerRadiusTopRight 3}
     :encoding   {:x       {:field "material"
                            :type  "nominal"
                            :axis  {:labelAngle 60
                                    :labelBound 65}
                            :sort  {:field "avg-tonnes"
                                    :order "descending"}}
                  :y       {:title "average tonnes per year"
                            :field "avg-tonnes"
                            :type  "quantitative"}
                  :color   {:value "#BF5748"}
                  :tooltip [{:title "material"
                             :field "material"
                             :type  "nominal"}
                            {:title "tonnes"
                             :field "avg-tonnes"
                             :type  "quantitative"}]}})
  (binding [*out* (io/writer "tmp/fairshare/chart-6-average-weight-per-material-per-year.vl.json")]
    (json/pprint
     (-> average-tonnes-per-material-per-year-chart-template
         (assoc-in [:transform 1 :aggregate 0 :field] "tonnes-weight")
         (assoc-in [:data :values] data3))))

    ;; -> tmp/fairshare/chart-6-average-weight-per-material-per-year.vl.json ...look at it with a a Vega viewer
  
  ;; plot the average co2e savings per material per year
  (binding [*out* (io/writer "tmp/fairshare/chart-7-average-co2e-savings-per-material-per-year.vl.json")]
    (json/pprint
     (-> average-tonnes-per-material-per-year-chart-template
         (assoc-in [:encoding :color :value] "#A2CAC1")
         (assoc-in [:transform 1 :aggregate 0 :field] "tonnes-co2e")
         (assoc-in [:data :values] data3))))

  ;; -> tmp/fairshare/chart-7-average-co2e-savings-per-material-per-year.vl.json ...look at it with a a Vega viewer
  
  ;; plot the weight per year
  (def tonnes-per-quarter-chart-template
    {:schema     "https://vega.github.io/schema/vega/v5.json"
     :width      370
     :height     200
     :background "#f2dfce"
     :data       {:values :PLACEHOLDER}
     :transform  [{:timeUnit "yearquarter"
                   :field    "yyyy-MM-dd"
                   :as       "quarter"}
                  {:aggregate [{:op    "sum"
                                :field :PLACEHOLDER
                                :as    "tonnes"}]
                   :groupby   ["quarter"]}]
     :mark       {:type                 "bar"
                  :cornerRadiusTopLeft  3
                  :cornerRadiusTopRight 3}
     :encoding   {:x       {:field "quarter"
                            :type  "temporal"
                            :axis  {:labelExpr  "timeFormat(datum.value, '%q') == '1' ? timeFormat(datum.value, 'Q%q %Y') : timeFormat(datum.value, 'Q%q')"
                                    :labelAngle 90
                                    :tickCount  {:interval "month"
                                                 :step     3
                                                 :start    0}}}
                  :y       {:field "tonnes"
                            :type  "quantitative"}
                  :color   {:value "#BF5748"}
                  :tooltip [{:title  "year quarter"
                             :field  "quarter"
                             :type   "temporal"
                             :format "Q%q %Y"}
                            {:field "tonnes"
                             :type  "quantitative"}]}})
  (binding [*out* (io/writer "tmp/fairshare/chart-8-weight-per-quarter.vl.json")]
    (json/pprint
     (-> tonnes-per-quarter-chart-template
         (assoc-in [:transform 1 :aggregate 0 :field] "tonnes-weight")
         (assoc-in [:data :values] data3))))

  ;; -> tmp/fairshare/chart-8-weight-per-quarter.vl.json ...look at it with a a Vega viewer
  
  ;; calc total weight across all years
  (apply + (map :tonnes-weight data3))
  
  )
  

  
