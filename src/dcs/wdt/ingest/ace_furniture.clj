(ns dcs.wdt.ingest.ace-furniture
  (:require [clojure.string :as str]
            [clojure.pprint :as pp]
            [clojure.java.io :as io]
            [clojure.data.csv :as csv]
            [clojure.data.json :as json]
            [taoensso.timbre :as log]
            [dk.ative.docjure.spreadsheet :as xls]
            [dcs.wdt.ingest.shared :as shared])
  (:import org.apache.commons.math3.stat.regression.SimpleRegression))


(def ingesting-dir "data/ingesting/ace-furniture")


;; ------------------------------------------------------
;; xls helpers

(defn to-bigdec [v]
  (bigdec (if (double? v)
            v
            0))) ;; ! this makes 0 indistinguishable from no-data - not great but leave for now

(defn to-int [v]
  (int (if (double? v)
         v
         0))) ;; ! this makes 0 indistinguishable from no-data - not great but leave for now

(defn select-cell [sheet row-number col-id]
  (xls/select-cell (str col-id row-number) sheet))

(defn read-row [sheet row-number]
  (let [select-cell' (partial select-cell sheet row-number)]
    {:category         (-> "A" select-cell' xls/read-cell) 
     :item             (-> "B" select-cell' xls/read-cell)
     :avg-kg           (-> "C" select-cell' xls/read-cell to-bigdec)
     :count-2018-02-28 (-> "D" select-cell' xls/read-cell to-int)
     :count-2019-02-28 (-> "F" select-cell' xls/read-cell to-int)
     :count-2019-08-31 (-> "H" select-cell' xls/read-cell to-int)}))

(defn read-sheet [sheet]
  (for [row-number (range 2 166)]
    (read-row sheet row-number)))


;; ------------------------------------------------------
;; entry point

(defn db-from-xls-and-csv-files
  "Create a seq of DB records from the Excel workbook that was supplied to us by ACE, and from the CSV that DCS constructed."
  []
  (let [;; read the workbook
        xls-filename (str ingesting-dir 
                          "/" 
                          (shared/dirname-with-max-supplied-date ingesting-dir) 
                          "/furniture reuse average weights.xls")
        _            (log/infof "Reading Excel file: %s" xls-filename)
        workbook     (xls/load-workbook xls-filename)

        ;; extract the data
        data0 (->> workbook
                   (xls/select-sheet "Base counts")
                   read-sheet)
        
        ;; pull out the [:category :item :yyyy-MM-dd :count]
        counts (->> data0
                    (map (fn [{:keys [category item]
                               :as   m}]
                           (for [date [:count-2018-02-28 :count-2019-02-28 :count-2019-08-31]]
                             {:category   category
                              :item       item
                              :yyyy-MM-dd (str/replace (name date) "count-" "")
                              :count      (date m)})))
                    flatten
                    (remove #(= 0 (:count %))))
        
        ;; pull out the [:category :item :avg-weight-tonnes]
        avg-weights (->> data0
                         (map #(select-keys % [:category :item :avg-kg])))

        ;; read the csv
        csv-filename (str ingesting-dir "/" (shared/dirname-with-max-supplied-date ingesting-dir) "/ace-furniture-to-scottish-carbon-metric.csv")
        _            (log/infof "Reading CSV file: %s" csv-filename)
        
        ;; pull out the [:category :item :material]
        materials    (->> csv-filename
                          slurp
                          csv/read-csv
                          (drop 1)
                          (map zipmap (repeat [:category :item :material])))
        
        ;; add record types then concat
        db (concat  (map #(assoc % :record-type :ace-furniture-count) counts)
                    (map #(assoc % :record-type :ace-furniture-avg-weight) avg-weights)
                    (map #(assoc % :record-type :ace-furniture-to-scottishCarbonMetric) materials))]
    
    (log/infof "Accepted records: %s" (count db))
    db))


;; ------------------------------------------------------
;; for REPL use

(comment

  ;; read the workbook
  (def workbook (xls/load-workbook (str ingesting-dir 
                                        "/" 
                                        (shared/dirname-with-max-supplied-date ingesting-dir) 
                                        "/furniture reuse average weights.xls")))


  ;; extract the data
  (def data0 (->> workbook
                  (xls/select-sheet "Base counts")
                  read-sheet))

  ;; have a look at it
  (pp/print-table [:category :item :avg-kg :count-2018-02-28 :count-2019-02-28 :count-2019-08-31]
                  (concat (take 5 data0)
                          (take-last 5 data0)))

  ;; pull out the [:category :item :yyyy-MM-dd :count]
  (def counts (->> data0
                   (map (fn [{:keys [category item]
                              :as   m}]
                          (for [date [:count-2018-02-28 :count-2019-02-28 :count-2019-08-31]]
                            {:category   category
                             :item       item
                             :yyyy-MM-dd (str/replace (name date) "count-" "")
                             :count      (date m)})))
                   flatten
                   (remove #(= 0 (:count %)))))

  ; have a look at it
  (pp/print-table [:category :item :yyyy-MM-dd :count]
                  (concat (take 5 counts)
                          (take-last 5 counts)))

  ;; check a couple of data points
  (assert (some #(= {:category   "Furniture "
                     :item       "Bedside unit, cabinet or table"
                     :yyyy-MM-dd "2018-02-28"
                     :count      28}
                    %)
                counts))
  (assert (some #(= {:category   "Cat 12 - Cooling Appliances containing refrigeration"
                     :item       "Fridge-Freezer "
                     :yyyy-MM-dd "2019-08-31"
                     :count      4}
                    %)
                counts))

  ;; pull out the [:category :item :avg-kg]
  (def avg-weights (->> data0
                        (map #(select-keys % [:category :item :avg-kg]))))

  ;; have a look at it
  (pp/print-table [:category :item :avg-kg]
                  (concat (take 5 avg-weights)
                          (take-last 5 avg-weights)))

  ;; check a couple of data points
  (assert (some #(= {:category "Furniture "
                     :item     "Bedside unit, cabinet or table"
                     :avg-kg   14M}
                    %)
                avg-weights))
  (assert (some #(= {:category "Cat 12 - Cooling Appliances containing refrigeration"
                     :item     "Fridge-Freezer "
                     :avg-kg   51M}
                    %)
                avg-weights))

  ;; read the csv
  (def materials (->> (str ingesting-dir 
                           "/" 
                           (shared/dirname-with-max-supplied-date ingesting-dir) 
                           "/ace-furniture-to-scottish-carbon-metric.csv")
                      slurp
                      csv/read-csv
                      (drop 1)
                      (map zipmap (repeat [:category :item :material]))))

  ;; have a look at it
  (pp/print-table [:category :item :material]
                  (concat (take 5 materials)
                          (take-last 5 materials)))

  ;; check they have the same sets of [:category :item] values
  (assert (= (->> avg-weights (map #(select-keys % [:category :item])) set)
             (->> materials (map #(select-keys % [:category :item])) set)))

  ;; prep for chart files
  (io/make-parents "tmp/ace-furniture/placeholder")

  ;; plotting helpers
  (def periods ["Mar 2017 - Feb 2018" "Mar 2018 - Feb 2019" "Mar 2019 - Aug 2019"])
  (def transform-spec [{:timeUnit "yearmonth"
                        :field    "yyyy-MM-dd"
                        :as       "instant"}
                       {:calculate "timeFormat(datum.instant, '%m %Y')"
                        :as        "simpleinstant"}
                       {:calculate (str "if(datum.simpleinstant == '02 2018', '" (get periods 0)
                                        "', if(datum.simpleinstant == '02 2019', '" (get periods 1)
                                        "', '" (get periods 2)
                                        "'))")
                        :as        "period"}])
  (def color-spec {:field  "period"
                   :type   "nominal"
                   :scale  {:domain periods
                            :range  ["#D99586" "#C0808C" "#9B708D"]}
                   :legend {:title "period"}})

  ;; plot the per-category per-accounting-period counts
  (def category-counts-chart-template
    {:schema     "https://vega.github.io/schema/vega/v5.json"
     :width      500
     :height     400
     :background "#f2dfce"
     :data       {:values :PLACEHOLDER}
     :transform  (conj transform-spec
                       {:aggregate [{:op    "sum"
                                     :field "count"
                                     :as    "count"}]
                        :groupby   ["period" "category"]})
     :mark       {:type                 "bar"
                  :cornerRadiusTopLeft  3
                  :cornerRadiusTopRight 3}
     :encoding   {:y       {:field "category"
                            :type  "nominal"
                            :sort  {:field "count"
                                    :order "descending"}}
                  :x       {:title "count"
                            :field "count"
                            :type  "quantitative"
                            :axis  {:title  "total count"
                                    :orient "top"}}
                  :color   color-spec
                  :tooltip [{:title "category"
                             :field "category"
                             :type  "nominal"}
                            {:title "period"
                             :field "period"
                             :type  "nominal"}
                            {:title "count"
                             :field "count"
                             :type  "quantitative"}]}})
  (binding [*out* (io/writer "tmp/ace-furniture/chart-1-category-counts.vl.json")]
    (json/pprint
     (-> category-counts-chart-template
         (assoc-in [:data :values] counts))))

  ;; plot the per-subcategory per-accounting-period counts
  (def subcategory-counts-chart-template
    {:schema     "https://vega.github.io/schema/vega/v5.json"
     :width      500
     :height     1500
     :background "#f2dfce"
     :data       {:values :PLACEHOLDER}
     :transform  transform-spec
     :mark       {:type                 "bar"
                  :cornerRadiusTopLeft  3
                  :cornerRadiusTopRight 3}
     :encoding   {:y       {:title "sub-category"
                            :field "item"
                            :type  "nominal"
                            :sort  {:field "count"
                                    :order "descending"}}
                  :x       {:title "count"
                            :field "count"
                            :type  "quantitative"
                            :axis  {:title  "total count"
                                    :orient "top"}}
                  :color   color-spec
                  :tooltip [{:title "category"
                             :field "category"
                             :type  "nominal"}
                            {:title "sub-category"
                             :field "item"
                             :type  "nominal"}
                            {:title "period"
                             :field "period"
                             :type  "nominal"}
                            {:title "count"
                             :field "count"
                             :type  "quantitative"}]}})
  (binding [*out* (io/writer "tmp/ace-furniture/chart-2-subcategory-counts.vl.json")]
    (json/pprint
     (-> subcategory-counts-chart-template
         (assoc-in [:data :values] counts))))

  ;; calc weights
  (def avg-weights-lookup-map (group-by (juxt :category :item) avg-weights))
  (defn lookup-avg-weight [category item]
    (->> [category item]
         (get avg-weights-lookup-map)
         first
         :avg-kg))
  (def weights (->> counts
                    (map (fn [{:keys [category item count]
                               :as   m}]
                           (assoc m :weight (* (lookup-avg-weight category item) count))))))

  ;; plot the per-category per-accounting-period weights
  (def category-weights-chart-template
    {:schema     "https://vega.github.io/schema/vega/v5.json"
     :width      500
     :height     400
     :background "#fff1e5"
     :data       {:values :PLACEHOLDER}
     :transform  (conj transform-spec
                       {:aggregate [{:op    "sum"
                                     :field "weight"
                                     :as    "weight"}]
                        :groupby   ["period" "category"]})
     :mark       {:type                 "bar"
                  :cornerRadiusTopLeft  3
                  :cornerRadiusTopRight 3}
     :encoding   {:y       {:field "category"
                            :type  "nominal"
                            :sort  {:field "weight"
                                    :order "descending"}}
                  :x       {:title "weight"
                            :field "weight"
                            :type  "quantitative"
                            :axis  {:title  "total kg"
                                    :orient "top"}}
                  :color   color-spec
                  :tooltip [{:title "category"
                             :field "category"
                             :type  "nominal"}
                            {:title "period"
                             :field "period"
                             :type  "nominal"}
                            {:title "kg"
                             :field "weight"
                             :type  "quantitative"}]}})
  (binding [*out* (io/writer "tmp/ace-furniture/chart-3-category-weights.vl.json")]
    (json/pprint
     (-> category-weights-chart-template
         (assoc-in [:data :values] weights))))

  ;; plot the per-subcategory per-accounting-period weights
  (def subcategory-weights-chart-template
    {:schema     "https://vega.github.io/schema/vega/v5.json"
     :width      500
     :height     1500
     :background "#fff1e5"
     :data       {:values :PLACEHOLDER}
     :transform  transform-spec
     :mark       {:type                 "bar"
                  :cornerRadiusTopLeft  3
                  :cornerRadiusTopRight 3}
     :encoding   {:y       {:title "sub-category"
                            :field "item"
                            :type  "nominal"
                            :sort  {:field "weight"
                                    :order "descending"}}
                  :x       {:title "weight"
                            :field "weight"
                            :type  "quantitative"
                            :axis  {:title  "total kg"
                                    :orient "top"}}
                  :color   color-spec
                  :tooltip [{:title "category"
                             :field "category"
                             :type  "nominal"}
                            {:title "sub-category"
                             :field "item"
                             :type  "nominal"}
                            {:title "period"
                             :field "period"
                             :type  "nominal"}
                            {:title "kg"
                             :field "weight"
                             :type  "quantitative"}]}})
  (binding [*out* (io/writer "tmp/ace-furniture/chart-4-subcategory-weights.vl.json")]
    (json/pprint
     (-> subcategory-weights-chart-template
         (assoc-in [:data :values] weights))))

  ;; flights-worth-of-CO2e calculations
  (def furniture->material  (->> materials
                                 (map (fn [{:keys [category item material]}] [[category item] material]))
                                 (into {})))
  (def co2e-multiplier-ingesting-dir "data/ingesting/co2e-multiplier")
  (def material->multiplier (->> (str co2e-multiplier-ingesting-dir
                                      "/"
                                      (shared/dirname-with-max-supplied-date co2e-multiplier-ingesting-dir)
                                      "/extract.csv")
                                 slurp
                                 csv/read-csv
                                 (drop 1)
                                 (map zipmap (repeat [:material :multiplier]))
                                 (map #(assoc % :multiplier (bigdec (:multiplier %))))
                                 (map (fn [{:keys [material multiplier]}] [material multiplier]))
                                 (into {})))
  (defn get-co2e-multiplier [category item]
    (->> [category item]
         (get furniture->material)
         (get material->multiplier)))
  (def weights-with-co2es  (->> weights
                                (map (fn [{:keys [category item weight]
                                           :as   m}]
                                       (assoc m :co2e (* weight (get-co2e-multiplier category item)))))))
  (def flights-per-category (->> weights-with-co2es
                                 ;; roll-up to per-category
                                 (group-by :category)
                                 (map (fn [[category coll]]
                                        {:category category
                                         :co2e     (apply + (map :co2e coll))}))
                                 ;; and calcuate the avoided CO2e in terms of flights (Glasgow -> Berlin, one-way)
                                 ;; 202.5 = average kg of CO2e per flight 
                                 (map (fn [{:keys [category co2e]}]
                                        {:category category
                                         :co2e     co2e
                                         :flights  (int (Math/round (/ co2e 202.5)))}))
                                 ;; for a Vega emoji representation, create a record per flight
                                 (map (fn [{:keys [category co2e flights]}]
                                        (repeat flights
                                                {:co2e          co2e
                                                 :category      category
                                                 :flights-total flights
                                                 :flight        1})))
                                 flatten))
  (def flights-per-subcategory (->> weights-with-co2es
                                    ;; roll-up to per-item
                                    (group-by (juxt :category :item))
                                    (map (fn [[[category item] coll]]
                                           {:category category
                                            :item     item
                                            :co2e     (apply + (map :co2e coll))}))
                                    ;; and calcuate the avoided CO2e in terms of flights (Glasgow -> Berlin, one-way)
                                    ;; 202.5 = average kg of CO2e per flight 
                                    (map (fn [{:keys [category item co2e]}]
                                           {:category category
                                            :item     item
                                            :co2e     co2e
                                            :flights  (int (Math/round (/ co2e 202.5)))}))
                                    ;; for a Vega emoji representation, create a record per flight
                                    (map (fn [{:keys [category item co2e flights]}]
                                           (repeat flights
                                                   {:co2e          co2e
                                                    :category      category
                                                    :item          item
                                                    :flights-total flights
                                                    :flight        1})))
                                    flatten))

  ;; plot the per-category Co2e avoided
  (def category-co2e-avoided-chart-template
    {:schema     "https://vega.github.io/schema/vega/v5.json"
     :width      500
     :height     400
     :background "#f2dfce"                                ;  "#980f3d"  "#fff1e5"
     :data       {:values :PLACEHOLDER}
     :transform  [{:calculate "datum.flight==1 ? '✈️' : ''"
                   :as        "emoji"}
                  {:window  [{:op    "sum"
                              :field "flight"
                              :as    "flights"}]
                   :groupby ["category"]}]
     :mark       {:type  "text"
                  :align "left"}
     :encoding   {:y       {:field "category"
                            :type  "nominal"
                            :sort  {:field "co2e"
                                    :order "descending"}}
                  :x       {:field "flights"
                            :type  "quantitative"
                            :axis  {:title "equivalent number of flights"}}
                  :text    {:field "emoji"
                            :type  "nominal"}
                  :size    {:value 15}
                  :tooltip [{:field "category"
                             :type  "nominal"}
                            {:title "kg of CO2e"
                             :field "co2e"
                             :type  "quantitative"}
                            {:title "equivalent number of flights"
                             :field "flights-total"
                             :type  "quantitative"}]}
     :config     {:axisX {:grid false}}})
  (binding [*out* (io/writer "tmp/ace-furniture/chart-5-category-co2e-avoided.vl.json")]
    (json/pprint
     (-> category-co2e-avoided-chart-template
         (assoc-in [:data :values] flights-per-category))))

  ;; plot the per-subcategory Co2e avoided
  (def subcategory-co2e-avoided-chart-template
    {:schema     "https://vega.github.io/schema/vega/v5.json"
     :width      500
     :height     1500
     :background "#f2dfce"                                ;  "#980f3d"  "#fff1e5"
     :data       {:values :PLACEHOLDER}
     :transform  [{:calculate "datum.flight==1 ? '✈️' : ''"
                   :as        "emoji"}
                  {:window  [{:op    "sum"
                              :field "flight"
                              :as    "flights"}]
                   :groupby ["category" "item"]}]
     :mark       {:type  "text"
                  :align "left"}
     :encoding   {:y       {:field "item"
                            :type  "nominal"
                            :sort  {:field "co2e"
                                    :order "descending"}}
                  :x       {:field "flights"
                            :type  "quantitative"
                            :axis  {:title "equivalent number of flights"}}
                  :text    {:field "emoji"
                            :type  "nominal"}
                  :size    {:value 15}
                  :tooltip [{:field "category"
                             :type  "nominal"}
                            {:field "item"
                             :type  "nominal"}
                            {:title "kg of CO2e"
                             :field "co2e"
                             :type  "quantitative"}
                            {:title "equivalent number of flights"
                             :field "flights-total"
                             :type  "quantitative"}]}
     :config     {:axisX {:grid false}}})
  (binding [*out* (io/writer "tmp/ace-furniture/chart-6-subcategory-co2e-avoided.vl.json")]
    (json/pprint
     (-> subcategory-co2e-avoided-chart-template
         (assoc-in [:data :values] flights-per-subcategory))))

  ;; calculate avg-count-per-month per period ...to convey trends
  
  ;; compute 'the trend of y'.
  ;;   (Returns the gradient of a linear approximation to the curve decribed by xy-pairs.)
  (defn trend [xy-pairs]
    (let [regression (SimpleRegression. true)]
      (doseq [[x y] xy-pairs]
        (.addData regression x y))
      (.getSlope regression)))

  ;; the x value for a yyyy-MM-dd, is the yyyy-MM-dd's index
  (def yyyy-MM-dds ["2018-02-28" "2019-02-28" "2019-08-31"])

  ;; the x value for a month-count, is the month-count's index
  (def month-counts [12 12 6])

  ;; calc...
  (def items-by-avg-count-per-month-at-x
    (->> counts
         (group-by (juxt :category :item))
         (map (fn [[[category item] coll]] (for [x [0 1 2]]
                                             (let [yyyy-MM-dd   (get yyyy-MM-dds x)
                                                   period-count (or (->> coll
                                                                         (filter #(= yyyy-MM-dd (:yyyy-MM-dd %)))
                                                                         first
                                                                         :count)
                                                                    0)
                                                   avg-count    (with-precision 6 (/ period-count (get month-counts x)))]
                                               {:category     category
                                                :item         item
                                                :yyyy-MM-dd   yyyy-MM-dd
                                                :x            x
                                                :period-count period-count
                                                :avg-count    avg-count}))))
         flatten
         (sort-by (juxt :category :item :yyyy-MM-dd :x))))

  ;; have a look at the result
  (pp/print-table [:category :item :yyyy-MM-dd :x :period-count :avg-count]
                  (concat (take 9 items-by-avg-count-per-month-at-x)
                          (take-last 9 items-by-avg-count-per-month-at-x)))

  ;; have a look at one
  (->> items-by-avg-count-per-month-at-x
       (filter (fn [m] (and (= "Soft Furniture " (:category m))
                            (= "Recliner" (:item m))
                            (= "2018-02-28" (:yyyy-MM-dd m))))))

  ;; check a few data points
  (assert (some #(= {:category     "Soft Furniture "
                     :item         "Recliner"
                     :yyyy-MM-dd   "2018-02-28"
                     :x            0
                     :period-count 3
                     :avg-count    1/4}
                    %)
                items-by-avg-count-per-month-at-x))
  (assert (some #(= {:category     "Soft Furniture "
                     :item         "Recliner"
                     :yyyy-MM-dd   "2019-02-28"
                     :x            1
                     :period-count 1
                     :avg-count    1/12}
                    %)
                items-by-avg-count-per-month-at-x))
  (assert (some #(= {:category     "Soft Furniture "
                     :item         "Recliner"
                     :yyyy-MM-dd   "2019-08-31"
                     :x            2
                     :period-count 15
                     :avg-count    15/6}
                    %)
                items-by-avg-count-per-month-at-x))
  
  ;; continue the calculation sequence
  
  ;; items-by-avg-count-per-month-trend
  (def items-by-avg-count-per-month-trend
    (->> items-by-avg-count-per-month-at-x
         (group-by (juxt :category :item))
         (map (fn [[[_ _] coll]] (let [trend-val (trend (map (fn [{:keys [x avg-count]}] [x avg-count]) coll))]
                                      ;; put this calculated trend-val into each item in the coll
                                   (map #(assoc % :trend trend-val) coll))))
         flatten
         (sort-by :trend)
         reverse))

  ;; have a look at the result
  (pp/print-table [:category :item :yyyy-MM-dd :x :period-count :avg-count :trend]
                  (concat (take 9 items-by-avg-count-per-month-trend)
                          (take-last 9 items-by-avg-count-per-month-trend)))
  
  ;; have a look at one
  (->> items-by-avg-count-per-month-trend
       (filter (fn [m] (and (= "Soft Furniture " (:category m))
                            (= "Recliner" (:item m))
                            (= "2018-02-28" (:yyyy-MM-dd m))))))

  ;; check a couple of data points (including that one above)
  (assert (some #(= {:category     "Soft Furniture "
                     :item         "Recliner"
                     :yyyy-MM-dd   "2018-02-28"
                     :x            0
                     :period-count 3
                     :avg-count    1/4
                     :trend        1.1249999999999998}
                    %)
                items-by-avg-count-per-month-trend))
  (assert (some #(= {:category     "Bedding & window dressings"
                     :item         "Blinds (wood, metal), curtains (thick, lined)"
                     :yyyy-MM-dd   "2019-08-31"
                     :x            2
                     :avg-count    0
                     :period-count 0
                     :trend        -0.041666666666666664}
                    %)
                items-by-avg-count-per-month-trend))
  
  ;; categories-by-avg-count-per-month-trend
  (def categories-by-avg-count-per-month-trend
    (->> items-by-avg-count-per-month-at-x
         (group-by (juxt :category :yyyy-MM-dd :x))
         (map (fn [[[category yyyy-MM-dd x] coll]] {:category     category
                                                    :yyyy-MM-dd   yyyy-MM-dd
                                                    :x            x
                                                    :period-count (->> coll
                                                                       (map :period-count)
                                                                       (apply +))
                                                    :avg-count    (->> coll
                                                                       (map :avg-count)
                                                                       (apply +))}))
         (group-by :category)
         (map (fn [[_ coll]] (let [trend-val (trend (map (fn [{:keys [x avg-count]}] [x avg-count]) coll))]
                                      ;; put this calculated trend-val into each item in the coll
                               (map #(assoc % :trend trend-val) coll))))
         flatten
         (sort-by :trend)
         reverse))

  ;; have a look at the result
  (pp/print-table [:category :yyyy-MM-dd :x :period-count :avg-count :trend]
                  (concat (take 9 categories-by-avg-count-per-month-trend)
                          (take-last 9 categories-by-avg-count-per-month-trend)))

  ;; check a data point
  (assert (some #(= {:category     "Bedding & window dressings"
                     :yyyy-MM-dd   "2019-02-28"
                     :x            1
                     :period-count 0
                     :avg-count    0
                     :trend        -0.041666666666666664}
                    %)
                categories-by-avg-count-per-month-trend))

  ;; plot the categories-by-avg-count-per-month-trend
  (def category-by-avg-count-per-month-trend-chart-template
    {:schema     "https://vega.github.io/schema/vega/v5.json"
     :background "floralwhite"
     :data       {:values :PLACEHOLDER}
     :transform  (conj transform-spec
                       {:calculate "truncate(datum.category, 30)"
                        :as        "categorytruncated"})
     :facet      {:field  "categorytruncated" 
                  :type   "nominal"
                  :sort   {:field "trend"
                           :op    "mean"
                           :order "descending"}
                  :header {:title        "category"
                           :titlePadding 30}}
     :columns    4
     :bounds     "flush"
     :spacing    35
       ;:resolve {:axis {:x "independent" :y "independent"}}
     :spec       {:width  130
                  :height 50
                  :layer  [{:mark     {:type  "line"
                                       :point {:filled false
                                               :fill   "#f2dfce"}}
                            :encoding {:x       {:field "instant"
                                                 :type  "temporal"
                                                 :axis  {:title      nil
                                                         :format     "%b %y"
                                                         :labelAngle 90
                                                  ;:labelBound 45
                                                         }
                                                 }
                                       :y       {:field "avg-count"
                                                 :type  "quantitative"
                                                 :axis  {:title nil}
                                                 :scale {:type "sqrt"}
                                                 }
                                       :color   {:condition {:test  "datum.trend >= 0"
                                                             :value "#00769C"}
                                                 :value     "#B46E6F"}
                                       :tooltip [{:title "category"
                                                  :field "category"
                                                  :type  "nominal"}
                                                 {:title "period"
                                                  :field "period"
                                                  :type  "nominal"}
                                                 {:title "total count for the period"
                                                  :field "period-count"
                                                  :type  "quantitative"}
                                                 {:title "avg count per month"
                                                  :field "avg-count"
                                                  :type  "quantitative"}
                                                 {:title "trend"
                                                  :field "trend"
                                                  :type  "quantitative"}]}}]}})
  (binding [*out* (io/writer "tmp/ace-furniture/chart-7-category-by-avg-count-per-month-trend.vl.json")]
    (json/pprint
     (-> category-by-avg-count-per-month-trend-chart-template
         (assoc-in [:data :values] categories-by-avg-count-per-month-trend))))

  ;; plot the items-by-avg-count-per-month-trend
  (def subcategory-by-avg-count-per-month-trend-chart-template
    {:schema     "https://vega.github.io/schema/vega/v5.json"
     :background "floralwhite"
     :data       {:values :PLACEHOLDER}
     :transform  (conj transform-spec
                       {:calculate "truncate(datum.item, 30)"
                        :as        "itemtruncated"})
     :facet      {:field  "itemtruncated"
                  :type   "nominal"
                  :sort   {:field "trend"
                           :op    "max"
                           :order "descending"}
                  :header {:title        "item"
                           :titlePadding 30}}
     :columns    5
     :bounds     "flush"
     :spacing    35
       ;:resolve {:axis {:x "independent" :y "independent"}}
     :spec       {:width  130
                  :height 50
                  :layer  [{:mark     {:type  "line"
                                       :point {:filled false
                                               :fill   "#f2dfce"}}
                            :encoding {:x       {:field "instant"
                                                 :type  "temporal"
                                                 :axis  {:title      nil
                                                         :format     "%b %y"
                                                         :labelAngle 90
                                                  ;:labelBound 45
                                                         }}
                                       :y       {:field "avg-count"
                                                 :type  "quantitative"
                                                 :axis  {:title nil}
                                                 :scale {:type "sqrt"}}
                                       :color   {:condition {:test  "datum.trend >= 0"
                                                             :value "#00769C"}
                                                 :value     "#B46E6F"}
                                       :tooltip [{:title "category"
                                                  :field "category"
                                                  :type  "nominal"}
                                                 {:title "item"
                                                  :field "item"
                                                  :type  "nominal"}
                                                 {:title "period"
                                                  :field "period"
                                                  :type  "nominal"}
                                                 {:title "total count for the period"
                                                  :field "period-count"
                                                  :type  "quantitative"}
                                                 {:title "avg count per month"
                                                  :field "avg-count"
                                                  :type  "quantitative"}
                                                 {:title "trend"
                                                  :field "trend"
                                                  :type  "quantitative"}]}}]}})
  (binding [*out* (io/writer "tmp/ace-furniture/chart-8-subcategory-by-avg-count-per-month-trend.vl.json")]
    (json/pprint
     (-> subcategory-by-avg-count-per-month-trend-chart-template
         (assoc-in [:data :values] items-by-avg-count-per-month-trend))))
  
  )