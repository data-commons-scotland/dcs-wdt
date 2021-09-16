(ns dcs.wdt.ingest.stirling-community-food
  (:require [clojure.string :as str]
            [clojure.pprint :as pp]
            [clojure.java.io :as io]
            [clojure.data.json :as json]
            [taoensso.timbre :as log]
            [dk.ative.docjure.spreadsheet :as xls])
  (:import java.text.SimpleDateFormat))


;; ------------------------------------------------------
;; xls helpers

(defn to-bigdec [v]
  (bigdec (if (double? v) 
            v 
            0)))

(defn select-cell [sheet row-number col-id]
  (xls/select-cell (str col-id row-number) sheet))

(defn read-row [sheet row-number]
  (let [select-cell' (partial select-cell sheet row-number)]
    {:date              (-> "B" select-cell' xls/read-cell) 
     :neighbourly       (-> "C" select-cell' xls/read-cell to-bigdec)
     :fareshare         (-> "D" select-cell' xls/read-cell to-bigdec)
     :foodcloud         (-> "E" select-cell' xls/read-cell to-bigdec)
     :sainsburys        (-> "F" select-cell' xls/read-cell to-bigdec)
     :cooperative       (-> "G" select-cell' xls/read-cell to-bigdec)
     :purchased         (-> "H" select-cell' xls/read-cell to-bigdec)
     :donated-as-waste  (-> "I" select-cell' xls/read-cell to-bigdec)
     :donated-not-waste (-> "J" select-cell' xls/read-cell to-bigdec)
     :other             (-> "K" select-cell' xls/read-cell to-bigdec)
     :bio-etc           (-> "M" select-cell' xls/read-cell to-bigdec)
     :compost-indiv     (-> "N" select-cell' xls/read-cell to-bigdec)
     :sanctuary         (-> "O" select-cell' xls/read-cell to-bigdec)
     :morning           (-> "P" select-cell' xls/read-cell to-bigdec)
     :evening           (-> "Q" select-cell' xls/read-cell to-bigdec)}))

(defn read-sheet [sheet]
  (for [row-number (range 5 394)]
    (read-row sheet row-number)))


;; ------------------------------------------------------
;; other helpers

(def yyyy-MM-dd-format (SimpleDateFormat. "yyyy-MM-dd"))
(def day-format (SimpleDateFormat. "E"))

(def counter-party-names
  {:local-supermarkets "Local supermarkets"
   :fareshare          "Fareshare"
   :purchased          "Purchased"
   :donated-as-waste   "Donated as waste"
   :donated-not-waste  "Donated not waste"
   :other              "Other"
   :bio-etc            "Council compost, Energen biogas, etc."
   :compost-indiv      "Used by individuals for compost"
   :sanctuary          "Donated to animal sanctuary"
   :used-as-food       "Used as food"})


;; ------------------------------------------------------
;; entry point

(defn db-from-xls-file
  "Create a seq of DB records from the Excel workbook that was supplied to us by Transition Stirling."
  []
  (let [;; read the workbookio/file 
        filename "data/ingesting/stirling-community-food/originals/New Data 2021.xlsx"
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

  
        
        ;; add record type
        data4    (map #(assoc % :record-type :stirling-community-food) data3)]
    
    (log/infof "Accepted records: %s" (count data4))
    data4))


;; ------------------------------------------------------
;; for REPL use

(comment

  ;; read the workbook
  (def workbook (xls/load-workbook "data/ingesting/stirling-community-food/originals/New Data 2021.xlsx"))

  ;; extract the data
  (def data0 (->> workbook
                  (xls/select-sheet "Stirling City Centre")
                  read-sheet))

  ;; -> data0 ≈ raw data
  
  ;; have a look at it
  (pp/print-table [:date
                   :neighbourly :fareshare :foodcloud :sainsburys :cooperative :purchased :donated-as-waste :donated-not-waste :other
                   :bio-etc :compost-indiv :sanctuary
                   :morning :evening]
                  (concat (take 5 data0)
                          (take-last 5 data0)))

  ;; check for the expected first and last rows 
  (assert (some #(= {:date              #inst "2020-03-28T00:00:00+00:00" ;; in Excel as a GMT value 
                     :neighbourly       361.4M
                     :fareshare         0M
                     :foodcloud         0M
                     :sainsburys        0M
                     :cooperative       0M
                     :purchased         0M
                     :donated-as-waste  0M
                     :donated-not-waste 0M
                     :other             0M
                     :bio-etc           0M
                     :compost-indiv     5.2M
                     :sanctuary         0M
                     :morning           0M
                     :evening           0M}
                    %)
                data0))
  (assert (some #(= {:date              #inst "2021-04-20T00:00:00+01:00" ;; in Excel as a BST value
                     :neighbourly       57.7M
                     :fareshare         0M
                     :foodcloud         0M
                     :sainsburys        0M
                     :cooperative       0M
                     :purchased         0M
                     :donated-as-waste  0M
                     :donated-not-waste 0M
                     :other             0M
                     :bio-etc           0M
                     :compost-indiv     0M
                     :sanctuary         0M
                     :morning           0M
                     :evening           0M}
                    %)
                data0))

  ;; Emma said: aggregate Neighbourly, Foodcloud, Sainsbury's and Cooperative into Local supermarkets (because of changing collectors)
  (def data1 (map (fn [{:keys [neighbourly foodcloud sainsburys cooperative]
                        :as   m}]
                    (-> m
                        (assoc :local-supermarkets (+' neighbourly foodcloud sainsburys cooperative))
                        (dissoc :neighbourly :foodcloud :sainsburys :cooperative)))
                  data0))

  ;; -> data1 ≈ data with the aggregate Local supermarkets
  
  ;; have a look at it
  (pp/print-table [:date
                   :local-supermarkets :fareshare :purchased :donated-as-waste :donated-not-waste :other
                   :bio-etc :compost-indiv :sanctuary
                   :morning :evening]
                  (concat (take 5 data1)
                          (take-last 5 data1)))

  ;; check a row for the expected aggregation
  (assert (some #(= {:date               #inst "2020-06-26T00:00:00+01:00" ;; in Excel as a BST value 
                     :local-supermarkets 61.5M
                     :fareshare          864M
                     :purchased          0M
                     :donated-as-waste   0M
                     :donated-not-waste  0M
                     :other              0M
                     :bio-etc            0M
                     :compost-indiv      0M
                     :sanctuary          0M
                     :morning            42M
                     :evening            0M}
                    %)
                data1))
  
  ;; rejig the food weight info into [:yyyy-MM-dd :io-direction :counterparty :tonnes]
  (def food (->> data1
                 (map (fn [{:keys [date]
                            :as   m}]
                        (let [yyyy-MM-dd (.format yyyy-MM-dd-format date)
                              ins        (for [counter-party [:local-supermarkets :fareshare :purchased :donated-as-waste :donated-not-waste :other]]
                                           {:yyyy-MM-dd    yyyy-MM-dd
                                            :io-direction  "in"
                                            :counter-party counter-party
                                            :tonnes        (/ (counter-party m) 1000)})
                              outs0      (for [counter-party [:bio-etc :compost-indiv :sanctuary]]
                                           {:yyyy-MM-dd    yyyy-MM-dd
                                            :io-direction  "out"
                                            :counter-party counter-party
                                            :tonnes        (/ (counter-party m) 1000)})
                              outs       (conj outs0 {:yyyy-MM-dd    yyyy-MM-dd
                                                      :io-direction  "out"
                                                      :counter-party :used-as-food
                                                      :tonnes        (- (apply + (map :tonnes ins))
                                                                        (apply + (map :tonnes outs0)))})]
                          (remove #(= 0M (:tonnes %))
                                  (concat ins outs)))))
                 flatten
                 (map #(assoc % :counter-party (counter-party-names (:counter-party %))))))
  
  ;; have a look at it
  (pp/print-table [:yyyy-MM-dd :io-direction :counter-party :tonnes]
                  (concat (take 5 food)
                          (take-last 5 food)))

  ;; rejig the footfall info into [:yyyy-MM-dd :day :count]
  ;; Emma said: aggregate Morning and Evening into a single daily footfall (because of changing opening hours)
  (def footfall (->> data1
                     (map (fn [{:keys [date morning evening]}]
                            {:yyyy-MM-dd (.format yyyy-MM-dd-format date)
                             :day        (.format day-format date)
                             :count      (+ morning evening)}))
                     (remove #(= 0M (:count %)))))
  
  ;; have a look at it
  (pp/print-table [:yyyy-MM-dd :day :count]
                  (concat (take 5 footfall)
                          (take-last 5 footfall)))

  ;; prep for chart files
  (io/make-parents "tmp/stirling-community-food/placeholder")

  ;; plot input per counter-party 
  (def input-per-counter-party-chart-template
    {:schema     "https://vega.github.io/schema/vega/v5.json"
     :width      370
     :height     200
     :background "#fff1e5"
     :data       {:values :PLACEHOLDER}
     :transform  [{:calculate "datum['counter-party']" :as "source"}
                  {:aggregate [{:op    "sum"
                                :field "tonnes"
                                :as    "tonnes"}]
                   :groupby   ["source"]}]
     :mark       {:type                 "bar"
                  :cornerRadiusTopLeft  3
                  :cornerRadiusTopRight 3}
     :encoding   {:x       {:field "source"
                            :type  "nominal"
                            :axis  {:labelAngle 45
                                    :labelBound 45}
                            :sort  {:field "tonnes"
                                    :order "descending"}}
                  :y       {:field "tonnes"
                            :type  "quantitative"}
                  :color   {:field  "source"
                            :type   "nominal"
                            :scale  {:domain ["Purchased"
                                              "Donated not waste"
                                              "Local supermarkets"
                                              "Fareshare"
                                              "Donated as waste"
                                              "Other"]
                                     :range  ["#FFC473"
                                              "#7158A1"
                                              "#006CAE"
                                              "#59896A"
                                              "#BF5748"
                                              "#928E85"]}
                            :legend {:symbolType "circle"
                                     :orient     "bottom"
                                     :columns    4}}
                  :tooltip [{:field "source"
                             :type  "nominal"}
                            {:field "tonnes"
                             :type  "quantitative"}]}})
  (binding [*out* (io/writer "tmp/stirling-community-food/chart-1-input-per-counter-party.vl.json")]
    (json/pprint
     (-> input-per-counter-party-chart-template
         (assoc-in [:data :values] (filter #(= "in" (:io-direction %)) food)))))

  ;; plot input per month
  (def input-per-month-chart-template
    {:schema     "https://vega.github.io/schema/vega/v5.json"
   :width      370
   :height     200
   :background "#fff1e5"
   :data       {:values :PLACEHOLDER}
   :transform  [{:calculate "datum['counter-party']" :as "source"}
                {:timeUnit "yearmonth" :field "yyyy-MM-dd" :as "month"}
                {:aggregate [{:op "sum" :field "tonnes" :as "tonnes"}]
                 :groupby ["month" "source"]}]
   :mark       {:type "line"
                :point {:filled false :fill "#fff1e5"}}
   :encoding   {:x       {:field "month" :type "temporal"
                          :axis {:format "%b %y"
                                 :labelAngle 60
                                 :labelBound 45}}
                :y       {:field "tonnes" :type "quantitative"}
                :color   {:field "source" :type "nominal"
                          :scale {:domain ["Purchased"
                                           "Donated not waste"
                                           "Local supermarkets"
                                           "Fareshare"
                                           "Donated as waste"
                                           "Other"]
                                  :range  ["#FFC473"
                                           "#7158A1"
                                           "#006CAE"
                                           "#59896A"
                                           "#BF5748"
                                           "#928E85"]}
                          :legend nil #_{:orient "bottom" :columns 4}}
                :tooltip [{:field "source"
                           :type  "nominal"}
                          {:field  "month"
                           :type   "temporal"
                           :format "%b %Y"}
                          {:field "tonnes"
                           :type  "quantitative"}]}})
  (binding [*out* (io/writer "tmp/stirling-community-food/chart-2-input-per-month.vl.json")]
    (json/pprint
     (-> input-per-month-chart-template
         (assoc-in [:data :values] (filter #(= "in" (:io-direction %)) food)))))
  
  ;; plot output per counter-party
  (def output-per-counter-party-chart-template
    {:schema     "https://vega.github.io/schema/vega/v5.json"
     :width      370
     :height     200
     :background "#f2dfce"
     :data       {:values :PLACEHOLDER}
     :transform  [{:calculate "datum['counter-party']" :as "outcome"}
                  {:aggregate [{:op "sum" :field "tonnes" :as "tonnes"}]
                   :groupby ["outcome"]}]
     :mark       {:type "bar"
                  :cornerRadiusTopLeft  3
                  :cornerRadiusTopRight 3}
     :encoding   {:x       {:field "outcome" :type "nominal"
                            :axis {:labelAngle 45
                                   :labelBound 45}
                            :sort {:field "tonnes" :order "descending"}}
                  :y       {:field "tonnes" :type "quantitative"}
                  :color {:field "outcome" :type "nominal"
                          :scale {:domain ["Used as food"
                                           "Donated to animal sanctuary"
                                           "Used by individuals for compost"
                                           "Council compost, Energen biogas, etc."]
                                  :range  ["#00AC8F"
                                           "#006AC7"
                                           "#B49531"
                                           "#E27E44"]}
                          :legend {:symbolType "circle"
                                   :orient "bottom" :columns 2}}
                  :tooltip [{:field "outcome" :type "nominal"}
                            {:field "tonnes" :type "quantitative"}]}})
  (binding [*out* (io/writer "tmp/stirling-community-food/chart-3-output-per-counter-party.vl.json")]
    (json/pprint
     (-> output-per-counter-party-chart-template
         (assoc-in [:data :values] (filter #(= "out" (:io-direction %)) food)))))
  
  ;; plot output per month
  (def output-per-month-chart-template
    {:schema     "https://vega.github.io/schema/vega/v5.json"
     :width      370
     :height     200
     :background "#f2dfce"
     :data       {:values :PLACEHOLDER}
     :transform  [{:calculate "datum['counter-party']"
                   :as        "outcome"}
                  {:timeUnit "yearmonth"
                   :field    "yyyy-MM-dd"
                   :as       "month"}
                  {:aggregate [{:op    "sum"
                                :field "tonnes"
                                :as    "tonnes"}]
                   :groupby   ["month" "outcome"]}]
     :mark       {:type  "line"
                  :point {:filled false
                          :fill   "#f2dfce"}}
     :encoding   {:x          {:field "month"
                               :type  "temporal"
                               :axis  {:format     "%b %y"
                                       :labelAngle 60
                                       :labelBound 45}}
                  :y          {:field "tonnes"
                               :type  "quantitative"}
                  :strokeDash {:condition {:test  "datum.outcome == 'Total received'"
                                           :value [5 5]}
                               :value     [0]}
                  :color      {:field  "outcome"
                               :type   "nominal"
                               :scale  {:domain ["Used as food"
                                                 "Donated to animal sanctuary"
                                                 "Used by individuals for compost"
                                                 "Council compost, Energen biogas, etc."
                                                 "Total received"]
                                        :range  ["#00AC8F"
                                                 "#006AC7"
                                                 "#B49531"
                                                 "#E27E44"
                                                 "grey" #_"#B1AB99"]}
                               :legend {:orient  "bottom"
                                        :columns 2}}
                  :tooltip    [{:field "outcome"
                                :type  "nominal"}
                               {:field  "month"
                                :type   "temporal"
                                :format "%b %Y"}
                               {:field "tonnes"
                                :type  "quantitative"}]}})
  (def total-received (->> food
                           (filter #(= "out" (:io-direction %)))
                           (group-by :yyyy-MM-dd)
                           (map (fn [[yyyy-MM-dd coll]] {:yyyy-MM-dd    yyyy-MM-dd
                                                         :counter-party "Total received"
                                                         :tonnes        (->> coll
                                                                             (map :tonnes)
                                                                             (apply +))}))))
  (binding [*out* (io/writer "tmp/stirling-community-food/chart-4-output-per-month.vl.json")]
    (json/pprint
     (-> output-per-month-chart-template
         (assoc-in [:data :values] (concat
                                    (filter #(= "out" (:io-direction %)) food)
                                        ;; for comparison, include the total received
                                    total-received)))))
    
  ;; plot logarithmic output per month
  (binding [*out* (io/writer "tmp/stirling-community-food/chart-5-logarithmic-output-per-month.vl.json")]
    (json/pprint
     (-> output-per-month-chart-template
         (assoc-in [:encoding :y :scale] {:type "log"})
         (assoc-in [:encoding :color :legend] nil)
         (assoc-in [:data :values] (filter #(= "out" (:io-direction %)) food)))))
  

  ;; plot footfall perDayOfWeek
  (def footfall-perDayOfWeek-chart-template
    {:schema     "https://vega.github.io/schema/vega/v5.json"
     :width      370
     :height     200
     :background "floralwhite"
     :data       {:values :PLACEHOLDER}
     :transform  [{:aggregate [{:op "sum" :field "count" :as "footfall"}]
                   :groupby ["day"]}]
     :mark       {:type "bar"
                  :cornerRadiusTopLeft  3
                  :cornerRadiusTopRight 3}
     :encoding   {:x       {:field "day" :type "nominal"
                            :axis {:title nil
                                   :labelAngle 60
                                   :labelBound 45}
                            :sort ["Sun" "Mon" "Tue" "Wed" "Thu" "Fri" "Sat"]}
                  :y       {:field "footfall" :type "quantitative"}
                  :color {:value "#8DD9E3"}
                  :tooltip [{:field "day" :type "nominal"}
                            {:field "footfall" :type "quantitative"}]}})
  (binding [*out* (io/writer "tmp/stirling-community-food/chart-6-footfall-perDayOfWeek.vl.json")]
    (json/pprint
     (-> footfall-perDayOfWeek-chart-template
         (assoc-in [:data :values] footfall))))
  
  ;; plot footfall perMonth
  (def footfall-perMonth-chart-template
    {:schema     "https://vega.github.io/schema/vega/v5.json"
     :width      370
     :height     200
     :background "floralwhite"
     :data       {:values :PLACEHOLDER}
     :transform  [{:timeUnit "yearmonth" :field "yyyy-MM-dd" :as "month"}
                  {:aggregate [{:op "sum" :field "count" :as "footfall"}]
                   :groupby ["month"]}]
     :mark       {:type "line"
                  :point {:filled false :fill "floralwhite"}}
     :encoding   {:x       {:field "month" :type "temporal"
                            :axis {:format "%b %y"
                                   :labelAngle 60
                                   :labelBound 45}}
                  :y       {:field "footfall" :type "quantitative"}
                  :color   {:value "#8DD9E3"}
                  :tooltip [{:field "month" :type "temporal" :format "%b %Y"}
                            {:field "footfall" :type "quantitative"}]}})
  (binding [*out* (io/writer "tmp/stirling-community-food/chart-7-footfall-perMonth.vl.json")]
    (json/pprint
     (-> footfall-perMonth-chart-template
         (assoc-in [:data :values] footfall))))
  ;; plot footfall perDayOfWeek perMonth
  (def footfall-perDayOfWeek-perMonth-chart-template
    {:schema     "https://vega.github.io/schema/vega/v5.json"
     :width      370
     :height     200
     :background "floralwhite"
     :data       {:values :PLACEHOLDER}
     :transform  [{:timeUnit "yearmonth"
                   :field    "yyyy-MM-dd"
                   :as       "month"}
                  {:aggregate [{:op    "sum"
                                :field "count"
                                :as    "footfall"}]
                   :groupby   ["month" "day"]}]
     :mark       {:type  "line"
                  :point {:filled false
                          :fill   "floralwhite"}}
     :encoding   {:x       {:field "month"
                            :type  "temporal"
                            :axis  {:format     "%b %y"
                                    :labelAngle 60
                                    :labelBound 45}}
                  :y       {:field "footfall"
                            :type  "quantitative"
                            :scale {:zero false}}
                  :color   {:field  "day"
                            :type   "nominal"
                            :sort   ["Sun" "Mon" "Tue" "Wed" "Thu" "Fri" "Sat"]
                            :scale  {:scheme "tableau20"}
                            :legend {:orient "bottom"}}
                  :tooltip [{:field "day"
                             :type  "nominal"}
                            {:field  "month"
                             :type   "temporal"
                             :format "%b %Y"}
                            {:field "footfall"
                             :type  "quantitative"}]}})
  (binding [*out* (io/writer "tmp/stirling-community-food/chart-8-footfall-perDayOfWeek-perMonth.vl.json")]
    (json/pprint
     (-> footfall-perDayOfWeek-perMonth-chart-template
         (assoc-in [:data :values] footfall))))

  

    )
  

  
