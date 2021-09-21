(ns dcs.wdt.ingest.stirling-community-food
  (:require [clojure.pprint :as pp]
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
            0))) ;; ! this makes 0 indistinguishable from no-data - not great but leave for now

(defn to-int [v]
  (int (if (double? v)
            v
            0))) ;; ! this makes 0 indistinguishable from no-data - not great but leave for now

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
     :morning           (-> "P" select-cell' xls/read-cell to-int)
     :evening           (-> "Q" select-cell' xls/read-cell to-int)}))

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
        _        (log/infof "Reading Excel file: %s" filename)
        workbook (xls/load-workbook filename)

        ;; extract the data
        data0 (->> workbook
                   (xls/select-sheet "Stirling City Centre")
                   read-sheet)
        
        ;; Emma said: aggregate Neighbourly, Foodcloud, Sainsbury's and Cooperative into Local supermarkets (because of changing collectors)
        data1 (map (fn [{:keys [neighbourly foodcloud sainsburys cooperative]
                         :as   m}]
                     (-> m
                         (assoc :local-supermarkets (+' neighbourly foodcloud sainsburys cooperative))
                         (dissoc :neighbourly :foodcloud :sainsburys :cooperative)))
                   data0)

        ;; rejig the food weight info into [:yyyy-MM-dd :io-direction :counterparty :tonnes]
        food (->> data1
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
                  (map #(assoc % :counter-party (counter-party-names (:counter-party %)))))

        ;; rejig the footfall info into [:yyyy-MM-dd :day :count]
        ;; Emma said: aggregate Morning and Evening into a single daily footfall (because of changing opening hours)
        footfall (->> data1
                      (map (fn [{:keys [date morning evening]}]
                             {:yyyy-MM-dd (.format yyyy-MM-dd-format date)
                              :day        (.format day-format date)
                              :count      (+ morning evening)}))
                      (remove #(= 0 (:count %))))
        
        ;; add record types then concat
        db (concat  (map #(assoc % :record-type :stirling-community-food-tonnes) food)
                    (map #(assoc % :record-type :stirling-community-food-footfall) footfall))]
    
    (log/infof "Accepted records: %s" (count db))
    db))


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
                     :morning           0
                     :evening           0}
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
                     :morning           0
                     :evening           0}
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
                     :morning            42
                     :evening            0}
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
                     (remove #(= 0 (:count %)))))

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
                           (filter #(= "in" (:io-direction %)))
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

  ;; plot food flow
  (def flow
    (letfn [(sum-counter-party-tonnes [counter-party]
              (->> food
                   (filter #(and (= "in" (:io-direction %))
                                 (= counter-party (:counter-party %))))
                   (map :tonnes)
                   (apply +)))

            (sum-subflows-tonnes [subflows]
              (->> subflows
                   (map #(nth % 2))
                   (apply +)))

            (sum-counter-parties-tonnes [counter-parties]
              (->> food
                   (filter #(and (= "out" (:io-direction %))
                                 (contains? counter-parties (:counter-party %))))
                   (map :tonnes)
                   (apply +)))]

      (let [source-keys               (->> food
                                           (filter #(= "in" (:io-direction %)))
                                           (map :counter-party)
                                           distinct)

            not-waste-sources         #{"Purchased" "Donated not waste"}
            waste-sources             (remove #(contains? not-waste-sources %) source-keys)

            used-as-food-outcomes     #{"Used as food"}
            not-used-as-food-outcomes #{"Donated to animal sanctuary" "Used by individuals for compost" "Council compost, Energen biogas, etc."}

            subflows-1a               (for [from waste-sources]
                                        [from "Would-be waste" (sum-counter-party-tonnes from)])

            subflows-1b               (for [from not-waste-sources]
                                        [from "Not waste" (sum-counter-party-tonnes from)])

            subflows-2                [["Not waste" "Stirling Community Food" (sum-subflows-tonnes subflows-1b)]
                                       ["Would-be waste" "Stirling Community Food" (sum-subflows-tonnes subflows-1a)]]

            subflows-3                [["Stirling Community Food" "Used as food" (sum-counter-parties-tonnes used-as-food-outcomes)]
                                       ["Stirling Community Food" "Not used as food" (sum-counter-parties-tonnes not-used-as-food-outcomes)]]

            subflows-4                (for [to not-used-as-food-outcomes]
                                        ["Not used as food" to (sum-counter-parties-tonnes #{to})])

                   ;; concat and order them
            
            ordered-froms             ["Purchased"
                                       "Donated not waste"
                                       "Local supermarkets"
                                       "Fareshare"
                                       "Donated as waste"
                                       "Other"
                                       "Not waste"
                                       "Would-be waste"
                                       "Stirling Community Food"]

            ordered-tos               ["Used as food" ;; should be no need to worry about the earlier ones in the flow
                                       "Not used as food"
                                       "Donated to animal sanctuary"
                                       "Used by individuals for compost"
                                       "Council compost, Energen biogas, etc."]

            comparator                (fn [[a-from a-to] [b-from b-to]] (if (not= a-from b-from)
                                                                          (< (.indexOf ordered-froms a-from) (.indexOf ordered-froms b-from))
                                                                          (< (.indexOf ordered-tos a-to) (.indexOf ordered-tos b-to))))
            
            flow                      (sort-by (juxt first second)
                                               comparator
                                               (concat subflows-1a subflows-1b subflows-2 subflows-3 subflows-4))]

        flow)))
  (def food-flow-chart-template
    {:chart         {:backgroundColor "#980f3d"}
     :navigation    {:buttonOptions {:enabled false}}
     :title         nil   ;{:text "The flow of food material"}
     :subtitle      nil   ;{:text "subtitle does here"}
     :accessibility {:point {:valueDescriptionFormat "{index}. {point.from} to {point.to}, {point.weight}."}}
     :tooltip       {:headerFormat nil
                     :pointFormat  "{point.fromNode.name} \u2192 {point.toNode.name}: {point.weight:.2f} tonnes"
                     :nodeFormat   "{point.name}: {point.sum:.2f} tonnes"}
     :plotOptions   {:sankey {;:label { :minFontSize 4}
                              :nodePadding 20
                              :dataLabels  {;:crop false
                              ;:overflow "allow"
                                            :allowOverlap true}}}
     :series        [{:keys         ["from" "to" "weight"]
                      :minLinkWidth 6
                      :label        {:minFontSize 6}
                      :nodes        [{:id    "Purchased"
                                      :color "#FFC473"}
                                     {:id    "Donated not waste"
                                      :color "#7158A1"}
                                     {:id    "Donated as waste"
                                      :color "#BF5748"}
                                     {:id    "Other"
                                      :color "#928E85"}
                                     {:id    "Local supermarkets"
                                      :color "#006CAE"}
                                     {:id    "Fareshare"
                                      :color "#59896A"}
                                     {:id    "Would-be waste"
                                      :color "#EF0606"}
                                     {:id    "Not waste"
                                      :color "#00C9A9"}
                                     {:id    "Stirling Community Food"
                                      :color "#009790"}
                                     {:id     "Used as food"
                                      :color  "#00AC8F"
                                      :level  4
                                      :offset -100}
                                     {:id     "Not used as food"
                                      :color  "#98B0A9"
                                      :offset 105}
                                     {:id     "Donated to animal sanctuary"
                                      :color  "#006AC7"
                                      :offset 280}
                                     {:id     "Used by individuals for compost"
                                      :color  "#B49531"
                                      :offset 280}
                                     {:id     "Council compost, Energen biogas, etc."
                                      :color  "#E27E44"
                                      :offset 280}]
                      :data         :PLACEHOLDER
                      :type         "sankey"
                      :name         "The flow of food material"}]
     :credits       {:enabled false}})
  (binding [*out* (io/writer "tmp/stirling-community-food/chart-9-food-flow.html")]
    (do
      (println "<script src=\"https://code.highcharts.com/highcharts.js\"></script>")
      (println "<script src=\"https://code.highcharts.com/modules/sankey.js\"></script>")
      (println "<script src=\"https://code.highcharts.com/modules/accessibility.js\"></script>")
      (println "<script>")
      (println "  document.addEventListener('DOMContentLoaded', function () {")
      (println "    const chart = Highcharts.chart('container', ")
      (json/pprint
       (-> food-flow-chart-template
           (assoc-in [:series 0 :data] flow)))
      (println "    );")
      (println "  });")
      (println "</script>")
      (println "<div id=\"container\" style=\"width:880px; height:400px;\"></div>")))

  )
  

  
