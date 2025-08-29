(ns metabase.query-processor.middleware.add-implicit-clauses
  "Middlware for adding an implicit `:order-by` clauses to certain queries."
  (:require
   [metabase.lib.core :as lib]
   [metabase.lib.metadata :as lib.metadata]
   [metabase.lib.schema :as lib.schema]
   [metabase.lib.util.match :as lib.util.match]
   [metabase.lib.walk :as lib.walk]
   [metabase.util.malli :as mu]))

(mu/defn- fix-order-by-field-refs :- ::lib.schema/stage
  "This function transforms top level integer field refs in order by to corresponding string field refs from breakout
  if present.

  ## Context
  In current situation, ie. model as a source, then aggregation and breakout, and finally order by a breakout field,
  [[metabase.lib.order-by/orderable-columns]] returns field ref with integer id, while reference to same field, but
  with string id is present in breakout. Then, [[add-implicit-breakout-order-by]] adds the string ref to order by.

  Resulting query would contain both references, while integral is transformed differently -- it contains no casting.
  As that is not part of group by, the query would fail.

  Reference: https://github.com/metabase/metabase/issues/44653."
  [query :- ::lib.schema/query
   ;; path  :- ::lib.walk/path
   {breakouts :breakout, :keys [order-by], :as stage} :- ::lib.schema/stage]
  (if (or (empty? breakouts) (empty? order-by))
    stage
    (let [name->breakout (into {}
                               (keep (fn [[tag _opts id-or-name :as clause]]
                                       (when (and (= :field tag)
                                                  (string? id-or-name))
                                         [id-or-name clause])))
                               breakouts)
          ref->maybe-field-name (fn [[tag _opts id-or-name]]
                                  (when (and (= :field tag)
                                             (integer? id-or-name))
                                    ((some-fn :lib/desired-column-alias :name)
                                     (lib.metadata/field query id-or-name))))
          maybe-convert-order-by-ref (fn [[dir opts a-ref :as order-by-elm]]
                                       (if-some [breakout (-> a-ref ref->maybe-field-name name->breakout)]
                                         [dir opts (lib/fresh-uuids breakout)]
                                         order-by-elm))]
      (update stage :order-by (partial mapv maybe-convert-order-by-ref)))))

(defn- has-window-function-aggregations? [stage]
  (or (lib.util.match/match (mapcat stage [:aggregation :expressions])
        #{:cum-sum :cum-count :offset}
        true)
      (when-let [source-query (:source-query stage)]
        (has-window-function-aggregations? source-query))))

(mu/defn- add-order-bys-for-breakouts* :- ::lib.schema/stage
  "Fields specified in `breakout` should add an implicit ascending `order-by` subclause *unless* that Field is already
  *explicitly* referenced in `order-by`."
  [query :- ::lib.schema/query
   path  :- ::lib.walk/path
   stage :- ::lib.schema/stage]
  ;; Add a new [:asc <breakout-field>] clause for each breakout. The cool thing is `add-order-by-clause` will
  ;; automatically ignore new ones that are reference Fields already in the order-by clause
  (let [{breakouts :breakout, :as stage} (fix-order-by-field-refs query stage)]
    (-> (lib.walk/apply-f-for-stage-at-path
         (fn [query stage-number]
           (reduce
            (fn [query orderable]
              (lib/order-by query stage-number (lib/fresh-uuids orderable) :asc))
            query
            (when-not (has-window-function-aggregations? stage)
              breakouts)))
         query
         path)
        (get-in (take-last 2 path)))))

(mu/defn add-order-bys-for-breakouts :- ::lib.schema/query
  "Add implicit `:order-by` clauses for fields specified in a `:breakout`."
  [query :- ::lib.schema/query]
  (lib.walk/walk-stages query add-order-bys-for-breakouts*))
