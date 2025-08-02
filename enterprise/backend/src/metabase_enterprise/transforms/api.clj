(ns metabase-enterprise.transforms.api
  (:require
   [clojure.set :as set]
   [metabase-enterprise.transforms.execute :as transforms.execute]
   [metabase-enterprise.transforms.util :as transforms.util]
   [metabase.api.common :as api]
   [metabase.api.macros :as api.macros]
   [metabase.api.routes.common :refer [+auth]]
   [metabase.api.util.handlers :as handlers]
   [metabase.driver.util :as driver.u]
   [metabase.util.i18n :refer [deferred-tru]]
   [metabase.util.log :as log]
   [metabase.util.malli.registry :as mr]
   [metabase.util.malli.schema :as ms]
   [ring.util.response :as response]
   [toucan2.core :as t2]))

(set! *warn-on-reflection* true)

(mr/def ::transform-source
  [:map
   [:type [:= "query"]]
   [:query [:map [:database :int]]]])

(mr/def ::transform-target
  [:map
   [:type [:enum "table" "view"]]
   [:schema {:optional true} :string]
   [:name :string]])

(mr/def ::execution-trigger
  [:enum "none" "global-schedule"])

(comment
  ;; Examples
  [{:id 1
    :name "Gadget Products"
    :source {:type "query"
             :query {:database 1
                     :type "native",
                     :native {:query "SELECT * FROM PRODUCTS WHERE CATEGORY = 'Gadget'"
                              :template-tags {}}}}
    :target {:type "table"
             :schema "transforms"
             :name "gadget_products"}}]
  -)

(defn- source-database-id
  [transform]
  (-> transform :source :query :database))

(defn- check-database-feature
  [transform]
  (let [database (api/check-400 (t2/select-one :model/Database (source-database-id transform))
                                (deferred-tru "The source database cannot be found."))
        feature (transforms.util/required-database-feature transform)]
    (api/check-400 (driver.u/supports? (:engine database) feature database)
                   (deferred-tru "The database does not support the requested transform target type."))))

(api.macros/defendpoint :get "/"
  "Get a list of transforms."
  [_route-params
   _query-params]
  (api/check-superuser)
  (t2/select :model/Transform))

(api.macros/defendpoint :post "/"
  [_route-params
   _query-params
   body :- [:map
            [:name :string]
            [:description {:optional true} [:maybe :string]]
            [:source ::transform-source]
            [:target ::transform-target]
            [:execution_trigger {:optional true} ::execution-trigger]]]
  (api/check-superuser)
  (check-database-feature body)
  (when (transforms.util/target-table-exists? body)
    (api/throw-403))
  (t2/insert-returning-instance!
   :model/Transform (select-keys body [:name :description :source :target :execution_trigger])))

(api.macros/defendpoint :get "/:id"
  [{:keys [id]} :- [:map
                    [:id ms/PositiveInt]]]
  (log/info "get transform" id)
  (api/check-superuser)
  (let [{:keys [target] :as transform} (api/check-404 (t2/select-one :model/Transform id))
        database-id (source-database-id transform)
        target-table (transforms.util/target-table database-id target :active true)]
    (assoc transform :table target-table)))

(def ^:private status->status
  {:exec-succeeded :success
   :sync-succeeded :success
   :sync-failed    :success
   :exec-failed    :failed
   :started        :started})

(comment

  (ttt {:offset 0 :limit 20})

  (t2/hydrate (t2/select :model/WorkerRun) :transform))

(api.macros/defendpoint :get "/execution"
  [{:keys [offset
           limit
           sort_column
           sort_direction
           transform_id
           status]} :-
   [:map
    [:offset                          ms/IntGreaterThanOrEqualToZero]
    [:limit                           ms/IntGreaterThanOrEqualToZero]
    [:sort_column    {:optional true} [:enum "started_at" "ended_at"]]
    [:sort_direction {:optional true} [:enum "asc" "desc"]]
    [:transform_id   {:optional true} ms/IntGreaterThanOrEqualToZero]
    [:status         {:optional true} [:enum "started" "succeeded" "failed"]]]]
  (log/info "get executions")
  (api/check-superuser)
  (let [offset (or offset 0)
        limit  (or limit 20)
        sort-direction (or (keyword sort_direction) :desc)
        order-by (if sort_column
                   [[(keyword sort_column) sort-direction]]
                   [[:start_time (keyword sort-direction)]
                    [:end_time   (keyword sort-direction)]])
        conditions (concat [:work_type :transform]
                           (when transform_id
                             [:work_id transform_id])
                           (when status
                             [:status status]))
        conditions-with-sort-and-pagination (concat conditions [{:order-by order-by
                                                                 :offset offset
                                                                 :limit limit}])
        runs (apply t2/select :model/WorkerRun conditions-with-sort-and-pagination)]
    {:data (mapv (fn [run]
                   (-> run
                       (set/rename-keys {:run_id     :id
                                         :work_id    :transform_id
                                         :run_method :trigger
                                         :start_time :started_at
                                         :end_time   :ended_at})
                       (update :status status->status)))
                 (t2/hydrate runs :transform))
     :limit limit
     :offset offset
     :total (apply t2/count :model/WorkerRun conditions)}))

(api.macros/defendpoint :put "/:id"
  [{:keys [id]} :- [:map
                    [:id ms/PositiveInt]]
   _query-params
   body :- [:map
            [:name {:optional true} :string]
            [:description {:optional true} [:maybe :string]]
            [:source {:optional true} ::transform-source]
            [:target {:optional true} ::transform-target]
            [:execution_trigger {:optional true} ::execution-trigger]]]
  (log/info "put transform" id)
  (api/check-superuser)
  (let [old (t2/select-one :model/Transform id)
        new (merge old body)
        target-fields #(-> % :target (select-keys [:schema :name]))]
    (check-database-feature new)
    (when (and (not= (target-fields old) (target-fields new))
               (transforms.util/target-table-exists? new))
      (api/throw-403)))
  (t2/update! :model/Transform id body)
  (t2/select-one :model/Transform id))

(api.macros/defendpoint :delete "/:id"
  [{:keys [id]} :- [:map
                    [:id ms/PositiveInt]]]
  (log/info "delete transform" id)
  (api/check-superuser)
  (t2/delete! :model/Transform id)
  nil)

(api.macros/defendpoint :delete "/:id/table"
  [{:keys [id]} :- [:map
                    [:id ms/PositiveInt]]]
  (log/info "delete transform target table" id)
  (api/check-superuser)
  (transforms.util/delete-target-table-by-id! id)
  nil)

(api.macros/defendpoint :post "/:id/execute"
  [{:keys [id]} :- [:map
                    [:id ms/PositiveInt]]]
  (log/info "execute transform" id)
  (api/check-superuser)
  (let [transform (api/check-404 (t2/select-one :model/Transform id))]
    (future
      (transforms.execute/execute-mbql-transform! transform {:run-method :manual}))
    (-> (response/response {:message (deferred-tru "Transform execution started")})
        (assoc :status 202))))

(def ^{:arglists '([request respond raise])} routes
  "`/api/ee/transform` routes."
  (handlers/routes
   (api.macros/ns-handler *ns* +auth)))
