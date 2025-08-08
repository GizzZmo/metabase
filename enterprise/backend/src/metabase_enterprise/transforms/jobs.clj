(ns metabase-enterprise.transforms.jobs
  (:require
   [clojure.string :as str]
   [metabase-enterprise.transforms.execute :as transforms.execute]
   [metabase-enterprise.transforms.models.job-run :as transforms.job-run]
   [metabase-enterprise.transforms.ordering :as transforms.ordering]
   [metabase.util :as u]
   [metabase.util.log :as log]
   [toucan2.core :as t2]))

(defn- get-deps [ordering transform-ids]
  (loop [found #{}
         [current-transform & more-transforms] transform-ids]
    (if current-transform
      (recur (conj found current-transform)
             (if (found current-transform)
               more-transforms
               (into more-transforms (get ordering current-transform))))
      found)))

(defn- get-plan [transform-ids]
  (let [all-transforms (t2/select :model/Transform)
        global-ordering (transforms.ordering/transform-ordering all-transforms)
        relevant-ids (get-deps global-ordering transform-ids)
        ordering (select-keys global-ordering relevant-ids)]
    (when-let [cycle (transforms.ordering/find-cycle ordering)]
      (let [id->name (into {} (map (juxt :id :name)) all-transforms)]
        (throw (ex-info (str "Cyclic transform definitions detected: "
                             (str/join " → " (map id->name cycle)))
                        {:cycle cycle}))))
    {:transforms-by-id (into {}
                             (keep (fn [{:keys [id] :as transform}]
                                     (when (relevant-ids id)
                                       [id transform])))
                             all-transforms)
     :ordering ordering}))

(defn- next-transform [{:keys [ordering transforms-by-id]} complete]
  (-> (transforms.ordering/available-transforms ordering #{} complete)
      first
      transforms-by-id))

(defn execute-transforms! [transform-ids-to-run {:keys [run-method start-promise]}]
  (let [plan (get-plan transform-ids-to-run)]
    (when start-promise
      (deliver start-promise :started))
    (loop [complete #{}]
      (when-let [current-transform (next-transform plan complete)]
        (log/info "Executing job transform" (pr-str (:id current-transform)))
        (transforms.execute/execute-mbql-transform! current-transform {:run-method run-method})
        (recur (conj complete (:id current-transform)))))))

(defn execute-job!
  [job-id {:keys [run-method] :as opts}]
  (let [transforms (t2/select-fn-set :transform_id
                                     :transform_job_tags
                                     {:select :transform_tags.transform_id
                                      :from :transform_job_tags
                                      :left-join [:transform_tags [:=
                                                                   :transform_tags.tag_id
                                                                   :transform_job_tags.tag_id]]
                                      :where [:= :transform_job_tags.job_id job-id]})
        run-id (str (u/generate-nano-id))]
    (log/info "Executing transform job" (pr-str job-id) "with transforms" (pr-str transforms))
    (transforms.job-run/start-run! run-id job-id run-method)
    (try
      (execute-transforms! transforms opts)
      (transforms.job-run/succeed-started-run! run-id)
      (catch Throwable t
        (transforms.job-run/fail-started-run! run-id {:message (.getMessage t)})
        (throw t)))))
