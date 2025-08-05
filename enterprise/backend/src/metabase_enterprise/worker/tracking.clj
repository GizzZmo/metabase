(ns metabase-enterprise.worker.tracking
  (:require
   [java-time.api :as t]
   [metabase.config.core :as config]
   [metabase.driver.sql-jdbc.execute :as sql-jdbc.execute])
  (:import
   (java.sql
    PreparedStatement)))

(set! *warn-on-reflection* true)

(defn- run-query! [driver conn-str sql params]
  (with-open [conn (java.sql.DriverManager/getConnection conn-str)
              stmt (sql-jdbc.execute/statement-or-prepared-statement driver
                                                                     conn
                                                                     sql
                                                                     params
                                                                     nil)]
    (let [rs (if (instance? PreparedStatement stmt)
               (.executeQuery ^PreparedStatement stmt)
               (.executeQuery stmt sql))
          rsmeta (.getMetaData rs)]
      (into [] (sql-jdbc.execute/reducible-rows driver rs rsmeta)))))

(defn- run-update! [driver conn-str sql params]
  (with-open [conn (java.sql.DriverManager/getConnection conn-str)
              stmt (sql-jdbc.execute/statement-or-prepared-statement driver
                                                                     conn
                                                                     sql
                                                                     params
                                                                     nil)]
    (if (instance? PreparedStatement stmt)
      (.executeUpdate ^PreparedStatement stmt)
      (.executeUpdate stmt sql))))

(defn track-start!
  [run-id mb-source]
  (-> (run-update! :postgres (config/config-str :mb-worker-db)
                   "
INSERT INTO worker_runs (run_id, source, status)
SELECT ?, ?, 'running'
WHERE NOT EXISTS (
    SELECT 1 FROM worker_runs WHERE run_id = ?
);"

                   [run-id mb-source run-id])
      (= 1))) ;; new?

(defn set-status!
  ([run-id status]
   (set-status! run-id status ""))
  ([run-id status note]
   (run-update! :postgres (config/config-str :mb-worker-db)
                "UPDATE worker_runs
                 SET status = ?, end_time = now(), note = ?
                 WHERE run_id = ?"
                [status note run-id])
   :ok))

(defn track-finish!
  [run-id]
  (set-status! run-id "success"))

(defn track-error!
  [run-id msg]
  (set-status! run-id "error" msg))

(def ^:private timeout-sec (* 4 60 60) #_sec)

(defn- handle-timeout
  [run]
  (if (and (= "running" (:status run))
           (> (:run-time run) timeout-sec))
    (-> run
        (assoc :status "timeout")
        (assoc :end-time (t/plus (:start-time run) (t/duration 4 :hours)))
        (assoc :note "Timed out.")
        (dissoc :run-time))
    (dissoc run :run-time)))

(defn get-status
  [run-id mb-source]
  (->> (run-query! :postgres (config/config-str :mb-worker-db)
                   "SELECT run_id, status, start_time, end_time, note, EXTRACT(EPOCH FROM now() - start_time) as run_time
                      FROM worker_runs
                      WHERE run_id = ? AND source = ?"
                   [run-id mb-source])
       first ;; row
       (zipmap [:run-id :status :start-time :end-time :note :run-time])
       handle-timeout
       not-empty))

(defn timeout-old-tasks
  []
  (run-update! :postgres (config/config-str :mb-worker-db)
               "UPDATE worker_runs
                  SET status = ?, end_time = NOW(), note = ?
                  WHERE status = 'running' AND start_time < NOW() - INTERVAL '4 hours'"
               ["timeout" "Timed out by worker"]))

(comment

  (let [driver :postgres
        sql "SELECT * from worker_runs"]

    (with-open [conn (java.sql.DriverManager/getConnection "jdbc:postgresql://localhost:5432/worker")]
      (with-open [stmt (sql-jdbc.execute/statement-or-prepared-statement driver
                                                                         conn
                                                                         sql
                                                                         []
                                                                         nil)]
        (let [rs (if (instance? PreparedStatement stmt)
                   (.executeQuery ^PreparedStatement stmt)
                   (.executeQuery stmt sql))
              rsmeta (.getMetaData rs)]
          (into [] (sql-jdbc.execute/reducible-rows driver rs rsmeta))))))

  (let [driver :postgres
        sql "insert into worker_runs (work_id, type, source, status) values (?, ?, ?, ?) RETURNING run_id"]

    (with-open [conn (java.sql.DriverManager/getConnection "jdbc:postgresql://localhost:5432/worker")]
      (with-open [stmt (sql-jdbc.execute/statement-or-prepared-statement driver
                                                                         conn
                                                                         sql
                                                                         [1, "sql-transform", "mb-1", "running"]
                                                                         nil)]
        (let [rs (if (instance? PreparedStatement stmt)
                   (.executeQuery ^PreparedStatement stmt)
                   (.executeQuery stmt sql))
              rsmeta (.getMetaData rs)]
          (into [] (sql-jdbc.execute/reducible-rows driver rs rsmeta)))))))

(comment

  (alter-var-root #'environ.core/env assoc :mb-worker-db "jdbc:postgresql://localhost:5432/worker")

  (def id (str (random-uuid)))
  (track-start! id "mb-1")
  (track-finish! id)
  (track-error! id "oops")

  (get-status id "mb-1"))
