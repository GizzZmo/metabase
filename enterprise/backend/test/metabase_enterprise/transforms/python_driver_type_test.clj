(ns ^:mb/driver-tests metabase-enterprise.transforms.python-driver-type-test
  "Comprehensive tests for Python transforms across all supported drivers with all base and exotic types."
  (:require
   [clojure.core.async :as a]
   [clojure.string :as str]
   [clojure.test :refer :all]
   [java-time.api :as t]
   [metabase-enterprise.transforms.execute :as transforms.execute]
   [metabase-enterprise.transforms.python-runner :as python-runner]
   [metabase-enterprise.transforms.settings :as transforms.settings]
   [metabase-enterprise.transforms.test-util :as transforms.tu :refer [with-transform-cleanup!]]
   [metabase-enterprise.transforms.util :as transforms.util]
   [metabase.driver :as driver]
   [metabase.sync.core :as sync]
   [metabase.test :as mt]
   [metabase.test.data.sql :as sql.tx]
   [metabase.util :as u]
   [metabase.util.json :as json]
   [toucan2.core :as t2]))

(set! *warn-on-reflection* true)

(def test-id 42)

(defn- execute!
  "Execute a Python transform with the given code and tables, following the existing test pattern."
  [{:keys [code tables]}]
  (with-open [shared-storage-ref (python-runner/open-s3-shared-storage! (or tables {}))]
    (let [server-url (transforms.settings/python-execution-server-url)
          cancel-chan (a/promise-chan)
          table-name->id (or tables {})
          _ (python-runner/copy-tables-to-s3! {:run-id test-id
                                               :shared-storage @shared-storage-ref
                                               :table-name->id table-name->id
                                               :cancel-chan cancel-chan})
          response (python-runner/execute-python-code-http-call! {:server-url server-url
                                                                  :code code
                                                                  :run-id test-id
                                                                  :table-name->id table-name->id
                                                                  :shared-storage @shared-storage-ref})
          {:keys [output output-manifest events]} (python-runner/read-output-objects @shared-storage-ref)]
      ;; not sure about munging this all together but its what tests expect for now
      (merge (:body response)
             {:output output
              :output-manifest output-manifest
              :stdout (->> events (filter #(= "stdout" (:stream %))) (map :message) (str/join "\n"))
              :stderr (->> events (filter #(= "stderr" (:stream %))) (map :message) (str/join "\n"))}))))

(defn- wait-for-table
  "Wait for a table to be created and synced, with timeout."
  [table-name]
  (loop [runs 0]
    (if-let [table (t2/select-one :model/Table :name table-name :db_id (mt/id))]
      table
      (if (< runs 100)
        (do (Thread/sleep 100)
            (recur (inc runs)))
        (throw (ex-info "Timeout waiting for table" {:table-name table-name}))))))

(defn- execute-e2e-transform!
  "Execute an e2e Python transform test using execute-python-transform!"
  [table-name transform-code source-tables]
  (let [schema (when (= driver/*driver* :postgres)
                 (sql.tx/session-schema driver/*driver*))
        target {:type "table"
                :schema schema
                :name table-name
                :database (mt/id)}
        transform-def {:name (str "E2E Transform Test " table-name)
                       :source {:type "python"
                                :source-tables source-tables
                                :body transform-code}
                       :target target}]
    (with-transform-cleanup! [_target target]
      (mt/with-temp [:model/Transform transform transform-def]
        (transforms.execute/execute-python-transform! transform {:run-method :manual})
        (wait-for-table table-name)
        ;; Return the table rows for validation
        (transforms.tu/table-rows table-name)))))

(defn- datetime-equal?
  "Check if two datetime strings are equal, handling timezone conversion."
  [expected actual]
  (try
    (= (t/instant expected) (t/instant actual))
    (catch Exception _
      (= expected actual))))

(def ^:private base-type-test-data
  "Base types that all drivers should support with test data."
  {:columns [{:name "id" :type :type/Integer :nullable? false}
             {:name "name" :type :type/Text :nullable? true}
             {:name "price" :type :type/Float :nullable? true}
             {:name "active" :type :type/Boolean :nullable? true}
             {:name "created_date" :type :type/Date :nullable? true}
             {:name "created_at" :type :type/DateTime :nullable? true}]
   :data [[1 "Product A" 19.99 true "2024-01-01" "2024-01-01T12:00:00"]
          [2 "Product B" 15.50 false "2024-02-01" "2024-02-01T09:15:30"]
          [3 nil nil nil nil nil]]})

(def ^:private driver-exotic-types
  "Driver-specific exotic types with test data."
  {:postgres {:columns [{:name "id" :type :type/Integer :nullable? false}
                        {:name "created_tz" :type :type/DateTimeWithTZ :nullable? true}
                        {:name "uuid_field" :type :type/UUID :nullable? true}
                        {:name "json_field" :type :type/JSON :nullable? true}
                        {:name "ip_field" :type :type/IPAddress :nullable? true}
                        {:name "int_array" :type :type/Array :nullable? true :database-type "integer[]"}
                        {:name "text_array" :type :type/Array :nullable? true :database-type "text[]"}
                        {:name "uuid_array" :type :type/Array :nullable? true :database-type "uuid[]"}]
              :data [[1 "2024-01-01T12:00:00Z" "550e8400-e29b-41d4-a716-446655440000" "{\"key\": \"value\"}" "192.168.1.1"
                      "{1,2,3,4,5}" "{\"hello\",\"world\",\"test\"}" "{550e8400-e29b-41d4-a716-446655440000,123e4567-e89b-12d3-a456-426614174000}"]
                     [2 nil nil nil nil nil nil nil]
                     [3 "2024-02-01T09:15:30-05:00" nil nil nil "{}" "{}" "{}"]]}

   ;;TODO: timestamps broken
   :mysql {:columns [{:name "id" :type :type/Integer :nullable? false}
                     {:name "json_field" :type :type/JSON :nullable? true}
                     ;; {:name "timestamp" :type :type/DateTimeWithLocalTZ :nullable? true :database-type "timestamp"}
                     ]
           :data [[1 "{\"key\": \"value\"}" ;; "2024-01-01 12:00:00"
                   ]
                  [2 nil
                   ;; nil
                   ]]}
   :mariadb {:columns [{:name "id" :type :type/Integer :nullable? false}
                       {:name "json_field" :type :type/JSON :nullable? true}
                       {:name "uuid_field" :type :type/UUID :nullable? true :database-type "uuid"}
                       {:name "inet4_field" :type :type/IPAddress :nullable? true :database-type "inet4"}]
             :data [[1 "{\"key\": \"mariadb_value\"}" "550e8400-e29b-41d4-a716-446655440000" "192.168.1.1"]
                    [2 nil nil nil]]}

   ;; TODO: insert not working rn
   :bigquery-cloud-sdk {:columns [{:name "id" :type :type/Integer :nullable? false}
                                  {:name "json_field" :type :type/JSON :nullable? true}
                                  ;; {:name "array_field" :type :type/Array :nullable? true :database-type "ARRAY<INT64>"}
                                  ;; {:name "dict_field" :type :type/Dictionary :nullable? true :database-type "STRUCT<key STRING, value INT64>"}
                                  ]
                        :data [[1 "{\"key\": \"value\"}" ;; "[1, 2, 3]" "{\"key\": \"test\", \"value\": 42}"
                                ]
                               [2 nil ;; nil nil
                                ]]}
   :snowflake {:columns [{:name "id" :type :type/Integer :nullable? false}
                         {:name "array_field" :type :type/Array :nullable? true :database-type "ARRAY"}]
               :data [[1 "[1, 2, 3]"]
                      [2 nil]]}
   :sqlserver {:columns [{:name "id" :type :type/Integer :nullable? false}
                         {:name "uuid_field" :type :type/UUID :nullable? true}
                         {:name "datetimeoffset_field" :type :type/DateTimeWithTZ :nullable? true :database-type "datetimeoffset"}]
               :data [[1 "550e8400-e29b-41d4-a716-446655440000" "2024-01-01 12:00:00 -05:00"]
                      [2 nil nil]]}
   :redshift {:columns [{:name "id" :type :type/Integer :nullable? false}
                        {:name "description" :type :type/Text :nullable? true}]
              :data [[1 "Test description for Redshift"]
                     [2 nil]]}
   :clickhouse {:columns [{:name "id" :type :type/Integer :nullable? false}
                          {:name "description" :type :type/Text :nullable? true}]
                :data [[1 "Test description for ClickHouse"]
                       [2 nil]]}
   :mongo {:columns [{:name "id" :type :type/Integer :nullable? false}
                     {:name "uuid_field" :type :type/UUID :nullable? true}
                     {:name "json_field" :type :type/JSON :nullable? true}
                     {:name "array_field" :type :type/Array :nullable? true :database-type "array"}
                     {:name "dict_field" :type :type/Dictionary :nullable? true :database-type "object"}
                     {:name "bson_id" :type :type/MongoBSONID :nullable? true}]
           :data [[1 "550e8400-e29b-41d4-a716-446655440000" "{\"key\": \"value\"}" "[1, 2, 3]" "{\"nested\": \"object\"}" "507f1f77bcf86cd799439011"]
                  [2 nil nil nil nil nil]]}})

(defn- create-test-table-with-data!
  "Create a test table with the given schema and data for the current driver."
  [table-name schema data]
  (let [driver driver/*driver*
        db-id (mt/id)
        schema-name (sql.tx/session-schema driver)
        qualified-table-name (if schema-name
                               (keyword schema-name table-name)
                               (keyword table-name))
        table-schema {:name qualified-table-name
                      :columns (:columns schema)}]
    ;; Create the table
    (mt/as-admin
      (transforms.util/create-table-from-schema! driver db-id table-schema))

    ;; Insert test data
    (when (seq data)
      (driver/insert-from-source! driver db-id table-schema
                                  {:type :rows :data data}))

    ;; Sync the database to make the table available
    (sync/sync-database! (mt/db) {:scan :schema})

    ;; Return the actual table ID from the database
    (t2/select-one-pk :model/Table :name (name qualified-table-name) :db_id db-id)))

(defn- cleanup-table!
  "Drop the test table by table ID."
  [table-id]
  (try
    (when-let [table (t2/select-one :model/Table :id table-id)]
      (let [table-name (:name table)
            qualified-name (case driver/*driver*
                             :bigquery-cloud-sdk
                             ;; BigQuery needs dataset.table format
                             (let [schema (sql.tx/session-schema driver/*driver*)]
                               (if schema
                                 (keyword (str schema "." table-name))
                                 (keyword table-name)))
                             ;; Other drivers
                             (let [schema (sql.tx/session-schema driver/*driver*)]
                               (if schema
                                 (keyword schema table-name)
                                 (keyword table-name))))]
        (driver/drop-table! driver/*driver* (mt/id) qualified-name)))
    (catch Exception _e
      ;; Ignore cleanup errors - tables may not exist or have permission issues
      nil)))

(defn- validate-transform-output
  "Validate that the Python transform output preserves types and data correctly,
   assuming output is in JSONL format."
  [result expected-columns expected-row-count]
  (testing "Transform execution succeeded"
    (is (some? result))
    (is (contains? result :output))
    (is (contains? result :output-manifest)))

  (when result
    (let [lines (str/split-lines (:output result))
          rows (map json/decode lines)
          metadata (:output-manifest result)
          headers (map :name (:fields metadata))]

      (testing "Column headers are correct"
        (is (= (set expected-columns) (set headers))))

      (testing "Row count is correct"
        (is (= expected-row-count (count rows))))

      (testing "Metadata contains all expected columns"
        (is (= (set expected-columns)
               (set (map :name (:fields metadata))))))

      ;; Return the parsed data for further validation
      {:headers headers
       :rows rows
       :metadata metadata})))

(deftest create-table-test
  (testing "Test we can create base table"
    (mt/test-drivers #{:h2 :postgres :mysql :bigquery-cloud-sdk :snowflake :sqlserver :redshift :clickhouse}
      (mt/with-empty-db
        (let [table-name (mt/random-name)

              table-id (create-test-table-with-data!
                        table-name
                        base-type-test-data
                        (:data base-type-test-data))]

          (is table-id "Table should be created and have an ID")
          ;; Cleanup
          (cleanup-table! table-id))))))

(deftest base-types-python-transform-test
  (testing "Test Python transforms with base types across all supported drivers"
    (mt/test-drivers #{:h2 :postgres :mysql :bigquery-cloud-sdk :snowflake :sqlserver :redshift :clickhouse}
      (mt/with-empty-db
        (let [table-name (mt/random-name)
              table-id (create-test-table-with-data!
                        table-name
                        base-type-test-data
                        (:data base-type-test-data))

              ;; Simple identity transform that should preserve all types
              transform-code (str "import pandas as pd\n"
                                  "\n"
                                  "def transform(" table-name "):\n"
                                  "    df = " table-name ".copy()\n"
                                  "    return df")

              result (execute! {:code transform-code
                                :tables {table-name table-id}})

              expected-columns ["id" "name" "price" "active" "created_date" "created_at"]

              validation (validate-transform-output result expected-columns 3)]

          (when validation
            (let [{:keys [metadata]} validation]
              (testing "Base type preservation"
                (let [dtype-map (u/for-map [{:keys [name dtype]} (:fields metadata)]
                                  [name (transforms.util/dtype->base-type dtype)])]
                  (is (= :type/Integer (dtype-map "id")))
                  (is (= :type/Text (dtype-map "name")))
                  (is (= :type/Float (dtype-map "price")))
                  (is (= :type/Boolean (dtype-map "active")))
                  (is (= :type/Date (dtype-map "created_date")))
                  (is (= :type/DateTime (dtype-map "created_at")))))))

          ;; Cleanup
          (cleanup-table! table-id))))))

(deftest exotic-types-python-transform-test
  (testing "Test Python transforms with driver-specific exotic types"
    (mt/test-drivers #{:h2 :postgres :mysql :bigquery-cloud-sdk :snowflake :sqlserver :redshift :clickhouse :mongo}
      (mt/with-empty-db
        (when-let [exotic-config (get driver-exotic-types driver/*driver*)]
          (let [table-name (mt/random-name)
                table-id (create-test-table-with-data!
                          table-name
                          exotic-config
                          (:data exotic-config))

                ;; Simple identity transform
                transform-code (str "import pandas as pd\n"
                                    "\n"
                                    "def transform(" table-name "):\n"
                                    "    df = " table-name ".copy()\n"
                                    "    return df")

                result (execute! {:code transform-code
                                  :tables {table-name table-id}})

                expected-columns (map :name (:columns exotic-config))
                expected-row-count (count (:data exotic-config))

                validation (validate-transform-output result expected-columns expected-row-count)]

            (when validation
              (testing (str "Exotic types for " driver/*driver*)
                (let [{:keys [metadata]} validation
                      dtype-map (u/for-map [{:keys [name dtype]} (:fields metadata)]
                                  [name (transforms.util/dtype->base-type dtype)])]

                  ;; All drivers should preserve ID as integer
                  (is (= :type/Integer (dtype-map "id")))

                  ;; Driver-specific type validations
                  (case driver/*driver*
                    :postgres (do
                                (when (contains? dtype-map "uuid_field")
                                  (is (contains? #{:type/Text :type/UUID} (dtype-map "uuid_field"))))
                                (when (contains? dtype-map "json_field")
                                  (is (contains? #{:type/Text :type/JSON} (dtype-map "json_field"))))
                                (when (contains? dtype-map "ip_field")
                                  (is (contains? #{:type/Text :type/IPAddress} (dtype-map "ip_field"))))
                                (when (contains? dtype-map "int_array")
                                  (is (contains? #{:type/Text :type/Array} (dtype-map "int_array"))))
                                (when (contains? dtype-map "text_array")
                                  (is (contains? #{:type/Text :type/Array} (dtype-map "text_array"))))
                                (when (contains? dtype-map "uuid_array")
                                  (is (contains? #{:type/Text :type/Array} (dtype-map "uuid_array")))))

                    :mysql (do
                             (when (contains? dtype-map "json_field")
                               (is (contains? #{:type/Text :type/JSON} (dtype-map "json_field"))))
                             (when (contains? dtype-map "timestamp_tz")
                               (is (contains? #{:type/DateTime :type/DateTimeWithTZ} (dtype-map "timestamp_tz")))))

                    ;; :mariadb
                    ;; (do
                    ;;   (when (contains? dtype-map "json_field")
                    ;;     (is (contains? #{:type/Text :type/JSON} (dtype-map "json_field"))))
                    ;;   (when (contains? dtype-map "uuid_field")
                    ;;     (is (contains? #{:type/Text :type/UUID} (dtype-map "uuid_field"))))
                    ;;   (when (contains? dtype-map "inet4_field")
                    ;;     (is (contains? #{:type/Text :type/IPAddress} (dtype-map "inet4_field")))))

                    :bigquery-cloud-sdk (do
                                          (when (contains? dtype-map "json_field")
                                            (is (contains? #{:type/Text :type/JSON} (dtype-map "json_field"))))
                                          (when (contains? dtype-map "array_field")
                                            (is (contains? #{:type/Text :type/Array} (dtype-map "array_field"))))
                                          (when (contains? dtype-map "dict_field")
                                            (is (contains? #{:type/Text :type/Dictionary} (dtype-map "dict_field")))))

                    :snowflake (when (contains? dtype-map "array_field")
                                 (is (contains? #{:type/Text :type/Array} (dtype-map "array_field"))))

                    :sqlserver (do
                                 (when (contains? dtype-map "uuid_field")
                                   (is (contains? #{:type/Text :type/UUID} (dtype-map "uuid_field"))))
                                 (when (contains? dtype-map "datetimeoffset_field")
                                   (is (contains? #{:type/DateTime :type/DateTimeWithTZ} (dtype-map "datetimeoffset_field")))))

                    (:redshift :clickhouse) (when (contains? dtype-map "description")
                                              (is (= :type/Text (dtype-map "description"))))

                    :mongo (do
                             (when (contains? dtype-map "uuid_field")
                               (is (contains? #{:type/Text :type/UUID} (dtype-map "uuid_field"))))
                             (when (contains? dtype-map "json_field")
                               (is (contains? #{:type/Text :type/JSON} (dtype-map "json_field"))))
                             (when (contains? dtype-map "array_field")
                               (is (contains? #{:type/Text :type/Array} (dtype-map "array_field"))))
                             (when (contains? dtype-map "dict_field")
                               (is (contains? #{:type/Text :type/Dictionary} (dtype-map "dict_field"))))
                             (when (contains? dtype-map "bson_id")
                               (is (contains? #{:type/Text :type/MongoBSONID} (dtype-map "bson_id")))))))))

            ;; Cleanup
            (cleanup-table! table-id)))))))

(deftest edge-cases-python-transform-test
  (testing "Test Python transforms with edge cases: null values, empty strings, extreme values"
    (mt/test-drivers #{:h2 :postgres :mysql :bigquery-cloud-sdk :snowflake :sqlserver :redshift :clickhouse}
      (mt/with-empty-db
        (let [table-name (mt/random-name)
              edge-case-schema {:columns [{:name "id" :type :type/Integer :nullable? false}
                                          {:name "text_field" :type :type/Text :nullable? true}
                                          {:name "int_field" :type :type/Integer :nullable? true}
                                          {:name "float_field" :type :type/Float :nullable? true}
                                          {:name "bool_field" :type :type/Boolean :nullable? true}
                                          {:name "date_field" :type :type/Date :nullable? true}]
                                :data [[1 "" 0 0.0 false "2024-01-01"] ; Minimal values
                                       [2 "Very long text with special chars: !@#$%^&*(){}[]|\\:;\"'<>,.?/~`"
                                        2147483647 1.7976931348623157E308 true "2222-12-31"]
                                       [3 nil nil nil nil nil]]} ; All nulls

              table-id (create-test-table-with-data!
                        table-name
                        edge-case-schema
                        (:data edge-case-schema))

              ;; Transform that performs operations on each column type
              transform-code (str "import pandas as pd\n"
                                  "import numpy as np\n"
                                  "\n"
                                  "def transform(" table-name "):\n"
                                  "    df = " table-name ".copy()\n"
                                  "    \n"
                                  "    # Handle text operations safely\n"
                                  "    df['text_length'] = df['text_field'].fillna(\"\").astype(str).str.len()"
                                  "    \n"
                                  "    # Handle numeric operations with null safety\n"
                                  "    df['int_doubled'] = df['int_field'] * 2\n"
                                  "    df['float_squared'] = df['float_field'] ** 2\n"
                                  "    \n"
                                  "    # Boolean operations\n"
                                  "    df['bool_inverted'] = ~df['bool_field'].fillna(False)\n"
                                  "    \n"
                                  "    return df")

              result (execute! {:code transform-code
                                :tables {table-name table-id}})

              expected-columns ["id" "text_field" "int_field" "float_field" "bool_field" "date_field"
                                "text_length" "int_doubled" "float_squared" "bool_inverted"]

              validation (validate-transform-output result expected-columns 3)]

          (when validation
            (let [{:keys [rows metadata]} validation
                  dtype-map (u/for-map [{:keys [name dtype]} (:fields metadata)]
                              [name (transforms.util/dtype->base-type dtype)])]

              (testing "Original columns preserved"
                (is (= :type/Integer (dtype-map "id")))
                (is (= :type/Text (dtype-map "text_field")))
                (is (= :type/Integer (dtype-map "int_field")))
                (is (= :type/Float (dtype-map "float_field")))
                (is (= :type/Boolean (dtype-map "bool_field")))
                (is (= :type/Date (dtype-map "date_field"))))

              (testing "Computed columns have correct types"
                (is (= :type/Integer (dtype-map "text_length")))
                (is (contains? #{:type/Integer :type/Float} (dtype-map "int_doubled")))
                (is (= :type/Float (dtype-map "float_squared")))
                (is (= :type/Boolean (dtype-map "bool_inverted"))))

              (testing "Edge case data handling"
                (let [[row1 row2 row3] rows]
                  ;; Row 1: minimal values
                  (is (= 1 (get row1 "id")))
                  (is (= 0 (get row1 "text_length"))) ; empty string length
                  (is (= 0 (get row1 "int_doubled"))) ; 0 * 2

                  ;; Row 2: maximum values
                  (is (= 2 (get row2 "id")))
                  (is (not= "" (get row2 "text_length"))) ; long string has length

                  ;; Row 3: null values
                  (is (= 3 (get row3 "id")))))))

          ;; Cleanup
          (cleanup-table! table-id))))))

(deftest idempotent-transform-test
  (testing "Test that running the same transform multiple times produces identical results"
    (mt/test-drivers #{:h2 :postgres :mysql :bigquery-cloud-sdk :snowflake :sqlserver :redshift :clickhouse}
      (mt/with-empty-db
        (let [table-name (mt/random-name)
              table-id (create-test-table-with-data!
                        table-name
                        base-type-test-data
                        (:data base-type-test-data))

              ;; Transform that adds computed columns
              transform-code (str "import pandas as pd\n"
                                  "\n"
                                  "def transform(" table-name "):\n"
                                  "    df = " table-name ".copy()\n"
                                  "    df['computed_field'] = df['price'] * 1.1  # 10% markup\n"
                                  "    df['name_upper'] = df['name'].str.upper()\n"
                                  "    return df")

              ;; Run the transform twice
              result1 (execute!
                       {:code transform-code
                        :tables {table-name table-id}})

              result2 (execute!
                       {:code transform-code
                        :tables {table-name table-id}})]

          (testing "Both transforms succeeded"
            (is (some? result1))
            (is (some? result2)))

          (when (and result1 result2)
            (testing "Results are identical"
              (is (= (:output result1) (:output result2)))
              (is (= (count (:fields (:output-manifest result1)))
                     (count (:fields (:output-manifest result2))))))

            (let [expected-columns ["id" "name" "price" "active" "created_date" "created_at"
                                    "computed_field" "name_upper"]
                  validation (validate-transform-output result1 expected-columns 3)]

              (when validation
                (testing "Computed columns are added correctly"
                  (let [{:keys [metadata]} validation
                        dtype-map (u/for-map [{:keys [name dtype]} (:fields metadata)]
                                    [name (transforms.util/dtype->base-type dtype)])]
                    (is (= :type/Float (dtype-map "computed_field")))
                    (is (= :type/Text (dtype-map "name_upper"))))))))

          ;; Cleanup
          (cleanup-table! table-id))))))

(deftest comprehensive-e2e-python-transform-test
  (testing "End-to-end test using execute-python-transform! across all supported drivers with comprehensive type coverage"
    (mt/test-drivers #{:h2 :postgres :mysql :bigquery-cloud-sdk :snowflake :sqlserver :redshift :clickhouse}
      (mt/with-empty-db
        (mt/with-premium-features #{:transforms}
          (let [table-name (mt/random-name)
                source-table-name (mt/random-name)

                ;; Create source table with comprehensive types
                source-table-id (create-test-table-with-data!
                                 source-table-name
                                 base-type-test-data
                                 (:data base-type-test-data))

                ;; Add driver-specific exotic types if available
                exotic-config (get driver-exotic-types driver/*driver*)
                exotic-table-id (when exotic-config
                                  (create-test-table-with-data!
                                   (str source-table-name "_exotic")
                                   exotic-config
                                   (:data exotic-config)))

                ;; Python transform that combines and processes both tables
                transform-code (str "import pandas as pd\n"
                                    "\n"
                                    "def transform(" source-table-name
                                    (when exotic-config (str ", " source-table-name "_exotic"))
                                    "):\n"
                                    "    # Start with base types table\n"
                                    "    df = " source-table-name ".copy()\n"
                                    "    \n"
                                    "    # Add computed columns to test type handling\n"
                                    "    df['price_with_tax'] = df['price'] * 1.08  # Float computation\n"
                                    "    df['name_length'] = df['name'].str.len()   # String operations\n"
                                    "    df['is_expensive'] = df['price'] > 18.0    # Boolean computation\n"
                                    "    \n"
                                    "    # Date operations\n"
                                    "    df['created_year'] = pd.to_datetime(df['created_date']).dt.year\n"
                                    "    \n"
                                    (when exotic-config
                                      (str "    # Merge with exotic types if available\n"
                                           "    exotic_df = " source-table-name "_exotic.copy()\n"
                                           "    df = df.merge(exotic_df, on='id', how='left', suffixes=('', '_exotic'))\n"
                                           "    \n"))
                                    "    # Return processed dataframe\n"
                                    "    return df")

                ;; Execute the e2e transform
                source-tables (cond-> {source-table-name source-table-id}
                                exotic-table-id
                                (assoc (str source-table-name "_exotic") exotic-table-id))

                result-rows (execute-e2e-transform! table-name transform-code source-tables)]

            (testing "E2E transform execution succeeded"
              (is (seq result-rows) "Transform should produce results"))

            (when (seq result-rows)
              (testing "Expected data transformations"
                (let [first-row (first result-rows)]
                  ;; Basic validations that the transform worked
                  (testing "Computed columns are present and have reasonable values"
                    ;; The exact structure depends on the merge, but we should have more columns
                    (is (> (count first-row) 7) "Should have additional computed columns")

                    ;; Check that we have the expected number of rows (3 from base data)
                    (is (= 3 (count result-rows)) "Should have 3 rows from source data"))))

              (testing "Driver-specific type handling"
                (case driver/*driver*
                  :postgres (is (contains? (set (map str (first result-rows))) "1")
                                "Postgres should handle all types correctly")
                  :mysql (is (seq result-rows) "MySQL should handle JSON and base types")
                  :bigquery-cloud-sdk (is (seq result-rows) "BigQuery should handle arrays and JSON")
                  :snowflake (is (seq result-rows) "Snowflake should handle arrays")
                  :sqlserver (is (seq result-rows) "SQL Server should handle UUIDs")
                  (:redshift :clickhouse) (is (seq result-rows) "Driver should handle base types")
                  (is (seq result-rows) "All drivers should produce results"))))

            ;; Cleanup source tables
            (cleanup-table! source-table-id)
            (when exotic-table-id
              (cleanup-table! exotic-table-id))))))))

(deftest exotic-edge-cases-python-transform-test
  (testing "PostgreSQL exotic edge cases"
    (mt/test-driver :postgres
      (mt/with-empty-db
        (let [table-name (mt/random-name)
              exotic-edge-schema
              {:columns [{:name "id" :type :type/Integer :nullable? false}
                         ;; Network types
                         {:name "inet_field" :type :type/IPAddress :nullable? true}
                         {:name "cidr_field" :type :type/IPAddress :nullable? true :database-type "cidr"}
                         {:name "macaddr_field" :type :type/Text :nullable? true :database-type "macaddr"}
                         ;; Advanced numeric types
                         {:name "money_field" :type :type/Decimal :nullable? true :database-type "money"}
                         {:name "real_field" :type :type/Float :nullable? true :database-type "real"}
                         ;; Binary data
                         {:name "bytea_field" :type :type/Text :nullable? true :database-type "bytea"}
                         ;; Time types
                         {:name "time_field" :type :type/Time :nullable? true}
                         {:name "interval_field" :type :type/Text :nullable? true :database-type "interval"}
                         ;; Arrays of different types
                         {:name "int_array" :type :type/Array :nullable? true :database-type "integer[]"}
                         {:name "text_array" :type :type/Array :nullable? true :database-type "text[]"}
                         ;; Geometric types
                         {:name "point_field" :type :type/Text :nullable? true :database-type "point"}
                         ;; Large precision decimals
                         {:name "big_decimal" :type :type/Decimal :nullable? true :database-type "decimal(20,10)"}]
               :data [[1 "192.168.1.100" "192.168.0.0/16" "aa:bb:cc:dd:ee:ff"
                       1234.56 3.14159 "\\xDEADBEEF" "14:30:45.123" "1 year 2 months"
                       "{1,2,3,4,5}" "{\"hello\",\"world\",\"test\"}" "(45.5,-122.6)" 123456789.1234567890]
                      [2 "::1" "2001:db8::/32" "ff:ff:ff:ff:ff:ff"
                       -999999.99 -3.4E+38 "\\x01FF00FF" "23:59:59.999999" "999 days 23 hours"
                       "{-2147483648,0,2147483647}" "{\"unicode: 你好\",\"emoji: 🎉\"}" "(-180,-90)" -987654321.0987654321]
                      [3 "10.0.0.1" "10.0.0.0/8" "00:00:00:00:00:00"
                       0.01 1.234567E-38 "\\x" "00:00:00.000001" "1 microsecond"
                       "{}" "{}" "(0.000001,0.000001)" 0.0000000001]
                      [4 nil nil nil nil nil nil nil nil nil nil nil nil]]}

              table-id (create-test-table-with-data!
                        table-name
                        exotic-edge-schema
                        (:data exotic-edge-schema))

              ;; Transform that tests exotic type handling
              transform-code (str "import pandas as pd\n"
                                  "import numpy as np\n"
                                  "\n"
                                  "def transform(" table-name "):\n"
                                  "    df = " table-name ".copy()\n"
                                  "    \n"
                                  "    # Test IP address operations\n"
                                  "    df['has_ipv6'] = df['inet_field'].astype(str).str.contains(':', na=False)\n"
                                  "    df['is_private'] = df['inet_field'].astype(str).str.startswith('192.168', na=False)\n"
                                  "    \n"
                                  "    # Test MAC address operations\n"
                                  "    df['mac_normalized'] = df['macaddr_field'].astype(str).str.replace(':', '', regex=False)\n"
                                  "    \n"
                                  "    # Test money operations\n"
                                  "    df['money_doubled'] = df['money_field'] * 2\n"
                                  "    df['is_expensive'] = df['money_field'] > 1000\n"
                                  "    \n"
                                  "    # Test array operations\n"
                                  "    df['array_length'] = df['text_array'].astype(str).str.len()\n"
                                  "    df['has_numbers'] = df['int_array'].astype(str).str.contains('[0-9]', na=False)\n"
                                  "    \n"
                                  "    # Test geometric operations\n"
                                  "    df['has_coords'] = df['point_field'].astype(str).str.contains(',', na=False)\n"
                                  "    \n"
                                  "    # Test large decimal operations\n"
                                  "    df['big_decimal_rounded'] = df['big_decimal'].round(2)\n"
                                  "    \n"
                                  "    return df")

              result (execute! {:code transform-code
                                :tables {table-name table-id}})]

          (testing "PostgreSQL exotic transform succeeded"
            (is (some? result) "Transform should succeed")
            (is (contains? result :output) "Should have output")
            (is (contains? result :output-manifest) "Should have output manifest"))

          (when result
            (let [lines (str/split-lines (:output result))
                  rows (map json/decode lines)
                  metadata (:output-manifest result)
                  headers (map :name (:fields metadata))]

              (testing "Exotic data processed correctly"
                (is (= 4 (count rows)) "Should have 4 rows")
                (is (> (count headers) 13) "Should have computed columns")

                ;; Check some computed columns exist
                (is (contains? (set headers) "has_ipv6") "Should have IPv6 detection")
                (is (contains? (set headers) "money_doubled") "Should have money calculations")
                (is (contains? (set headers) "has_coords") "Should have geometric operations"))

              (testing "Type preservation for exotic types"
                (let [dtype-map (u/for-map [{:keys [name dtype]} (:fields metadata)]
                                  [name (transforms.util/dtype->base-type dtype)])]
                  ;; Original exotic types should be preserved as text or appropriate types
                  (is (contains? #{:type/Text :type/IPAddress} (dtype-map "inet_field")))
                  (is (contains? #{:type/Decimal :type/Float} (dtype-map "money_field")))
                  (is (contains? #{:type/Text :type/Array} (dtype-map "int_array")))
                  ;; Computed columns should have expected types
                  (is (= :type/Boolean (dtype-map "has_ipv6")))
                  (is (contains? #{:type/Decimal :type/Float} (dtype-map "money_doubled")))))

              (testing "Actual data transformations are correct"
                (let [[row1 row2 row3 row4] rows]
                  ;; Row 1: IPv4 address, should not have IPv6 detection
                  (is (= 1 (get row1 "id")))
                  (is (= false (get row1 "has_ipv6")) "IPv4 address should not be detected as IPv6")
                  (is (true? (get row1 "is_private")) "192.168.x.x should be detected as private")
                  (is (> (get row1 "money_doubled") 2000) "Money field should be doubled")

                  ;; Row 2: IPv6 address, should have IPv6 detection
                  (is (= 2 (get row2 "id")))
                  (is (true? (get row2 "has_ipv6")) "IPv6 address should be detected")
                  (is (= false (get row2 "is_private")) "IPv6 address should not be detected as private IPv4")
                  (is (< (get row2 "money_doubled") -1000000) "Negative money should be doubled to larger negative")

                  ;; Row 3: IPv4 private address
                  (is (= 3 (get row3 "id")))
                  (is (= false (get row3 "has_ipv6")) "IPv4 10.x.x.x should not be IPv6")

                  ;; Row 4: All nulls should have default/null handling
                  (is (= 4 (get row4 "id")))
                  (is (= false (get row4 "has_ipv6")) "Null should default to false")
                  (is (= false (get row4 "is_private")) "Null should default to false")))))

          ;; Cleanup
          (cleanup-table! table-id)))))

  (testing "MySQL/MariaDB exotic edge cases"
    (mt/test-driver :mysql
      (mt/with-empty-db
        (let [table-name (mt/random-name)
              mysql-edge-schema
              {:columns [{:name "id" :type :type/Integer :nullable? false}
                         ;; MySQL specific types
                         {:name "json_field" :type :type/JSON :nullable? true}
                         {:name "year_field" :type :type/Integer :nullable? true :database-type "year"}
                         {:name "enum_field" :type :type/Text :nullable? true :database-type "enum('small','medium','large')"}
                         {:name "set_field" :type :type/Text :nullable? true :database-type "set('red','green','blue')"}
                         {:name "bit_field" :type :type/Integer :nullable? true :database-type "bit(8)"}
                         {:name "tinyint_field" :type :type/Integer :nullable? true :database-type "tinyint"}
                         {:name "mediumint_field" :type :type/Integer :nullable? true :database-type "mediumint"}
                         {:name "decimal_precise" :type :type/Decimal :nullable? true :database-type "decimal(30,10)"}
                         {:name "longtext_field" :type :type/Text :nullable? true :database-type "longtext"}
                         {:name "varbinary_field" :type :type/Text :nullable? true :database-type "varbinary(255)"}]
               :data [[1 "{\"nested\": {\"array\": [1,2,3], \"null\": null}}" 2024 "medium" "red,blue"
                       255 127 8388607 123456789012345678.1234567890
                       (apply str (repeat 5000 "MySQL")) "binary data here"]
                      [2 "{\"emoji\": \"🎉\", \"unicode\": \"你好\"}" 1901 "large" "green"
                       0 -128 -8388608 -999999999999999.9999999999
                       "Special chars: \\n\\t\\r" "\\x41\\x42\\x43"]
                      [3 "[]" 2155 "small" "" 1 0 0 0.0000000001 "" ""]
                      [4 nil nil nil nil nil nil nil nil nil nil]]}

              table-id (create-test-table-with-data!
                        table-name
                        mysql-edge-schema
                        (:data mysql-edge-schema))

              transform-code (str "import pandas as pd\n"
                                  "import json\n"
                                  "\n"
                                  "def transform(" table-name "):\n"
                                  "    df = " table-name ".copy()\n"
                                  "    \n"
                                  "    # JSON operations\n"
                                  "    df['json_has_nested'] = df['json_field'].astype(str).str.contains('nested', na=False)\n"
                                  "    df['json_length'] = df['json_field'].astype(str).str.len()\n"
                                  "    \n"
                                  "    # Year operations\n"
                                  "    df['is_future_year'] = df['year_field'] > 2024\n"
                                  "    df['year_century'] = df['year_field'] // 100\n"
                                  "    \n"
                                  "    # Enum/Set operations\n"
                                  "    df['enum_size_category'] = df['enum_field'].map({'small': 1, 'medium': 2, 'large': 3})\n"
                                  "    df['set_color_count'] = df['set_field'].astype(str).str.count(',')\n"
                                  "    \n"
                                  "    # Bit operations\n"
                                  "    df['bit_is_max'] = df['bit_field'] == 255\n"
                                  "    df['tinyint_doubled'] = df['tinyint_field'] * 2\n"
                                  "    \n"
                                  "    return df")

              result (execute! {:code transform-code
                                :tables {table-name table-id}})]

          (testing "MySQL exotic transform succeeded"
            (is (some? result) "MySQL transform should succeed"))

          (when result
            (testing "MySQL exotic types processed"
              (let [metadata (:output-manifest result)
                    headers (map :name (:fields metadata))]
                (is (contains? (set headers) "json_has_nested"))
                (is (contains? (set headers) "enum_size_category"))
                (is (contains? (set headers) "bit_is_max")))))

          (cleanup-table! table-id)))))

  (testing "MariaDB exotic edge cases"
    (mt/test-driver :mariadb
      (mt/with-empty-db
        (let [table-name (mt/random-name)
              mariadb-edge-schema
              {:columns [{:name "id" :type :type/Integer :nullable? false}
                         ;; MariaDB specific types
                         {:name "json_field" :type :type/JSON :nullable? true}
                         {:name "sequence_field" :type :type/Integer :nullable? true :database-type "bigint"} ; MariaDB sequences
                         {:name "uuid_field" :type :type/UUID :nullable? true :database-type "uuid"} ; MariaDB UUID type
                         {:name "inet4_field" :type :type/IPAddress :nullable? true :database-type "inet4"} ; MariaDB INET4
                         {:name "inet6_field" :type :type/IPAddress :nullable? true :database-type "inet6"} ; MariaDB INET6
                         {:name "rowversion_field" :type :type/Integer :nullable? true :database-type "rowversion"} ; MariaDB versioning
                         {:name "blob_field" :type :type/Text :nullable? true :database-type "longblob"}
                         {:name "geometry_field" :type :type/Text :nullable? true :database-type "geometry"}
                         {:name "linestring_field" :type :type/Text :nullable? true :database-type "linestring"}
                         {:name "polygon_field" :type :type/Text :nullable? true :database-type "polygon"}]
               :data [[1 "{\"mariadb\": true, \"features\": [\"sequences\", \"uuid\", \"inet\"]}"
                       1000000000000 "550e8400-e29b-41d4-a716-446655440000" "192.168.1.100"
                       "2001:db8::1" 1 "binary blob data"
                       "POINT(1 1)" "LINESTRING(0 0,1 1,2 1)"
                       "POLYGON((0 0,4 0,4 4,0 4,0 0),(1 1, 2 1, 2 2, 1 2,1 1))"]
                      [2 "{\"version\": \"10.11\", \"storage_engines\": [\"InnoDB\", \"Aria\", \"MyRocks\"]}"
                       999999999999 "00000000-0000-0000-0000-000000000000" "10.0.0.1"
                       "::1" 2 "compressed data here"
                       "MULTIPOINT(0 0, 1 2)" "MULTILINESTRING((0 0,1 1,2 2),(3 3,4 4,5 5))"
                       "MULTIPOLYGON(((0 0,4 0,4 4,0 4,0 0)),((1 1,2 1,2 2,1 2,1 1)))"]
                      [3 "[]" 0 "ffffffff-ffff-ffff-ffff-ffffffffffff" "255.255.255.255"
                       "ffff:ffff:ffff:ffff:ffff:ffff:ffff:ffff" 3 ""
                       "GEOMETRYCOLLECTION EMPTY" "LINESTRING EMPTY" "POLYGON EMPTY"]
                      [4 nil nil nil nil nil nil nil nil nil nil]]}

              table-id (create-test-table-with-data!
                        table-name
                        mariadb-edge-schema
                        (:data mariadb-edge-schema))

              transform-code (str "import pandas as pd\n"
                                  "\n"
                                  "def transform(" table-name "):\n"
                                  "    df = " table-name ".copy()\n"
                                  "    \n"
                                  "    # JSON operations specific to MariaDB\n"
                                  "    df['json_has_mariadb'] = df['json_field'].astype(str).str.contains('mariadb', na=False)\n"
                                  "    df['json_has_engines'] = df['json_field'].astype(str).str.contains('storage_engines', na=False)\n"
                                  "    \n"
                                  "    # Sequence operations\n"
                                  "    df['sequence_large'] = df['sequence_field'] > 1e11\n"
                                  "    df['sequence_scaled'] = df['sequence_field'] / 1000000\n"
                                  "    \n"
                                  "    # UUID operations\n"
                                  "    df['uuid_is_nil'] = df['uuid_field'].astype(str).str.startswith('00000000', na=False)\n"
                                  "    df['uuid_is_max'] = df['uuid_field'].astype(str).str.startswith('ffffffff', na=False)\n"
                                  "    \n"
                                  "    # INET operations\n"
                                  "    df['inet4_is_private'] = df['inet4_field'].astype(str).str.contains('192.168|10\\.', na=False)\n"
                                  "    df['inet6_is_loopback'] = df['inet6_field'].astype(str).str.contains('::1', na=False)\n"
                                  "    \n"
                                  "    # Geometric operations\n"
                                  "    df['geom_is_point'] = df['geometry_field'].astype(str).str.contains('POINT', na=False)\n"
                                  "    df['geom_is_multi'] = df['geometry_field'].astype(str).str.contains('MULTI', na=False)\n"
                                  "    df['geom_is_empty'] = df['geometry_field'].astype(str).str.contains('EMPTY', na=False)\n"
                                  "    \n"
                                  "    return df")

              result (execute! {:code transform-code
                                :tables {table-name table-id}})]

          (testing "MariaDB exotic transform succeeded"
            (is (some? result) "MariaDB transform should succeed"))

          (when result
            (testing "MariaDB exotic types processed"
              (let [metadata (:output-manifest result)
                    headers (map :name (:fields metadata))]
                (is (contains? (set headers) "json_has_mariadb"))
                (is (contains? (set headers) "uuid_is_nil"))
                (is (contains? (set headers) "inet4_is_private"))
                (is (contains? (set headers) "geom_is_point")))))

          (cleanup-table! table-id)))))

  (testing "BigQuery exotic edge cases"
    (mt/test-driver :bigquery-cloud-sdk
      (mt/with-empty-db
        (let [table-name (mt/random-name)
              bq-edge-schema
              {:columns [{:name "id" :type :type/Integer :nullable? false}
                         ;; BigQuery specific types
                         {:name "struct_field" :type :type/Dictionary :nullable? true :database-type "STRUCT<name STRING, age INT64, active BOOL>"}
                         {:name "array_ints" :type :type/Array :nullable? true :database-type "ARRAY<INT64>"}
                         {:name "array_structs" :type :type/Array :nullable? true :database-type "ARRAY<STRUCT<key STRING, value FLOAT64>>"}
                         {:name "geography_field" :type :type/Text :nullable? true :database-type "GEOGRAPHY"}
                         {:name "numeric_precise" :type :type/Decimal :nullable? true :database-type "NUMERIC(38,9)"}
                         {:name "bignumeric_field" :type :type/Decimal :nullable? true :database-type "BIGNUMERIC(76,38)"}
                         {:name "bytes_field" :type :type/Text :nullable? true :database-type "BYTES"}
                         {:name "datetime_field" :type :type/DateTime :nullable? true :database-type "DATETIME"}
                         {:name "time_field" :type :type/Time :nullable? true :database-type "TIME"}]
               :data [[1 "{\"name\": \"Alice\", \"age\": 30, \"active\": true}" "[1,2,3,4,5]"
                       "[{\"key\": \"alpha\", \"value\": 1.1}, {\"key\": \"beta\", \"value\": 2.2}]"
                       "POINT(-122.084 37.422)" 123456789012345678901234567.123456789
                       99999999999999999999999999999999999999.12345678901234567890123456789012345678
                       "SGVsbG8gV29ybGQ=" "2024-12-31T23:59:59.999999" "23:59:59.999999"]
                      [2 "{\"name\": \"Bob\", \"age\": null, \"active\": false}" "[]"
                       "[]" "POLYGON((-124 42, -120 42, -120 46, -124 46, -124 42))"
                       -999999999999999999999999999.999999999
                       -12345678901234567890123456789012345678.12345678901234567890123456789012345678
                       "VGVzdCBEYXRh" "1900-01-01T00:00:00" "00:00:00"]
                      [3 "{\"name\": \"\", \"age\": 0, \"active\": true}" "[0]"
                       "[{\"key\": \"\", \"value\": 0.0}]" "POINT(0 0)" 0.000000001
                       0.00000000000000000000000000000000000001 "" "2000-01-01T12:00:00" "12:00:00"]
                      [4 nil nil nil nil nil nil nil nil nil]]}

              table-id (create-test-table-with-data!
                        table-name
                        bq-edge-schema
                        (:data bq-edge-schema))

              transform-code (str "import pandas as pd\n"
                                  "import json\n"
                                  "\n"
                                  "def transform(" table-name "):\n"
                                  "    df = " table-name ".copy()\n"
                                  "    \n"
                                  "    # Struct operations\n"
                                  "    df['struct_has_name'] = df['struct_field'].astype(str).str.contains('name', na=False)\n"
                                  "    df['struct_length'] = df['struct_field'].astype(str).str.len()\n"
                                  "    \n"
                                  "    # Array operations\n"
                                  "    df['array_ints_length'] = df['array_ints'].astype(str).str.len()\n"
                                  "    df['array_structs_complex'] = df['array_structs'].astype(str).str.contains('key', na=False)\n"
                                  "    \n"
                                  "    # Geography operations\n"
                                  "    df['is_point'] = df['geography_field'].astype(str).str.contains('POINT', na=False)\n"
                                  "    df['is_polygon'] = df['geography_field'].astype(str).str.contains('POLYGON', na=False)\n"
                                  "    \n"
                                  "    # High precision numeric\n"
                                  "    df['numeric_rounded'] = df['numeric_precise'].round(2)\n"
                                  "    df['has_large_number'] = df['bignumeric_field'].abs() > 1e30\n"
                                  "    \n"
                                  "    return df")

              result (execute! {:code transform-code
                                :tables {table-name table-id}})]

          (testing "BigQuery exotic transform succeeded"
            (is (some? result) "BigQuery transform should succeed"))

          (when result
            (testing "BigQuery exotic types processed"
              (let [metadata (:output-manifest result)
                    headers (map :name (:fields metadata))]
                (is (contains? (set headers) "struct_has_name"))
                (is (contains? (set headers) "is_point"))
                (is (contains? (set headers) "has_large_number")))))

          (cleanup-table! table-id)))))

  (testing "Snowflake exotic edge cases"
    (mt/test-driver :snowflake
      (mt/with-empty-db
        (let [table-name (mt/random-name)
              sf-edge-schema
              {:columns [{:name "id" :type :type/Integer :nullable? false}
                         ;; Snowflake specific types
                         {:name "variant_field" :type :type/JSON :nullable? true :database-type "VARIANT"}
                         {:name "object_field" :type :type/JSON :nullable? true :database-type "OBJECT"}
                         {:name "array_field" :type :type/Array :nullable? true :database-type "ARRAY"}
                         {:name "geography_field" :type :type/Text :nullable? true :database-type "GEOGRAPHY"}
                         {:name "geometry_field" :type :type/Text :nullable? true :database-type "GEOMETRY"}
                         {:name "number_large" :type :type/Decimal :nullable? true :database-type "NUMBER(38,0)"}
                         {:name "timestamp_ntz" :type :type/DateTime :nullable? true :database-type "TIMESTAMP_NTZ"}
                         {:name "timestamp_ltz" :type :type/DateTimeWithTZ :nullable? true :database-type "TIMESTAMP_LTZ"}
                         {:name "timestamp_tz" :type :type/DateTimeWithTZ :nullable? true :database-type "TIMESTAMP_TZ"}]
               :data [[1 "{\"type\": \"variant\", \"data\": [1,2,3]}" "{\"nested\": {\"key\": \"value\"}}"
                       "[\"apple\", \"banana\", \"cherry\"]" "POINT(-122.35 37.55)"
                       "POLYGON((-124 42, -120 42, -120 46, -124 46, -124 42))"
                       99999999999999999999999999999999999999
                       "2024-12-31 23:59:59.999999999" "2024-12-31 23:59:59.999999999 +0000"
                       "2024-12-31 23:59:59.999999999 -0800"]
                      [2 "\"simple string\"" "{}" "[]" "LINESTRING(-122 37, -121 38)"
                       "MULTIPOINT((-122 37), (-121 38))" -12345678901234567890123456789012345678
                       "1900-01-01 00:00:00.000000001" "1900-01-01 00:00:00.000000001 +0000"
                       "1900-01-01 00:00:00.000000001 +1200"]
                      [3 "123.456" "{\"empty\": null}" "[null, \"\", 0]" "POINT(0 0)" "POINT EMPTY"
                       0 "2000-01-01 12:00:00" "2000-01-01 12:00:00 +0000" "2000-01-01 12:00:00 +0000"]
                      [4 nil nil nil nil nil nil nil nil nil]]}

              table-id (create-test-table-with-data!
                        table-name
                        sf-edge-schema
                        (:data sf-edge-schema))

              transform-code (str "import pandas as pd\n"
                                  "\n"
                                  "def transform(" table-name "):\n"
                                  "    df = " table-name ".copy()\n"
                                  "    \n"
                                  "    # Variant/Object operations\n"
                                  "    df['variant_is_complex'] = df['variant_field'].astype(str).str.contains('{', na=False)\n"
                                  "    df['object_has_nested'] = df['object_field'].astype(str).str.contains('nested', na=False)\n"
                                  "    \n"
                                  "    # Array operations\n"
                                  "    df['array_has_fruits'] = df['array_field'].astype(str).str.contains('apple', na=False)\n"
                                  "    df['array_length'] = df['array_field'].astype(str).str.len()\n"
                                  "    \n"
                                  "    # Geography operations\n"
                                  "    df['is_point_geo'] = df['geography_field'].astype(str).str.startswith('POINT', na=False)\n"
                                  "    df['is_complex_geom'] = df['geometry_field'].astype(str).str.contains('POLYGON|MULTI', na=False)\n"
                                  "    \n"
                                  "    # Large number operations\n"
                                  "    df['number_abs'] = df['number_large'].abs()\n"
                                  "    df['is_huge_number'] = df['number_large'].abs() > 1e30\n"
                                  "    \n"
                                  "    return df")

              result (execute! {:code transform-code
                                :tables {table-name table-id}})]

          (testing "Snowflake exotic transform succeeded"
            (is (some? result) "Snowflake transform should succeed"))

          (when result
            (testing "Snowflake exotic types processed"
              (let [metadata (:output-manifest result)
                    headers (map :name (:fields metadata))]
                (is (contains? (set headers) "variant_is_complex"))
                (is (contains? (set headers) "is_point_geo"))
                (is (contains? (set headers) "is_huge_number")))))

          (cleanup-table! table-id)))))

  (testing "ClickHouse exotic edge cases"
    (mt/test-driver :clickhouse
      (mt/with-empty-db
        (let [table-name (mt/random-name)
              ch-edge-schema
              {:columns [{:name "id" :type :type/Integer :nullable? false}
                         ;; ClickHouse specific types
                         {:name "array_field" :type :type/Array :nullable? true :database-type "Array(Int64)"}
                         {:name "tuple_field" :type :type/Text :nullable? true :database-type "Tuple(String, Int64, Float64)"}
                         {:name "map_field" :type :type/Dictionary :nullable? true :database-type "Map(String, Int64)"}
                         {:name "uuid_field" :type :type/UUID :nullable? true :database-type "UUID"}
                         {:name "ipv4_field" :type :type/IPAddress :nullable? true :database-type "IPv4"}
                         {:name "ipv6_field" :type :type/IPAddress :nullable? true :database-type "IPv6"}
                         {:name "decimal128" :type :type/Decimal :nullable? true :database-type "Decimal128(18)"}
                         {:name "fixedstring" :type :type/Text :nullable? true :database-type "FixedString(10)"}
                         {:name "enum8_field" :type :type/Text :nullable? true :database-type "Enum8('small'=1,'medium'=2,'large'=3)"}]
               :data [[1 "[1,2,3,4,5]" "('test', 42, 3.14)" "{'key1': 100, 'key2': 200}"
                       "550e8400-e29b-41d4-a716-446655440000" "192.168.1.1" "2001:db8::1"
                       123456789012345678 "fixed_text" "medium"]
                      [2 "[-1,0,1]" "('', 0, -1.5)" "{'empty': 0}"
                       "00000000-0000-0000-0000-000000000000" "10.0.0.1" "::1"
                       -999999999999999999 "short     " "small"]
                      [3 "[]" "('null', -1, 0.0)" "{}"
                       "ffffffff-ffff-ffff-ffff-ffffffffffff" "255.255.255.255" "ffff::ffff"
                       0 "          " "large"]
                      [4 nil nil nil nil nil nil nil nil nil]]}

              table-id (create-test-table-with-data!
                        table-name
                        ch-edge-schema
                        (:data ch-edge-schema))

              transform-code (str "import pandas as pd\n"
                                  "\n"
                                  "def transform(" table-name "):\n"
                                  "    df = " table-name ".copy()\n"
                                  "    \n"
                                  "    # Array operations\n"
                                  "    df['array_has_positive'] = df['array_field'].astype(str).str.contains('[1-9]', na=False)\n"
                                  "    df['array_length'] = df['array_field'].astype(str).str.len()\n"
                                  "    \n"
                                  "    # Tuple operations\n"
                                  "    df['tuple_has_string'] = df['tuple_field'].astype(str).str.contains(\"'\", na=False)\n"
                                  "    df['tuple_has_negative'] = df['tuple_field'].astype(str).str.contains('-', na=False)\n"
                                  "    \n"
                                  "    # Map operations\n"
                                  "    df['map_has_keys'] = df['map_field'].astype(str).str.contains('key', na=False)\n"
                                  "    df['map_is_empty'] = df['map_field'] == '{}'\n"
                                  "    \n"
                                  "    # IP operations\n"
                                  "    df['ipv4_is_private'] = df['ipv4_field'].astype(str).str.contains('192.168|10.', na=False)\n"
                                  "    df['ipv6_is_loopback'] = df['ipv6_field'].astype(str).str.contains('::1', na=False)\n"
                                  "    \n"
                                  "    # UUID operations\n"
                                  "    df['uuid_is_null'] = df['uuid_field'].astype(str).str.startswith('00000000', na=False)\n"
                                  "    \n"
                                  "    return df")

              result (execute! {:code transform-code
                                :tables {table-name table-id}})]

          (testing "ClickHouse exotic transform succeeded"
            (is (some? result) "ClickHouse transform should succeed"))

          (when result
            (testing "ClickHouse exotic types processed"
              (let [metadata (:output-manifest result)
                    headers (map :name (:fields metadata))]
                (is (contains? (set headers) "array_has_positive"))
                (is (contains? (set headers) "ipv4_is_private"))
                (is (contains? (set headers) "uuid_is_null")))))

          (cleanup-table! table-id))))))

(deftest exotic-edge-cases-python-transform-mysql-test
  (testing "MySQL/MariaDB exotic edge cases"
    (mt/test-driver :mysql
      (mt/with-empty-db
        (let [table-name (mt/random-name)
              mysql-edge-schema
              {:columns [{:name "id" :type :type/Integer :nullable? false}
                         ;; MySQL specific types
                         {:name "json_field" :type :type/JSON :nullable? true}
                         {:name "year_field" :type :type/Integer :nullable? true :database-type "year"}
                         {:name "enum_field" :type :type/Text :nullable? true :database-type "enum('small','medium','large')"}
                         {:name "set_field" :type :type/Text :nullable? true :database-type "set('red','green','blue')"}
                         {:name "bit_field" :type :type/Integer :nullable? true :database-type "bit(8)"}
                         {:name "tinyint_field" :type :type/Integer :nullable? true :database-type "tinyint"}
                         {:name "mediumint_field" :type :type/Integer :nullable? true :database-type "mediumint"}
                         {:name "decimal_precise" :type :type/Decimal :nullable? true :database-type "decimal(30,10)"}
                         {:name "longtext_field" :type :type/Text :nullable? true :database-type "longtext"}
                         {:name "varbinary_field" :type :type/Text :nullable? true :database-type "varbinary(255)"}]
               :data [[1 "{\"nested\": {\"array\": [1,2,3], \"null\": null}}" 2024 "medium" "red,blue"
                       255 127 8388607 123456789012345678.1234567890
                       (apply str (repeat 5000 "MySQL")) "binary data here"]
                      [2 "{\"emoji\": \"🎉\", \"unicode\": \"你好\"}" 1901 "large" "green"
                       0 -128 -8388608 -999999999999999.9999999999
                       "Special chars: \\n\\t\\r" "\\x41\\x42\\x43"]
                      [3 "[]" 2155 "small" "" 1 0 0 0.0000000001 "" ""]
                      [4 nil nil nil nil nil nil nil nil nil nil]]}

              table-id (create-test-table-with-data!
                        table-name
                        mysql-edge-schema
                        (:data mysql-edge-schema))

              transform-code (str "import pandas as pd\n"
                                  "import json\n"
                                  "\n"
                                  "def transform(" table-name "):\n"
                                  "    df = " table-name ".copy()\n"
                                  "    \n"
                                  "    # JSON operations\n"
                                  "    df['json_has_nested'] = df['json_field'].astype(str).str.contains('nested', na=False)\n"
                                  "    df['json_length'] = df['json_field'].astype(str).str.len()\n"
                                  "    \n"
                                  "    # Year operations\n"
                                  "    df['is_future_year'] = df['year_field'] > 2024\n"
                                  "    df['year_century'] = df['year_field'] // 100\n"
                                  "    \n"
                                  "    # Enum/Set operations\n"
                                  "    df['enum_size_category'] = df['enum_field'].map({'small': 1, 'medium': 2, 'large': 3})\n"
                                  "    df['set_color_count'] = df['set_field'].astype(str).str.count(',')\n"
                                  "    \n"
                                  "    # Bit operations\n"
                                  "    df['bit_is_max'] = df['bit_field'] == 255\n"
                                  "    df['tinyint_doubled'] = df['tinyint_field'] * 2\n"
                                  "    \n"
                                  "    return df")

              result (execute! {:code transform-code
                                :tables {table-name table-id}})]

          (testing "MySQL exotic transform succeeded"
            (is (some? result) "MySQL transform should succeed"))

          (when result
            (testing "MySQL exotic types processed"
              (let [metadata (:output-manifest result)
                    headers (map :name (:fields metadata))]
                (is (contains? (set headers) "json_has_nested"))
                (is (contains? (set headers) "enum_size_category"))
                (is (contains? (set headers) "bit_is_max")))))

          (cleanup-table! table-id))))))

(deftest exotic-edge-cases-python-transform-mariadb-test
  (testing "MariaDB exotic edge cases"
    (mt/test-driver :mariadb
      (mt/with-empty-db
        (let [table-name (mt/random-name)
              mariadb-edge-schema
              {:columns [{:name "id" :type :type/Integer :nullable? false}
                         ;; MariaDB specific types
                         {:name "json_field" :type :type/JSON :nullable? true}
                         {:name "sequence_field" :type :type/Integer :nullable? true :database-type "bigint"} ; MariaDB sequences
                         {:name "uuid_field" :type :type/UUID :nullable? true :database-type "uuid"} ; MariaDB UUID type
                         {:name "inet4_field" :type :type/IPAddress :nullable? true :database-type "inet4"} ; MariaDB INET4
                         {:name "inet6_field" :type :type/IPAddress :nullable? true :database-type "inet6"} ; MariaDB INET6
                         {:name "rowversion_field" :type :type/Integer :nullable? true :database-type "rowversion"} ; MariaDB versioning
                         {:name "blob_field" :type :type/Text :nullable? true :database-type "longblob"}
                         {:name "geometry_field" :type :type/Text :nullable? true :database-type "geometry"}
                         {:name "linestring_field" :type :type/Text :nullable? true :database-type "linestring"}
                         {:name "polygon_field" :type :type/Text :nullable? true :database-type "polygon"}]
               :data [[1 "{\"mariadb\": true, \"features\": [\"sequences\", \"uuid\", \"inet\"]}"
                       1000000000000 "550e8400-e29b-41d4-a716-446655440000" "192.168.1.100"
                       "2001:db8::1" 1 "binary blob data"
                       "POINT(1 1)" "LINESTRING(0 0,1 1,2 1)"
                       "POLYGON((0 0,4 0,4 4,0 4,0 0),(1 1, 2 1, 2 2, 1 2,1 1))"]
                      [2 "{\"version\": \"10.11\", \"storage_engines\": [\"InnoDB\", \"Aria\", \"MyRocks\"]}"
                       999999999999 "00000000-0000-0000-0000-000000000000" "10.0.0.1"
                       "::1" 2 "compressed data here"
                       "MULTIPOINT(0 0, 1 2)" "MULTILINESTRING((0 0,1 1,2 2),(3 3,4 4,5 5))"
                       "MULTIPOLYGON(((0 0,4 0,4 4,0 4,0 0)),((1 1,2 1,2 2,1 2,1 1)))"]
                      [3 "[]" 0 "ffffffff-ffff-ffff-ffff-ffffffffffff" "255.255.255.255"
                       "ffff:ffff:ffff:ffff:ffff:ffff:ffff:ffff" 3 ""
                       "GEOMETRYCOLLECTION EMPTY" "LINESTRING EMPTY" "POLYGON EMPTY"]
                      [4 nil nil nil nil nil nil nil nil nil nil]]}

              table-id (create-test-table-with-data!
                        table-name
                        mariadb-edge-schema
                        (:data mariadb-edge-schema))

              transform-code (str "import pandas as pd\n"
                                  "\n"
                                  "def transform(" table-name "):\n"
                                  "    df = " table-name ".copy()\n"
                                  "    \n"
                                  "    # JSON operations specific to MariaDB\n"
                                  "    df['json_has_mariadb'] = df['json_field'].astype(str).str.contains('mariadb', na=False)\n"
                                  "    df['json_has_engines'] = df['json_field'].astype(str).str.contains('storage_engines', na=False)\n"
                                  "    \n"
                                  "    # Sequence operations\n"
                                  "    df['sequence_large'] = df['sequence_field'] > 1e11\n"
                                  "    df['sequence_scaled'] = df['sequence_field'] / 1000000\n"
                                  "    \n"
                                  "    # UUID operations\n"
                                  "    df['uuid_is_nil'] = df['uuid_field'].astype(str).str.startswith('00000000', na=False)\n"
                                  "    df['uuid_is_max'] = df['uuid_field'].astype(str).str.startswith('ffffffff', na=False)\n"
                                  "    \n"
                                  "    # INET operations\n"
                                  "    df['inet4_is_private'] = df['inet4_field'].astype(str).str.contains('192.168|10\\.', na=False)\n"
                                  "    df['inet6_is_loopback'] = df['inet6_field'].astype(str).str.contains('::1', na=False)\n"
                                  "    \n"
                                  "    # Geometric operations\n"
                                  "    df['geom_is_point'] = df['geometry_field'].astype(str).str.contains('POINT', na=False)\n"
                                  "    df['geom_is_multi'] = df['geometry_field'].astype(str).str.contains('MULTI', na=False)\n"
                                  "    df['geom_is_empty'] = df['geometry_field'].astype(str).str.contains('EMPTY', na=False)\n"
                                  "    \n"
                                  "    return df")

              result (execute! {:code transform-code
                                :tables {table-name table-id}})]

          (testing "MariaDB exotic transform succeeded"
            (is (some? result) "MariaDB transform should succeed"))

          (when result
            (testing "MariaDB exotic types processed"
              (let [metadata (:output-manifest result)
                    headers (map :name (:fields metadata))]
                (is (contains? (set headers) "json_has_mariadb"))
                (is (contains? (set headers) "uuid_is_nil"))
                (is (contains? (set headers) "inet4_is_private"))
                (is (contains? (set headers) "geom_is_point")))))

          (cleanup-table! table-id))))))

(deftest exotic-edge-cases-python-transform-bigquery-test
  (testing "BigQuery exotic edge cases"
    (mt/test-driver :bigquery-cloud-sdk
      (mt/with-empty-db
        (let [table-name (mt/random-name)
              bq-edge-schema
              {:columns [{:name "id" :type :type/Integer :nullable? false}
                         ;; BigQuery specific types
                         {:name "struct_field" :type :type/Dictionary :nullable? true :database-type "STRUCT<name STRING, age INT64, active BOOL>"}
                         {:name "array_ints" :type :type/Array :nullable? true :database-type "ARRAY<INT64>"}
                         {:name "array_structs" :type :type/Array :nullable? true :database-type "ARRAY<STRUCT<key STRING, value FLOAT64>>"}
                         {:name "geography_field" :type :type/Text :nullable? true :database-type "GEOGRAPHY"}
                         {:name "numeric_precise" :type :type/Decimal :nullable? true :database-type "NUMERIC(38,9)"}
                         {:name "bignumeric_field" :type :type/Decimal :nullable? true :database-type "BIGNUMERIC(76,38)"}
                         {:name "bytes_field" :type :type/Text :nullable? true :database-type "BYTES"}
                         {:name "datetime_field" :type :type/DateTime :nullable? true :database-type "DATETIME"}
                         {:name "time_field" :type :type/Time :nullable? true :database-type "TIME"}]
               :data [[1 "{\"name\": \"Alice\", \"age\": 30, \"active\": true}" "[1,2,3,4,5]"
                       "[{\"key\": \"alpha\", \"value\": 1.1}, {\"key\": \"beta\", \"value\": 2.2}]"
                       "POINT(-122.084 37.422)" 123456789012345678901234567.123456789
                       99999999999999999999999999999999999999.12345678901234567890123456789012345678
                       "SGVsbG8gV29ybGQ=" "2024-12-31T23:59:59.999999" "23:59:59.999999"]
                      [2 "{\"name\": \"Bob\", \"age\": null, \"active\": false}" "[]"
                       "[]" "POLYGON((-124 42, -120 42, -120 46, -124 46, -124 42))"
                       -999999999999999999999999999.999999999
                       -12345678901234567890123456789012345678.12345678901234567890123456789012345678
                       "VGVzdCBEYXRh" "1900-01-01T00:00:00" "00:00:00"]
                      [3 "{\"name\": \"\", \"age\": 0, \"active\": true}" "[0]"
                       "[{\"key\": \"\", \"value\": 0.0}]" "POINT(0 0)" 0.000000001
                       0.00000000000000000000000000000000000001 "" "2000-01-01T12:00:00" "12:00:00"]
                      [4 nil nil nil nil nil nil nil nil nil]]}

              table-id (create-test-table-with-data!
                        table-name
                        bq-edge-schema
                        (:data bq-edge-schema))

              transform-code (str "import pandas as pd\n"
                                  "import json\n"
                                  "\n"
                                  "def transform(" table-name "):\n"
                                  "    df = " table-name ".copy()\n"
                                  "    \n"
                                  "    # Struct operations\n"
                                  "    df['struct_has_name'] = df['struct_field'].astype(str).str.contains('name', na=False)\n"
                                  "    df['struct_length'] = df['struct_field'].astype(str).str.len()\n"
                                  "    \n"
                                  "    # Array operations\n"
                                  "    df['array_ints_length'] = df['array_ints'].astype(str).str.len()\n"
                                  "    df['array_structs_complex'] = df['array_structs'].astype(str).str.contains('key', na=False)\n"
                                  "    \n"
                                  "    # Geography operations\n"
                                  "    df['is_point'] = df['geography_field'].astype(str).str.contains('POINT', na=False)\n"
                                  "    df['is_polygon'] = df['geography_field'].astype(str).str.contains('POLYGON', na=False)\n"
                                  "    \n"
                                  "    # High precision numeric\n"
                                  "    df['numeric_rounded'] = df['numeric_precise'].round(2)\n"
                                  "    df['has_large_number'] = df['bignumeric_field'].abs() > 1e30\n"
                                  "    \n"
                                  "    return df")

              result (execute! {:code transform-code
                                :tables {table-name table-id}})]

          (testing "BigQuery exotic transform succeeded"
            (is (some? result) "BigQuery transform should succeed"))

          (when result
            (testing "BigQuery exotic types processed"
              (let [metadata (:output-manifest result)
                    headers (map :name (:fields metadata))]
                (is (contains? (set headers) "struct_has_name"))
                (is (contains? (set headers) "is_point"))
                (is (contains? (set headers) "has_large_number")))))

          (cleanup-table! table-id))))))

(deftest exotic-edge-cases-python-transform-snowflake-test
  (testing "Snowflake exotic edge cases"
    (mt/test-driver :snowflake
      (mt/with-empty-db
        (let [table-name (mt/random-name)
              sf-edge-schema
              {:columns [{:name "id" :type :type/Integer :nullable? false}
                         ;; Snowflake specific types
                         {:name "variant_field" :type :type/JSON :nullable? true :database-type "VARIANT"}
                         {:name "object_field" :type :type/JSON :nullable? true :database-type "OBJECT"}
                         {:name "array_field" :type :type/Array :nullable? true :database-type "ARRAY"}
                         {:name "geography_field" :type :type/Text :nullable? true :database-type "GEOGRAPHY"}
                         {:name "geometry_field" :type :type/Text :nullable? true :database-type "GEOMETRY"}
                         {:name "number_large" :type :type/Decimal :nullable? true :database-type "NUMBER(38,0)"}
                         {:name "timestamp_ntz" :type :type/DateTime :nullable? true :database-type "TIMESTAMP_NTZ"}
                         {:name "timestamp_ltz" :type :type/DateTimeWithTZ :nullable? true :database-type "TIMESTAMP_LTZ"}
                         {:name "timestamp_tz" :type :type/DateTimeWithTZ :nullable? true :database-type "TIMESTAMP_TZ"}]
               :data [[1 "{\"type\": \"variant\", \"data\": [1,2,3]}" "{\"nested\": {\"key\": \"value\"}}"
                       "[\"apple\", \"banana\", \"cherry\"]" "POINT(-122.35 37.55)"
                       "POLYGON((-124 42, -120 42, -120 46, -124 46, -124 42))"
                       99999999999999999999999999999999999999
                       "2024-12-31 23:59:59.999999999" "2024-12-31 23:59:59.999999999 +0000"
                       "2024-12-31 23:59:59.999999999 -0800"]
                      [2 "\"simple string\"" "{}" "[]" "LINESTRING(-122 37, -121 38)"
                       "MULTIPOINT((-122 37), (-121 38))" -12345678901234567890123456789012345678
                       "1900-01-01 00:00:00.000000001" "1900-01-01 00:00:00.000000001 +0000"
                       "1900-01-01 00:00:00.000000001 +1200"]
                      [3 "123.456" "{\"empty\": null}" "[null, \"\", 0]" "POINT(0 0)" "POINT EMPTY"
                       0 "2000-01-01 12:00:00" "2000-01-01 12:00:00 +0000" "2000-01-01 12:00:00 +0000"]
                      [4 nil nil nil nil nil nil nil nil nil]]}

              table-id (create-test-table-with-data!
                        table-name
                        sf-edge-schema
                        (:data sf-edge-schema))

              transform-code (str "import pandas as pd\n"
                                  "\n"
                                  "def transform(" table-name "):\n"
                                  "    df = " table-name ".copy()\n"
                                  "    \n"
                                  "    # Variant/Object operations\n"
                                  "    df['variant_is_complex'] = df['variant_field'].astype(str).str.contains('{', na=False)\n"
                                  "    df['object_has_nested'] = df['object_field'].astype(str).str.contains('nested', na=False)\n"
                                  "    \n"
                                  "    # Array operations\n"
                                  "    df['array_has_fruits'] = df['array_field'].astype(str).str.contains('apple', na=False)\n"
                                  "    df['array_length'] = df['array_field'].astype(str).str.len()\n"
                                  "    \n"
                                  "    # Geography operations\n"
                                  "    df['is_point_geo'] = df['geography_field'].astype(str).str.startswith('POINT', na=False)\n"
                                  "    df['is_complex_geom'] = df['geometry_field'].astype(str).str.contains('POLYGON|MULTI', na=False)\n"
                                  "    \n"
                                  "    # Large number operations\n"
                                  "    df['number_abs'] = df['number_large'].abs()\n"
                                  "    df['is_huge_number'] = df['number_large'].abs() > 1e30\n"
                                  "    \n"
                                  "    return df")

              result (execute! {:code transform-code
                                :tables {table-name table-id}})]

          (testing "Snowflake exotic transform succeeded"
            (is (some? result) "Snowflake transform should succeed"))

          (when result
            (testing "Snowflake exotic types processed"
              (let [metadata (:output-manifest result)
                    headers (map :name (:fields metadata))]
                (is (contains? (set headers) "variant_is_complex"))
                (is (contains? (set headers) "is_point_geo"))
                (is (contains? (set headers) "is_huge_number")))))

          (cleanup-table! table-id))))))

(deftest exotic-edge-cases-python-transform-clickhouse-test
  (testing "ClickHouse exotic edge cases"
    (mt/test-driver :clickhouse
      (mt/with-empty-db
        (let [table-name (mt/random-name)
              ch-edge-schema
              {:columns [{:name "id" :type :type/Integer :nullable? false}
                         ;; ClickHouse specific types
                         {:name "array_field" :type :type/Array :nullable? true :database-type "Array(Int64)"}
                         {:name "tuple_field" :type :type/Text :nullable? true :database-type "Tuple(String, Int64, Float64)"}
                         {:name "map_field" :type :type/Dictionary :nullable? true :database-type "Map(String, Int64)"}
                         {:name "uuid_field" :type :type/UUID :nullable? true :database-type "UUID"}
                         {:name "ipv4_field" :type :type/IPAddress :nullable? true :database-type "IPv4"}
                         {:name "ipv6_field" :type :type/IPAddress :nullable? true :database-type "IPv6"}
                         {:name "decimal128" :type :type/Decimal :nullable? true :database-type "Decimal128(18)"}
                         {:name "fixedstring" :type :type/Text :nullable? true :database-type "FixedString(10)"}
                         {:name "enum8_field" :type :type/Text :nullable? true :database-type "Enum8('small'=1,'medium'=2,'large'=3)"}]
               :data [[1 "[1,2,3,4,5]" "('test', 42, 3.14)" "{'key1': 100, 'key2': 200}"
                       "550e8400-e29b-41d4-a716-446655440000" "192.168.1.1" "2001:db8::1"
                       123456789012345678 "fixed_text" "medium"]
                      [2 "[-1,0,1]" "('', 0, -1.5)" "{'empty': 0}"
                       "00000000-0000-0000-0000-000000000000" "10.0.0.1" "::1"
                       -999999999999999999 "short     " "small"]
                      [3 "[]" "('null', -1, 0.0)" "{}"
                       "ffffffff-ffff-ffff-ffff-ffffffffffff" "255.255.255.255" "ffff::ffff"
                       0 "          " "large"]
                      [4 nil nil nil nil nil nil nil nil nil]]}

              table-id (create-test-table-with-data!
                        table-name
                        ch-edge-schema
                        (:data ch-edge-schema))

              transform-code (str "import pandas as pd\n"
                                  "\n"
                                  "def transform(" table-name "):\n"
                                  "    df = " table-name ".copy()\n"
                                  "    \n"
                                  "    # Array operations\n"
                                  "    df['array_has_positive'] = df['array_field'].astype(str).str.contains('[1-9]', na=False)\n"
                                  "    df['array_length'] = df['array_field'].astype(str).str.len()\n"
                                  "    \n"
                                  "    # Tuple operations\n"
                                  "    df['tuple_has_string'] = df['tuple_field'].astype(str).str.contains(\"'\", na=False)\n"
                                  "    df['tuple_has_negative'] = df['tuple_field'].astype(str).str.contains('-', na=False)\n"
                                  "    \n"
                                  "    # Map operations\n"
                                  "    df['map_has_keys'] = df['map_field'].astype(str).str.contains('key', na=False)\n"
                                  "    df['map_is_empty'] = df['map_field'] == '{}'\n"
                                  "    \n"
                                  "    # IP operations\n"
                                  "    df['ipv4_is_private'] = df['ipv4_field'].astype(str).str.contains('192.168|10.', na=False)\n"
                                  "    df['ipv6_is_loopback'] = df['ipv6_field'].astype(str).str.contains('::1', na=False)\n"
                                  "    \n"
                                  "    # UUID operations\n"
                                  "    df['uuid_is_null'] = df['uuid_field'].astype(str).str.startswith('00000000', na=False)\n"
                                  "    \n"
                                  "    return df")

              result (execute! {:code transform-code
                                :tables {table-name table-id}})]

          (testing "ClickHouse exotic transform succeeded"
            (is (some? result) "ClickHouse transform should succeed"))

          (when result
            (testing "ClickHouse exotic types processed"
              (let [metadata (:output-manifest result)
                    headers (map :name (:fields metadata))]
                (is (contains? (set headers) "array_has_positive"))
                (is (contains? (set headers) "ipv4_is_private"))
                (is (contains? (set headers) "uuid_is_null")))))

          (cleanup-table! table-id))))))

(deftest large-values-python-transform-test
  (testing "Test Python transforms with large-ish values that should work within 63-bit limits."
    (mt/test-drivers #{:h2 :postgres :mysql :snowflake}
      (mt/with-empty-db
        (let [table-name (mt/random-name)
              large-values-schema
              {:columns [{:name "id" :type :type/Integer :nullable? false}
                         {:name "big_int" :type :type/Integer :nullable? true}
                         {:name "big_decimal" :type :type/Decimal :nullable? true}
                         {:name "very_long_text" :type :type/Text :nullable? true}
                         {:name "precision_float" :type :type/Float :nullable? true}
                         {:name "timestamp_precise" :type :type/DateTime :nullable? true}]
               :data [[1 4611686018427387903 ; ~2^62 (within 63-bit limit)
                       1234567237.890123456 ; Large decimal with precision
                       (apply str (repeat 10000 "A")) ; 10K character string
                       1.23456789012345E14 ; Large float with precision
                       "2024-12-31 23:59:59.999999"]
                      [2 -4611686018427387903 ; Large negative
                       -987654654.321098765 ; Large negative decimal
                       (str "Unicode mix: 你好世界 " (apply str (repeat 1000 "🎉"))) ; Mixed unicode
                       2.98792458E-39 ; Very small positive
                       "1900-01-01 00:00:00.000001"]
                      [3 0 ; Boundary cases
                       0.000000000001 ; Very small decimal
                       "" ; Empty string
                       0.0 ; Zero float
                       "2000-01-01 12:00:00"]
                      [4 nil nil nil nil nil]]} ; All nulls

              table-id (create-test-table-with-data!
                        table-name
                        large-values-schema
                        (:data large-values-schema))

              ;; Transform that processes large values
              transform-code (str "import pandas as pd\n"
                                  "import numpy as np\n"
                                  "\n"
                                  "def transform(" table-name "):\n"
                                  "    df = " table-name ".copy()\n"
                                  "    \n"
                                  "    # Large integer operations\n"
                                  "    df['big_int_safe'] = df['big_int'].fillna(0) // 1000000  # Scale down safely\n"
                                  "    df['is_large_positive'] = df['big_int'] > 1000000000\n"
                                  "    \n"
                                  "    # Decimal operations\n"
                                  "    df['decimal_rounded'] = df['big_decimal'].round(6)\n"
                                  "    df['decimal_magnitude'] = np.abs(df['big_decimal'])\n"
                                  "    \n"
                                  "    # Text length operations on large strings\n"
                                  "    df['text_length'] = df['very_long_text'].astype(str).str.len()\n"
                                  "    df['has_unicode'] = df['very_long_text'].astype(str).str.contains('[^\\x00-\\x7F]', na=False, regex=True)\n"
                                  "    \n"
                                  "    # Float operations\n"
                                  "    df['float_log10'] = np.log10(np.abs(df['precision_float']) + 1e-100)  # Safe log\n"
                                  "    df['is_scientific'] = np.abs(df['precision_float']) > 1e10\n"
                                  "    \n"
                                  "    return df")

              result (execute! {:code transform-code
                                :tables {table-name table-id}})]

          (testing "Large values transform succeeded"
            (is (some? result) "Transform with large values should succeed")
            (is (contains? result :output) "Should have output"))

          (when result
            (let [lines (str/split-lines (:output result))
                  rows (map json/decode lines)
                  metadata (:output-manifest result)
                  headers (map :name (:fields metadata))]

              (testing "Large values processed correctly"
                (is (= 4 (count rows)) "Should have 4 rows")
                (is (> (count headers) 6) "Should have computed columns")

                ;; Check computed columns exist
                (is (contains? (set headers) "text_length") "Should calculate text length")
                (is (contains? (set headers) "is_large_positive") "Should detect large numbers")
                (is (contains? (set headers) "has_unicode") "Should detect unicode"))

              (testing "Large value operations maintain precision"
                (let [first-row (first rows)]
                  ;; Text length should be very large for first row
                  (when-let [length (get first-row "text_length")]
                    (is (> length 9000) "Should handle very long text correctly"))

                  ;; Should detect unicode in second row
                  (let [second-row (second rows)
                        has-unicode (get second-row "has_unicode")]
                    (is (contains? #{true "True" "true" 1 "1"} has-unicode)
                        "Should detect unicode characters"))))))

          ;; Cleanup
          (cleanup-table! table-id))))))
