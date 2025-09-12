(ns ^:mb/driver-tests metabase-enterprise.transforms.python-driver-type-test
  "Comprehensive tests for Python transforms across all supported drivers with all base and exotic types."
  (:require
   [clojure.core.async :as a]
   [clojure.data.csv :as csv]
   [clojure.string :as str]
   [clojure.test :refer :all]
   [java-time.api :as t]
   [metabase-enterprise.transforms.python-runner :as python-runner]
   [metabase-enterprise.transforms.settings :as transforms.settings]
   [metabase-enterprise.transforms.util :as transforms.util]
   [metabase.driver :as driver]
   [metabase.sync.core :as sync]
   [metabase.test :as mt]
   [metabase.test.data.interface :as tx]
   [metabase.test.data.sql :as sql.tx]
   [metabase.util :as u]))

(def test-id (str (random-uuid)))

(defn- execute
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
             {:name "created_at" :type :type/DateTime :nullable? true}
             {:name "created_tz" :type :type/DateTimeWithTZ :nullable? true}]
   :data [[1 "Product A" 19.99 true "2024-01-01" "2024-01-01T12:00:00" "2024-01-01T12:00:00Z"]
          [2 "Product B" 15.50 false "2024-02-01" "2024-02-01T09:15:30" "2024-02-01T09:15:30-05:00"]
          [3 nil nil nil nil nil nil]]})

(def ^:private driver-exotic-types
  "Driver-specific exotic types with test data."
  {:postgres {:columns [{:name "id" :type :type/Integer :nullable? false}
                        {:name "uuid_field" :type :type/UUID :nullable? true}
                        {:name "json_field" :type :type/JSON :nullable? true}
                        {:name "ip_field" :type :type/IPAddress :nullable? true}]
              :data [[1 "550e8400-e29b-41d4-a716-446655440000" "{\"key\": \"value\"}" "192.168.1.1"]
                     [2 nil nil nil]]}
   :mysql {:columns [{:name "id" :type :type/Integer :nullable? false}
                     {:name "json_field" :type :type/JSON :nullable? true}]
           :data [[1 "{\"key\": \"value\"}"]
                  [2 nil]]}
   :bigquery-cloud-sdk {:columns [{:name "id" :type :type/Integer :nullable? false}
                                  {:name "json_field" :type :type/JSON :nullable? true}
                                  {:name "array_field" :type :type/Array :nullable? true}]
                        :data [[1 "{\"key\": \"value\"}" "[1, 2, 3]"]
                               [2 nil nil]]}
   :snowflake {:columns [{:name "id" :type :type/Integer :nullable? false}
                         {:name "array_field" :type :type/Array :nullable? true}]
               :data [[1 "[1, 2, 3]"]
                      [2 nil]]}
   :sqlserver {:columns [{:name "id" :type :type/Integer :nullable? false}
                         {:name "uuid_field" :type :type/UUID :nullable? true}]
               :data [[1 "550e8400-e29b-41d4-a716-446655440000"]
                      [2 nil]]}
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
                     {:name "array_field" :type :type/Array :nullable? true}
                     {:name "bson_id" :type :type/MongoBSONID :nullable? true}]
           :data [[1 "550e8400-e29b-41d4-a716-446655440000" "{\"key\": \"value\"}" "[1, 2, 3]" "507f1f77bcf86cd799439011"]
                  [2 nil nil nil nil]]}})

(defn- create-test-table-with-data
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

    qualified-table-name))

(defn- cleanup-table
  "Drop the test table."
  [qualified-table-name]
  (try
    (driver/drop-table! driver/*driver* (mt/id) qualified-table-name)
    (catch Exception _e
      ;; Ignore cleanup errors
      nil)))

(defn- validate-transform-output
  "Validate that the Python transform output preserves types and data correctly."
  [result expected-columns expected-row-count]
  (testing "Transform execution succeeded"
    (is (some? result))
    (is (contains? result :output))
    (is (contains? result :output-manifest)))

  (when result
    (let [csv-data (csv/read-csv (:output result))
          headers (first csv-data)
          rows (rest csv-data)
          metadata (:output-manifest result)]

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

(deftest base-types-python-transform-test
  "Test Python transforms with base types across all supported drivers."
  (mt/test-drivers #{:postgres :mysql :bigquery-cloud-sdk :snowflake :sqlserver :redshift :clickhouse}
    (mt/with-empty-db
      (let [table-name "base_types_test"
            qualified-table-name (create-test-table-with-data
                                  table-name
                                  base-type-test-data
                                  (:data base-type-test-data))

            ;; Simple identity transform that should preserve all types
            transform-code (str "import pandas as pd\n"
                                "\n"
                                "def transform(" table-name "):\n"
                                "    df = " table-name ".copy()\n"
                                "    return df")

            result (execute {:code transform-code
                             :tables {table-name (mt/id qualified-table-name)}})

            expected-columns ["id" "name" "price" "active" "created_date" "created_at" "created_tz"]

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
                (is (= :type/DateTime (dtype-map "created_at")))
                (is (= :type/DateTimeWithTZ (dtype-map "created_tz")))))))

        ;; Cleanup
        (cleanup-table qualified-table-name)))))

(deftest exotic-types-python-transform-test
  "Test Python transforms with driver-specific exotic types."
  (mt/test-drivers #{:postgres :mysql :bigquery-cloud-sdk :snowflake :sqlserver :redshift :clickhouse :mongo}
    (mt/with-empty-db
      (when-let [exotic-config (get driver-exotic-types driver/*driver*)]
        (let [table-name "exotic_types_test"
              qualified-table-name (create-test-table-with-data
                                    table-name
                                    exotic-config
                                    (:data exotic-config))

              ;; Simple identity transform
              transform-code (str "import pandas as pd\n"
                                  "\n"
                                  "def transform(" table-name "):\n"
                                  "    df = " table-name ".copy()\n"
                                  "    return df")

              result (execute {:code transform-code
                               :tables {table-name (mt/id qualified-table-name)}})

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
                                (is (contains? #{:type/Text :type/IPAddress} (dtype-map "ip_field")))))

                  :mysql (when (contains? dtype-map "json_field")
                           (is (contains? #{:type/Text :type/JSON} (dtype-map "json_field"))))

                  :bigquery-cloud-sdk (do
                                        (when (contains? dtype-map "json_field")
                                          (is (contains? #{:type/Text :type/JSON} (dtype-map "json_field"))))
                                        (when (contains? dtype-map "array_field")
                                          (is (contains? #{:type/Text :type/Array} (dtype-map "array_field")))))

                  :snowflake (when (contains? dtype-map "array_field")
                               (is (contains? #{:type/Text :type/Array} (dtype-map "array_field"))))

                  :sqlserver (when (contains? dtype-map "uuid_field")
                               (is (contains? #{:type/Text :type/UUID} (dtype-map "uuid_field"))))

                  (:redshift :clickhouse) (when (contains? dtype-map "description")
                                            (is (= :type/Text (dtype-map "description"))))

                  :mongo (do
                           (when (contains? dtype-map "uuid_field")
                             (is (contains? #{:type/Text :type/UUID} (dtype-map "uuid_field"))))
                           (when (contains? dtype-map "json_field")
                             (is (contains? #{:type/Text :type/JSON} (dtype-map "json_field"))))
                           (when (contains? dtype-map "array_field")
                             (is (contains? #{:type/Text :type/Array} (dtype-map "array_field"))))
                           (when (contains? dtype-map "bson_id")
                             (is (contains? #{:type/Text :type/MongoBSONID} (dtype-map "bson_id")))))))))

          ;; Cleanup
          (cleanup-table qualified-table-name))))))

(deftest edge-cases-python-transform-test
  "Test Python transforms with edge cases: null values, empty strings, extreme values."
  (mt/test-drivers #{:postgres :mysql :bigquery-cloud-sdk :snowflake :sqlserver :redshift :clickhouse}
    (mt/with-empty-db
      (let [table-name "edge_cases_test"
            edge-case-schema {:columns [{:name "id" :type :type/Integer :nullable? false}
                                        {:name "text_field" :type :type/Text :nullable? true}
                                        {:name "int_field" :type :type/Integer :nullable? true}
                                        {:name "float_field" :type :type/Float :nullable? true}
                                        {:name "bool_field" :type :type/Boolean :nullable? true}
                                        {:name "date_field" :type :type/Date :nullable? true}]
                              :data [[1 "" 0 0.0 false "2024-01-01"] ; Minimal values
                                     [2 "Very long text with special chars: !@#$%^&*(){}[]|\\:;\"'<>,.?/~`"
                                      2147483647 1.7976931348623157E308 true "9999-12-31"] ; Maximum values
                                     [3 nil nil nil nil nil]]} ; All nulls

            qualified-table-name (create-test-table-with-data
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
                                "    df['text_length'] = df['text_field'].astype(str).str.len()\n"
                                "    \n"
                                "    # Handle numeric operations with null safety\n"
                                "    df['int_doubled'] = df['int_field'] * 2\n"
                                "    df['float_squared'] = df['float_field'] ** 2\n"
                                "    \n"
                                "    # Boolean operations\n"
                                "    df['bool_inverted'] = ~df['bool_field'].fillna(False)\n"
                                "    \n"
                                "    return df")

            result (execute {:code transform-code
                             :tables {table-name (mt/id qualified-table-name)}})

            expected-columns ["id" "text_field" "int_field" "float_field" "bool_field" "date_field"
                              "text_length" "int_doubled" "float_squared" "bool_inverted"]

            validation (validate-transform-output result expected-columns 3)]

        (when validation
          (let [{:keys [rows headers metadata]} validation
                get-col (fn [row col-name]
                          (nth row (.indexOf headers col-name)))
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
                (is (= "1" (get-col row1 "id")))
                (is (= "0" (get-col row1 "text_length"))) ; empty string length
                (is (= "0" (get-col row1 "int_doubled"))) ; 0 * 2

                ;; Row 2: maximum values  
                (is (= "2" (get-col row2 "id")))
                (is (not= "" (get-col row2 "text_length"))) ; long string has length

                ;; Row 3: null values
                (is (= "3" (get-col row3 "id")))))))

        ;; Cleanup
        (cleanup-table qualified-table-name)))))

(deftest idempotent-transform-test
  "Test that running the same transform multiple times produces identical results."
  (mt/test-drivers #{:postgres :mysql :bigquery-cloud-sdk :snowflake :sqlserver :redshift :clickhouse}
    (mt/with-empty-db
      (let [table-name "idempotent_test"
            qualified-table-name (create-test-table-with-data
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
            result1 (execute-python-transform
                     transform-code
                     {table-name (mt/id qualified-table-name)})

            result2 (execute-python-transform
                     transform-code
                     {table-name (mt/id qualified-table-name)})]

        (testing "Both transforms succeeded"
          (is (some? result1))
          (is (some? result2)))

        (when (and result1 result2)
          (testing "Results are identical"
            (is (= (:output result1) (:output result2)))
            (is (= (count (:fields (:output-manifest result1)))
                   (count (:fields (:output-manifest result2))))))

          (let [expected-columns ["id" "name" "price" "active" "created_date" "created_at" "created_tz"
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
        (cleanup-table qualified-table-name)))))