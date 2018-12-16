;; This buffer is for Clojure experiments and evaluation.
;; Press C-j to evaluate the last expression.

(require '[honeysql.format :as h])
(require '[honeysql.helpers :as hp])
(require '[clojure.java.jdbc :as j])

(def h2-db {:dbtype "h2:mem"
            :dbname "clojure_test_h2"})

(j/execute! h2-db (j/drop-table-ddl :string))
(j/execute! h2-db (j/drop-table-ddl :entities))
(j/execute! h2-db (j/drop-table-ddl :transactions))

(j/execute!
 h2-db
 (j/create-table-ddl
  :string
  [[:id :int "PRIMARY KEY AUTO_INCREMENT"]
   [:value :varchar "unique not null"]]))

(j/execute!
 h2-db
 (j/create-table-ddl
  :entities
  [[:id :int "PRIMARY KEY AUTO_INCREMENT"]
   [:attribute_ns :varchar "not null"]
   [:attribute_name :varchar "not null"]
   [:value_type :varchar "not null"]
   [:value_id :int]
   [:t :int "not null AUTO_INCREMENT"]
   [:transaction_id :int "not null"]]))

(j/execute!
 h2-db
 (j/create-table-ddl
  :transactions
  [[:id :int "PRIMARY KEY AUTO_INCREMENT"]]))

(defn v->table [v]
  (cond (string? v) "string"))

(defn transact [db vals]
  (let [transaction-id (:id (first (j/insert! h2-db :transactions {})))]
    (doseq [[k v] vals]
      (let [v-type (v->table v)
            v-id (if-let [v-id (-> (hp/select :*)
                                   (hp/from (keyword v-type))
                                   (hp/where [:= :value v])
                                   h/format
                                   (->> (j/query db))
                                   first
                                   :id)]
                   v-id
                   (-> (j/insert! db (keyword v-type)
                                  {:value v})
                       first
                       :id))]
        (j/insert!
         h2-db
         :entities
         {:attribute_ns (namespace k)
          :attribute_name (name k)
          :value_type "string"
          :value_id v-id
          :transaction_id transaction-id})))))

(defn pull-1 [db id]
  (let [{:keys [id attribute_ns attribute_name
                value_type value_id t]}
        (-> (hp/select :*)
            (hp/from :entities)
            (hp/where [:= :id id])
            h/format
            (->> (j/query db))
            first)]
    {(keyword attribute_ns attribute_name)
     (-> (hp/select :value)
         (hp/from (keyword (v->table value_type)))
         (hp/where [:= :id value_id])
         h/format
         (->> (j/query db))
         first
         :value)}))

(comment
  (transact h2-db {:foo/bar "foo"})
  (pull-1 h2-db 1)

  (j/query h2-db "select * from entities")
  (j/query h2-db "select * from string")
  (let [{:keys [entity value]}
        (:entity (first (j/query h2-db "select * from string where value = 'foo'")))]
    (j/query "select * from")))
