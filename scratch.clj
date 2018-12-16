;; This buffer is for Clojure experiments and evaluation.
;; Press C-j to evaluate the last expression.

(require '[honeysql.format :as h])
(require '[honeysql.helpers :as hp])
(require '[clojure.java.jdbc :as j])

(def h2-db {:dbtype "h2:mem"
            :dbname "clojure_test_h2"})

(j/execute! h2-db (j/drop-table-ddl :string))
(j/execute! h2-db (j/drop-table-ddl :int))
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
  :int
  [[:id :int "PRIMARY KEY AUTO_INCREMENT"]
   [:value :int "unique not null"]]))

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
  (cond (string? v) "string"
        (integer? v) "int"))

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
          :value_type (v->table v)
          :value_id v-id
          :transaction_id transaction-id})))))

(defn find-entity-1 [db input-id]
  (-> (hp/select :*)
      (hp/from :entities)
      (hp/where [:= :id input-id])
      h/format
      (->> (j/query db))
      first))

(defn pull-1 [db input-id]
  (let [{:keys [transaction_id]} (find-entity-1 db input-id)
        siblings (-> (hp/select :*)
                     (hp/from :entities)
                     (hp/where [:= :transaction_id transaction_id])
                     h/format
                     (->> (j/query db)))]
    (->> siblings
         (map (fn [{:keys [id attribute_ns attribute_name
                           value_type value_id t transaction_id]}]
                {(keyword attribute_ns attribute_name)
                 (-> (hp/select :value)
                     (hp/from (keyword value_type))
                     (hp/where [:= :id value_id])
                     h/format
                     (->> (j/query db))
                     first
                     :value)}))
         (into {}))))

(defn find [db attr v]
  (let [{:keys [id value]}
        (-> (hp/select :*)
            (hp/from (keyword (v->table v)))
            (hp/where [:= :value v])
            h/format
            (->> (j/query db))
            first)]
    (-> (hp/select :*)
        (hp/from :entities)
        (hp/where [:= :value_id id])
        h/format
        (->> (j/query db)))))

(comment
  (transact h2-db {:foo/bar "foo"
                   :foo/int 3})
  (pull-1 h2-db 1)
  (find h2-db :foo/bar "foo")
  (j/query h2-db "select * from entities")
  (j/query h2-db "select * from string")
  (j/query h2-db "select * from int"))
