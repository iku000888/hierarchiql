;; This buffer is for Clojure experiments and evaluation.
;; Press C-j to evaluate the last expression.

(require '[honeysql.format :as h])
(require '[honeysql.helpers :as hp])
(require '[clojure.java.jdbc :as j])
(require '[clojure.walk :as w])

(def h2-db {:dbtype "h2:mem"
            :dbname "clojure_test_h2"})

(do (j/execute! h2-db (j/drop-table-ddl :string))
    (j/execute! h2-db (j/drop-table-ddl :int))
    (j/execute! h2-db (j/drop-table-ddl :entities))
    (j/execute! h2-db (j/drop-table-ddl :transactions))
    (j/execute! h2-db (j/drop-table-ddl :eids)))

(do
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
     [:eid :int "not null"]
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

  (j/execute!
   h2-db
   (j/create-table-ddl
    :eids
    [[:id :int "PRIMARY KEY AUTO_INCREMENT"]])))

(defn v->table [v]
  (cond (string? v) "string"
        (integer? v) "int"))

(defn transact-1 [db vals {:keys [tx-id components]}]
  (let [transaction-id tx-id
        eid (:id (first (j/insert! db :eids {})))]
    (doall
     (map (fn [[k v]]
            (let [v-type (if ((or components #{}) k)
                           "entities"
                           (v->table v))
                  v-id (when-not (= v-type "entities")
                         (if-let [v-id (-> (hp/select :*)
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
                               :id)))]
              (merge (first (j/insert!
                             h2-db
                             :entities
                             {:attribute_ns (namespace k)
                              :eid eid
                              :attribute_name (name k)
                              :value_type (if (= v-type "entities")
                                            "entities"
                                            (v->table v))
                              :value_id (if (= v-type "entities")
                                          v
                                          v-id)
                              :transaction_id transaction-id}))
                     {:eid eid
                      :transaction_id transaction-id})))
          vals))))

(defn map->component-keys [m db-id-store]
  (->> m
       (filter (comp #(get db-id-store %) second))
       (map first)
       set))

(defn transact [db vals]
  (let [db-id-store (atom {})
        transaction-id (:id (first (j/insert! h2-db :transactions {})))]
    (->> vals
         (map (fn [v]
                (let [tmp-db-id (:db/id v)
                      ckeys (map->component-keys v @db-id-store)
                      inserted (:eid (first (transact-1 db
                                                        (->> (dissoc v :db/id)
                                                             (map (fn [[k v]]
                                                                    {k (get @db-id-store v v)}))
                                                             (into {}))
                                                        {:tx-id transaction-id
                                                         :components ckeys})))]
                  (when tmp-db-id
                    (swap! db-id-store assoc tmp-db-id inserted)))))
         doall)))

(defn find-entity-1 [db input-id]
  (-> (hp/select :*)
      (hp/from :entities)
      (hp/where [:= :id input-id])
      h/format
      (->> (j/query db))
      first))

(defn pull-recursive [db m])

(defn pull-1 [db input-id {:keys [exclude-ids]}]
  (let [{:keys [eid]} (find-entity-1 db input-id)
        siblings (-> (hp/select :*)
                     (hp/from :entities)
                     (hp/where [:= :eid eid])
                     h/format
                     (->> (j/query db)))]
    siblings
    #_(->> siblings
           (keep (fn [[id {:keys [id attribute_ns attribute_name
                                  value_type value_id t transaction_id]}]]
                   (cond
                     (component-ids id) nil
                     (= value_type "entities")
                     {(keyword attribute_ns attribute_name)
                      (let [{:keys [id attribute_ns attribute_name
                                    value_type value_id t transaction_id]}
                            (get siblings value_id)]
                        {(keyword attribute_ns attribute_name)
                         (-> (hp/select :value)
                             (hp/from (keyword value_type))
                             (hp/where [:= :id value_id])
                             h/format
                             (->> (j/query db))
                             first
                             :value)})}
                     :default {(keyword attribute_ns attribute_name)
                               (-> (hp/select :value)
                                   (hp/from (keyword value_type))
                                   (hp/where [:= :id value_id])
                                   h/format
                                   (->> (j/query db))
                                   first
                                   :value)})))
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
        (hp/where [:= :value_id id]
                  [:= :attribute_ns (namespace attr)]
                  [:= :attribute_name (name attr)])
        h/format
        (->> (j/query db)))))

(defn expand-results [db res]
  (w/prewalk
   (fn [v]
     (when (list? v) (prn v))
     (cond
       (nil? (:value_type v)) v
       (not= (:value_type v) "entities") (let [r {(keyword (:attribute_ns v) (:attribute_name v))
                                                  (-> (hp/select :value)
                                                      (hp/from (keyword (:value_type v)))
                                                      (hp/where [:= :id (:value_id v)])
                                                      h/format
                                                      (->> (j/query db))
                                                      first
                                                      :value)}]
                                           (prn "Resulttttt" r)
                                           r)
       (= (:value_type v) "entities") {(keyword (:attribute_ns v) (:attribute_name v))
                                       (let [eid (-> (hp/select :eid)
                                                     (hp/from :entities)
                                                     (hp/where [:= :id (:value_id v)])
                                                     h/format
                                                     (->> (j/query db))
                                                     first
                                                     :eid)]
                                         {:db/components
                                          (-> (hp/select :*)
                                              (hp/from :entities)
                                              (hp/where [:= :eid eid])
                                              h/format
                                              (->> (j/query db)))})}))
   res))

(defn merge-components [v]
  (w/prewalk
   (fn [v]
     (if-let [comps (:db/components v)]
       (apply merge comps)
       v))
   v))

(defn pull-* [db id]
  (->>  (pull-1 db id {})
        (map (partial expand-results db))
        doall
        (into {})
        merge-components))

(comment
  (transact-1 h2-db {:bar/baz "boo"} nil)
  (transact-1 h2-db {:foo/bar "foo"
                     :foo/int 3
                     :foo/component 1} nil)
  (transact h2-db
            [{:db/id "voodoo"
              :voo/doo 493}
             {:db/id "barbaz"
              :bar/baz "boo"
              :bar/voodoo "voodoo"}
             {:foo/bar "foo"
              :foo/int 3
              :foo/component "barbaz"}])
  (pull-* h2-db 4)

  (find h2-db :voo/doo 493)
  (j/query h2-db "select * from entities")
  (j/query h2-db "select * from string")
  (j/query h2-db "select * from int"))
