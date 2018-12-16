;; This buffer is for Clojure experiments and evaluation.
;; Press C-j to evaluate the last expression.


(require '[clojure.java.jdbc :as j])

(def h2-db {:dbtype "h2:mem"
            :dbname "clojure_test_h2"})

(j/execute! h2-db (j/drop-table-ddl :strings))
(j/execute! h2-db (j/drop-table-ddl :entities))

(j/execute!
 h2-db
 (j/create-table-ddl
  :strings
  [[:entity :int "PRIMARY KEY AUTO_INCREMENT"]
   [:value :varchar "not null"]]))

(j/execute!
 h2-db
 (j/create-table-ddl
  :entities
  [[:id :int "PRIMARY KEY AUTO_INCREMENT"]
   [:attribute_ns :varchar "not null"]
   [:attribute_name :varchar "not null"]
   [:type :varchar "not null"]
   [:entity :int]
   [:t :int "not null AUTO_INCREMENT"]]))


(j/insert!
 h2-db
 :entities
 {:attribute_ns "foo"
  :attribute_name "bar"
  :type "strings"
  :entity 1})


(defn transact [db vals]
  )

(transact h2-db {:foo/bar "foo"})
(j/query h2-db "select * from entities")
(j/query h2-db "select * from strings")



(j/execute! h2-db "create table string ")
(j/insert! h2-db :foo {})
(j/query h2-db "select * from foo")
