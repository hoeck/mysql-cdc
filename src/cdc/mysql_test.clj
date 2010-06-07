
(ns cdc.mysql-test
  (:use cdc.mysql
        clojure.test
        clojure.contrib.sql
        clojure.contrib.pprint
        [clojure.contrib.sql.internal :only [*db*]])
  (require [cdc.mysql-binlog :as binlog])
  (:import java.util.concurrent.TimeUnit))


;;; mysql jdbc

(def mysql {:classname "com.mysql.jdbc.Driver"
            :subprotocol "mysql"
            :subname "//localhost"
            :username "root"
            :user "root"
            :password "root"})

(defmacro with-mysql [& body]
  `(with-connection ~'mysql
     ~@body))

(defn mysql-query [sql]
  (with-connection mysql
    (with-query-results res
      [sql]
      (doall res))))

(defn mysql-do [& sql-commands]
  (with-connection mysql
    (apply do-commands sql-commands)))


;;; event poppers

(defn cdc-pop-event []
  (.poll binlog/*event-queue* TimeUnit/MILLISECONDS 1))

(defn space [n]
  (apply str (take n (repeat " "))))

(defn str-align
  "Calls str on its second argument and aligns string to length n,
  padding spaces to the left (n<0) or right (0<n)
  ex: (str-aling 5 'abc)  -> \"  abc\"
      (str-aling -5 'abc) -> \"abc  \""
  [n s]
  (let [s (str s)]
    (if (< n 0)
      (str s (space (- (- n) (count s))))
      (str (space (- n (count s))) s))))

(defn pprint-str-log-entry
  "Print the most useful information for a given log-entry, usually
  the type and additional information."
  ([e]
     (str (str-align -20 (:type e))
          " "
          (condp = (:type e)
            'XID_EVENT (str (:xid e))
            'QUERY_EVENT (str "\"" (:sql e) "\"")
            'TABLE_MAP_EVENT (str (:db-name e)"."(:table-name e)": "(:table-id e))
            'WRITE_ROWS_EVENT  (str (:table-name e) " ("(:table-id e)") " (:rows e))
            'DELETE_ROWS_EVENT (str (:table-name e) " ("(:table-id e)") " (:rows e))
            'UPDATE_ROWS_EVENT (str (:table-name e) " ("(:table-id e)") " (:rows e))
            "")))
  ([e & more]
     (->> (cons e more)
          (map pprint-str-log-entry)
          (interpose \newline)
          (apply str))))
 
(defn find-thread [like-name]
  (->> (Thread/getAllStackTraces)
       keys 
       (filter #(re-matches (re-pattern (str ".*" like-name ".*"))
                            (.toLowerCase (.getName %))))
       first))

(defn queue-printer []
  (let [out *out* ]
    (-> (fn []
          (binding [*out* out]
            (->> (repeatedly #(.take binlog/*event-queue*))
                 (map #(apply pprint-str-log-entry %))
                 (map println)
                 dorun)))
        (Thread. "queue-printer thread")
        .start)))

;; (queue-printer)
;; (.interrupt (find-thread "queue-printer"))
;; (send *state* cdc-start "/var/log/mysql/binlog-files.index")

(defn test-1 []

  (mysql-do "create table foo.test1 ( id int, nr decimal(10,4) )")
  (mysql-do "drop table foo.test1")
  
  (mysql-do "insert into foo.test1 (nr) values (0383)")
  (mysql-do "insert into foo.test1 (id, nr) values (100, 1.208)"
            "insert into foo.foo (name,id) values ('aaa', 100)")
  (mysql-do "update foo.test1 set id = 100 where id = 10")
  (mysql-query "select * from foo.test1")
  
  

  ;; example test:
  (mysql-do "insert into foo.test1 (id, nr) values (100, 1.208)"
            "insert into foo.foo (name,id) values ('aaa', 100)")

  (defn count-and-insert [delaytime-in-ms]
    (with-connection mysql
      (transaction
       (let [c (-> (mysql-query "select count(*) as c from foo.bar") first :c)]
         (println (format "%s found %s rows" delaytime-in-ms c))
         (Thread/sleep delaytime-in-ms)
         (mysql-do (format "insert into foo.bar (id, name ,status) values (-1,'a counted: %s',0)" c))
         (println (format "%s wrote that" delaytime-in-ms c))))))
  ;;(count-and-insert 0)
  (do
    ;; long running tx
    (.start (Thread. #(count-and-insert 10000)))
    (Thread/sleep 2000)
    ;; short running tx
    (.start (Thread. #(count-and-insert 1000))))
  
  (mysql-do "update foo.bar set status = 0 where name = 'a'")
  (mysql-do "update foo.bar set name = 'a' where status = 0")

  (mysql-query "show master status")
({:file "binlog.000096", :position 106, :binlog_do_db "", :binlog_ignore_db ""})


)

(let [start (int \a)
      end (- (int \z) (int \a))]
  (defn rand-str [n]
    (apply str (repeatedly n #(char (+ start (rand-int end)))))))

(defn fill-table [n]
  (apply mysql-do
         (repeatedly n #(format "insert into foo.auto (value) values ('%s')"
                                (rand-str 100)))))


;;(fill-table 10000)

(with-mysql
  (.getMetaData (:connection *db*)))

