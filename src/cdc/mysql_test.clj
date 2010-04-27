
(ns cdc.mysql-test
  (:use cdc.mysql
        clojure.test
        clojure.contrib.sql
        clojure.contrib.pprint)
  (:import java.util.concurrent.TimeUnit))


;;; mysql jdbc

(def mysql {:classname "com.mysql.jdbc.Driver"
            :subprotocol "mysql"
            :subname "//localhost"
            :username "root"
            :user "root"
            :password "root"})

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
  (.poll *event-queue* TimeUnit/MILLISECONDS 1))

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
       first)) :offset 110,

(defn queue-printer []
  (let [out *out* ]
    (-> (fn []
          (binding [*out* out]
            (->> (repeatedly #(.take *event-queue*))
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
  (mysql-do "insert into foo.test1 (nr) values (0383)")
  (mysql-do "insert into foo.test1 (id, nr) values (100, 1.208)"
            "insert into foo.foo (name,id) values ('aaa', 100)")
  (mysql-do "update foo.test1 set id = 10 where id = 100")
  (mysql-query "select * from foo.test1")
  ;; irgendwo in der decimal klasse ist noch ein byteoverflow drin
  
  )


