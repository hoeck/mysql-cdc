
;;; toplevel mysql cdc file, read parsed binlog events and make
;;; some sense out of them

(ns cdc.mysql
  (:use [clojure.contrib.pprint :only [pprint]]
        [clojure.contrib.except :only [throwf]])
  (:require [cdc.mysql-binlog :as binlog]
            [clojure.contrib.sql :as sql])
  (:import java.util.concurrent.TimeUnit))


;;; mysql jdbc

(def mysql {:classname "com.mysql.jdbc.Driver"
            :subprotocol "mysql"
            :subname "//localhost"
            :username "root"
            :user "root"
            :password "root"})

(defn mysql-query [sql]
  (sql/with-connection mysql
    (sql/with-query-results res
      [sql]
      (doall res))))

(defn mysql-do [& sql-commands]
  (sql/with-connection mysql
    (apply sql/do-commands sql-commands)))

;;;; structure:
;; use .take to block until the next available set/vec of events
;; use (.poll binlog/*event-queue* 200 TimeUnit/MILLISECONDS) to block with timeout
;; now the hard part: interpret those events 

(def *state* (binlog/cdc-init))

(defn go []
  (send-off *state* binlog/cdc-start "/var/log/mysql/binlog-files.index"))

(comment
  (def *state* )
  ;; start the cdc-mechanism
  (send-off *state* cdc-start "/var/log/mysql/binlog-files.index")
  ;; or
  (go)
  ;; stop it
  (send *state* cdc-stop)

  (mysql-do "create temporary table foo.t (a INT)")

  (mysql-do "drop table foo.test1")
  (mysql-do "create table foo.test1 ( id int, nr decimal(10,4) )")
  (defn rand-insert []
    (str "insert into foo.test1 (nr) values (" (rand-int 9999) ")"))
  (apply mysql-do (repeatedly 1000 rand-insert))
  
  (mysql-do
   "START TRANSACTION"
   "insert into foo.test1 (nr) values (88888)"
   ;;"rollback"
   )

  (mysql-do "drop table foo.t1")
  (mysql-do
   "CREATE TABLE foo.t1 (a INT) ENGINE = INNODB"
   "START TRANSACTION"
   "INSERT INTO foo.t1 VALUES (1)"
   "COMMIT")
  (mysql-do "alter table foo.t1 change column a b int")
  (mysql-do "alter table foo.t1 change column b a int")
  (mysql-do "insert into foo.t1 values (10)")
  
  (def _events (.poll binlog/*event-queue* 200 TimeUnit/MILLISECONDS))
  
  (-> *state* agent-error throw)

  (mysql-do "delete from foo.bar where a=8")

  (defn mysql-do-rollback
    [& commands]
    (sql/with-connection mysql
      (with-open [stmt (.createStatement (sql/connection))]
        (doseq [cmd commands]
          (.addBatch stmt cmd))
        (sql/transaction
         (let [ret (seq (.executeBatch stmt))]
           (sql/set-rollback-only)
           ret)))))

  ;; try a really *big* tx
  (defn big-rollback-tx []
    (->> (repeat 1000000 (str "insert into foo.bar values ("(rand-int 100000)", 8, '8')"))
         (apply mysql-do-rollback)
         (filter (partial = 1))
         count))
  

  
  
;;; note: mysql-admin:startup-parameters:advanced:binlog-cache-size (defaults to 32K)

  ;; mysql -uroot -proot
  ;; set autocommit = 0
  ;; select @@autocommit
  )
