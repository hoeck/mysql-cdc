;; The MIT License
;;
;; Copyright (c) 2010 Erik Soehnel
;;
;; Permission is hereby granted, free of charge, to any person obtaining a copy
;; of this software and associated documentation files (the "Software"), to deal
;; in the Software without restriction, including without limitation the rights
;; to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
;; copies of the Software, and to permit persons to whom the Software is
;; furnished to do so, subject to the following conditions:
;;
;; The above copyright notice and this permission notice shall be included in
;; all copies or substantial portions of the Software.
;;
;; THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
;; IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
;; FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
;; AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
;; LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
;; OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
;; THE SOFTWARE.

;; functions for benchmarking specific mysql and cdc funcitonality

(ns cdc.mysql-bench
  (:use [clojure.contrib.pprint :only [pprint]]
        [clojure.contrib.except :only [throwf]]
        criterium.core)
  (:require [cdc.mysql-binlog :as binlog]
            [cdc.jdbc :as jdbc]
            [clojure.contrib.sql :as sql]
            [clojure.contrib.sql.internal :as sql-i]
            [clojure.contrib.io :as io])
  (:import (java.util.concurrent TimeUnit
                                 BlockingQueue
                                 LinkedBlockingQueue)
           cdc.mysql.Decimal))


;;; simple mysql jdbc functions

(def mysql {:classname "com.mysql.jdbc.Driver"
            :subprotocol "mysql"
            :subname "//localhost"
            :username "root"
            :user "root"
            :password "root"})

(defn mysql-query
  "Execute a query on mysql and return a resultset-seq."
  [sql]
  (sql/with-connection mysql
    (sql/with-query-results res
      [sql]
      (doall res))))

(defn mysql-do
  "Execute a statement on mysql."
  [& sql-commands]
  (sql/with-connection mysql
    (apply sql/do-commands sql-commands)))


;; creating test data

(let [start (int \a)
      end (- (int \z) (int \a))]
  (defn rand-str
    "Return a string of n randomly chosen lower letter characters."
    [n]
    (apply str (repeatedly n #(char (+ start (rand-int end)))))))

(defn rand-decimal
  "Return the string representation of a randomly chosen decimal number
  with a digits and b decimal places."
  [a b]
  (str (long (rand (Math/pow 10 (- a b))))
       "."
       (long (rand (Math/pow 10 b)))))

(defmacro try-ignore [& body]
  `(try ~@body (catch Exception e# nil)))

(defn create-big-table
  "Create a benchmark table and or database named bench.big.
  big will have the 3 columntypes recogniced by read-binlog:
  int, decimal and string."
  []
  (try-ignore (mysql-do "create database bench"))
  (try-ignore (mysql-do "drop table bench.big"))
  (mysql-do "create table bench.big (
id int auto_increment primary key,
val decimal(12,4),
word varchar(50))")
  (mysql-do (format "insert into bench.big (val, word) values (%s, '%s')"
                    (rand-decimal 12 4)
                    (rand-str 50))))

(defn big-table-count []
  (-> (mysql-query "select count(*) from bench.big")
      first first val))

(defn required-big-table-grows [megabytes-at-least]
  (Math/ceil (/ (Math/log (/ (* megabytes-at-least
                                1000000)
                             60))
                (Math/log 2))))

(defn grow-big-table
  "Insert megabytes-at-least rows into bench.big."
  [megabytes-at-least]
  (dotimes [_ (required-big-table-grows megabytes-at-least)]
    (mysql-do "insert into bench.big (val, word) select val, word from bench.big"))
  (big-table-count))

(defn big-table-data-size []
  {:data-size
   (* (+ (Decimal/decimalBinSize 12 4) 50 4)
      (big-table-count))
   :table-size (let [ts (first (mysql-query "show table status from bench where name = 'big'"))
                     size (- (:data_length ts) (:data_free ts))]
                 (if (< 0 size)
                   size
                   (:data_length ts)))})

(defn big-table-update
  "Set val and word cols in bench.table to a new value.
  Will generate binlog data roughly twice as big as the current table size."
  []
  (mysql-do (format "update bench.big set val=%s, word='%s'"
                    (rand-decimal 12 4)
                    (rand-str 50))))

(defn get-penultimate-binlog []
  ;;(mysql-query "show master status")
  (-> (mysql-query "show binary logs") reverse second :log_name))

(defn check-read-data
  "Given a seq of rows, test wether they are equal except
  for the first element."
  [s]
  (->> (filter #(= (:type %) 'UPDATE_ROWS_EVENT) s)
       (mapcat :rows)
       (map second)
       (map next)
       (map (fn [[v w]] [(str v) w]))
       (reduce #(and (= % %2) %))
       boolean))

;;; benchmark mysql and read-binlog performance

(defn binlog-io-setup []
  (create-big-table)
  (grow-big-table 10)
  (big-table-data-size))

(defn read-penultimate-binlog
  []
  (->> (get-penultimate-binlog)
       (str "/var/log/mysql/" )
       (binlog/read-binlog)
       first
       ;; do something with the data
       check-read-data))

(comment
  ;; == binlog io test ==
  ;; compare the runtime of writing n megabytes to the binlog
  ;; and reading those n megabytes from the binlog
  ;; In /etc/mysql/my.cnf set:
  ;;   max_binlog_size=1m
  ;;   binlog_format=ROW
  ;; and start the jvm with a reasonable amout of memory: java -server -Xmx256m -cp ...
  ;; (1) create the test database
  (binlog-io-setup)
  ;; (2) benchmark the database
  (with-progress-reporting (bench (big-table-update) :verbose))
  ;; (3) benchmark the binlog parser
  (with-progress-reporting (bench (read-penultimate-binlog) :verbose))
  
  )


(def state (agent {}))

(defn binlog2-results [{s :start-time, e :end-time c :counter :as ag} result-list-key]
  (let [total-time (double (/ (double (- e s)) 1000000.0))]
    (println (format "Total time for %s turns: %sms" @c total-time))
    (update-in ag [result-list-key] #(if % (conj % %2) [%2]) total-time)))

(defn binlog2 [s times]
  ;;(reset! ping-start (System/nanoTime))
  ;;(reset! latencies [])
  ;;(when binlog-state (send-off binlog-state binlog/cdc-stop))
  (create-big-table)
  (let [ag *agent*
        counter (atom 0)
        bs (binlog/cdc-init (fn [_]
                              (if (< (swap! counter inc) times)
                                (big-table-update)
                                (let [end-time (System/nanoTime)]
                                  (send ag (fn [a]
                                             (send-off (:binlog-state a) binlog/cdc-stop)
                                             (assoc a :end-time end-time)))
                                  (send ag binlog2-results :binlog2)))))]
    (let [ret (assoc s
                :start-time (System/nanoTime)
                :binlog-state bs
                :counter counter
                :end-time 0)]
      (send-off bs
                (fn [a]
                  (let [ret (binlog/cdc-start a "/var/log/mysql/binlog-files.index")]
                    ;; start the benchmarking right after cdc has been started
                    (send ag (fn [a]
                               (let [s (System/nanoTime)]
                                 (big-table-update)
                                 (assoc a :start-time s))))
                    ret)))
      ret)))

(defn binlog3 [s times]
  (create-big-table)
  (let [counter (atom 0)
        ret (assoc s
              :start-time (System/nanoTime)
              :counter counter
              :end-time 0)
        step (fn step [a] (if (< (swap! counter inc) times)
                            (do (big-table-update)
                                (send *agent* step)
                                a)
                            (let [end-time (System/nanoTime)]
                              (send *agent* binlog2-results)
                              (assoc a :end-time end-time :binlog3))))]
    (big-table-update)
    (send *agent* step)
    ret))

(defn binlog4 [s times]
  (let [counter (atom 0)
        ret (assoc s
              :start-time (System/nanoTime)
              :counter counter
              :end-time 0)
        ms0 (first (mysql-query "show master status"))
        ;; updating a single row generates 4 events:
        ;; query - table-map - update-row - xid
        _ (big-table-update)
        start-pos (:position ms0)
        logfile (str "/var/log/mysql/" (:file ms0))
        step (fn step [a] (if (< (swap! counter inc) times)
                            (if (= 4 (count (first (binlog/read-binlog logfile start-pos))))
                              (do (send *agent* step)
                                  a)
                              (throwf "did not read 4 events!"))
                            (let [end-time (System/nanoTime)]
                              (send *agent* binlog2-results :binlog4)
                              (assoc a :end-time end-time))))]
    (send *agent* step)
    ret))

(defn binlog-clean-state [s]
  (dissoc s :binlog-state :start-time :end-time :counter))

(defn save-csv-data []
  (io/spit "~/tmp/binlog-latency.csv"
           (map
            (fn [a b c] (str a "," b "," c "\n"))
            (:binlog2 @state) (:binlog3 @state) (:binlog4 @state))))

(comment
  ;; == testing binlog latency performance ==
  (send state binlog2 100) ;; cdc-full
  (send state binlog3 100) ;; raw db updates
  (send state binlog4 100) ;; plain reads
  ;; latency = cdc-full - raw-db-updates - plain-reads
  (binlog-clean-state)
  
  )






(comment
  
  (do
    (def *queue* (LinkedBlockingQueue. 100000))
    (def *state* (binlog/cdc-init #(.put #^BlockingQueue *queue* %)))
    (send-off *state* binlog/cdc-start "/var/log/mysql/binlog-files.index"))

  (dotimes [n 10]
    (mysql-do "update foo.auto set value = '_' where value like '%X%'")
    (mysql-do "update foo.auto set value = 'X' where value like '%_%'"))
  (def _events (.poll *queue* 200 TimeUnit/MILLISECONDS))
  (do (clear-agent-errors *state*)
      (send *state* binlog/cdc-stop))
  (-> *state* agent-error throw)
  
  
  (def *sql-source* (agent {:latency 1000 :run true :turns 0}))
  (defn sql-generator [s]
    (if (:run s)
      (do (mysql-do "update foo.auto set value = '1'")
          (mysql-do "update foo.auto set value = '-1'")
          (Thread/sleep (:latency s))
          (send-off *agent* sql-generator)
          (update-in s [:turns] inc))
      s))
  (send-off *sql-source* sql-generator)
  (send-off *sql-source* assoc :run false)

  (def *cdc* (agent {:sum 0 :events 0 :run true :turns 0}))
  (defn reduce-events [s]
    (if (:run s)
      (let [ev (.take *queue*) ;; .take blocks
            sum (->> ev
                     (filter #(= (:type %) 'UPDATE_ROWS_EVENT))
                     (mapcat :rows)
                     (map second)
                     (map second)
                     (map #(Integer/valueOf %))
                     (reduce + (:sum s)))
            news (assoc s
                   :sum sum
                   :events (+ (:events s) (count ev))
                   :turns (+ (:turns s) 1))]
        (send-off *agent* reduce-events)
        (println "read" (:events news) "and got" (:sum news))
        news)
      s))
  (send-off *cdc* reduce-events)
  (send-off *cdc* assoc :run false)
  (send-off *cdc* assoc :run true)
  (clear-agent-errors *cdc*)
  (reduce-events {:sum 0 :events 0 :run true}))


(comment
  ;; playing with jdbc

  (require 'cdc.jdbc :reload)
  (def _c (sql-i/get-connection {:classname "cdc.jdbc.CDCDriver"
                                 :subprotocol "mysql-cdc"
                                 :subname "/var/log/binlog-files.index"}))
  
  (def _c (.connect (cdc.jdbc.CDCDriver.)
                    "jdbc:mysql-cdc:/var/log/mysql/binlog-files.index" nil))
  (.close _c)
  
  (def _s (.createStatement _c))
  (def _r (.executeQuery _s "select * from foo.auto"))

  (pprint @jdbc/jdbc-state)

  (defn test-lazy-concat []
    (let [q (doto (java.util.concurrent.LinkedBlockingQueue. 10)
              (.put [[1 2 3 4]
                     [5 6 7 8]
                     [9 10 11 12]]))
          s (->> (repeatedly #(lazy-seq (.take q)))
                 ;;(repeatedly #(.take q))
                 (filter identity)
                 (apply concat)
                 (map #(lazy-seq %))
                 (apply concat)
                 (filter identity))]
      s))


;;;(jdbc/create-resultset _s "select * from foo.auto")
  (mysql-do "insert into foo.auto (value) values ('in ordnung')" )
  (mysql-query "show master status" )

  (.next _r)

  (.getString _r 2)
  (pprint (seq (.getFields (type _r))))

  )