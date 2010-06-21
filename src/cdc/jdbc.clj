
(ns cdc.jdbc
  (:use [clojure.pprint :only [pprint]])
  (:require [cdc.mysql-binlog :as binlog]
            [clojure.contrib.sql.internal :as sql])
  (:import (java.sql DriverManager
                     DriverPropertyInfo)
           (java.util Properties)
           java.util.concurrent.LinkedBlockingQueue))
;; do not import the classes we are implementing

(def jdbc-state (ref {:connection nil
                      :statement nil
                      :resultset nil
                      :binlog-state nil
                      :queue nil}))

(defn set-new-queue
  "set a new queue"
  []
  (let [queue (LinkedBlockingQueue. 10)
        event-fn #(do (println "event-fn: got" (count %) "events!")
                      (.put #^LinkedBlockingQueue queue %))]
    (dosync (alter jdbc-state assoc :queue queue)
            (send (:binlog-state @jdbc-state) assoc :event-fn event-fn))))

(defn throwf-sql-not-supported [& args]
  (throw (java.sql.SQLFeatureNotSupportedException. (apply format (or args [""])))))

(defn throwf-unsupported [& args]
  (throw (UnsupportedOperationException. (apply format (or args [""])))))

(defn throwf-illegal [& args]
  (throw (IllegalArgumentException. (apply format (or args [""])))))

(defn throwf [& args]
  (throw (Exception. (apply format (or args [""])))))

(defn driver-property-info [& {n :name,
                               v :value,
                               d :description,
                               c :choices,
                               r? :required}]
  (let [info (DriverPropertyInfo. (str n) (str v))]
    (set! (.choices info) (into-array String (map str c)))
    (set! (.description info) (str d))
    (set! (.required info) (boolean r?))
    info))

;;(def _ee (binlog/read-binlog "/var/log/mysql/binlog.001024"))

(deftype ResultSetMetaData [schema table row]
  java.sql.ResultSetMetaData 
  (getCatalogName [t c] "")       ;; Gets the designated column's table's catalog name.
  (getColumnClassName [t c] (type (nth row c)))   ;;Returns the fully-qualified name of the Java class whose instances are manufactured if the method ResultSet.getObject is called to retrieve a value from the column.
  (getColumnCount [t] (count row))       ;; int; Returns the number of columns in this ResultSet object.
  (getColumnDisplaySize [t c] 1024) ;; int; Indicates the designated column's normal maximum width in characters.
  (getColumnLabel [t c] (str c)) ;; String; Gets the designated column's suggested title for use in printouts and displays.
  (getColumnName [t c] (str c)) ;; String; Get the designated column's name.
  (getColumnType [t c] ;; int; Retrieves the designated column's SQL type.
                 (let [v (nth row c)]
                   (cond (decimal? v) java.sql.Types/DECIMAL 
                         (integer? v) java.sql.Types/INTEGER
                         (string? c) java.sql.Types/VARCHAR
                         :else (throwf "unsupported type: %s" (type v)))))
  (getColumnTypeName [t c] (str (.getColumnType t c))) ;; String; Retrieves the designated column's database-specific type name.
  (getPrecision [t c] 10) ;; int; Get the designated column's specified column size.
  (getScale [t c] 10) ;; int; Gets the designated column's number of digits to right of the decimal point.
  (getSchemaName [t c] schema) ;; String; Get the designated column's table's schema.
  (getTableName [t c] table) ;; String; Gets the designated column's table name.
  (isAutoIncrement [t c] false) ;; boolean; Indicates whether the designated column is automatically numbered.
  (isCaseSensitive [t c] true) ;; boolean; Indicates whether a column's case matters.
  (isCurrency [t c] false) ;; boolean; Indicates whether the designated column is a cash value.
  (isDefinitelyWritable [t c] false) ;; boolean; Indicates whether a write on the designated column will definitely succeed.
  (isNullable [t c] true) ;; int; Indicates the nullability of values in the designated column.
  (isReadOnly [t c] true) ;; boolean; Indicates whether the designated column is definitely not writable.
  (isSearchable [t c] false) ;; boolean; Indicates whether the designated column can be used in a where clause.
  (isSigned [t c] true) ;; boolean; Indicates whether values in the designated column are signed numbers.
  (isWritable [t c] false) ;; boolean; Indicates whether it is possible for a write on the designated column to succeed.
)

(defn rows-delta-type
  "Return a fn that, given an event returns a seq of rows according to
  the quantifier."
  [{r :rows, t :type, tbl :table}]
  (mapcat (condp = t
            'WRITE_ROWS_EVENT #(list (conj % "insert"))
            'DELETE_ROWS_EVENT #(list (conj % "delete"))
            'UPDATE_ROWS_EVENT #(list (conj (first %) "update-before")
                                      (conj (second %) "update"))
            ;; ignore non-data events
            nil)
          r))

(def _example-statement "select * from \"foo\".\"auto\" where _delta_type = '  insert'")

(defn tokenize-statement [s]
  (loop [[t & more :as s] (.split s " ")
         ast []]
    (cond (empty? s) ast
          (or (= t "\"") (= t "'"))
          (recur (drop-while #(not= % t) more)
                 (->> more
                      (take-while #(not= % t))
                      (map #(if (= "" %) " " %))
                      (apply str t)
                      (conj ast)))
          (= t "")
          (recur more ast)
          :else
          (recur more (conj ast t)))))

(defn parse-statement [s]
  (loop [[t & more :as s] (tokenize-statement s)
         ast {:select nil :from nil :where nil}]
    (cond (empty? s) ast
          (#{"select"} t)
          (recur (next more) (assoc ast :select (first more)))
          (#{"from"} t)
          (recur (drop-while #(not= % "where") more)
                 (assoc ast :from (apply str (take-while #(not= % "where") more))))
          (= t "where")
          (assoc ast :where (next s))
          :else
          (throwf-illegal "unknown statement: %s" t))))

;;(parse-statement _example-statement)

(defn really-lazy-concat
  "circumvent a bug in mapcat which prevents full lazyness."
  ([r]
     (lazy-seq (really-lazy-concat (first r) (rest r))))
  ([s r]
     (lazy-seq
      (if (empty? s)
        (if (empty? r)
          nil
          (really-lazy-concat r))
        (cons (first s)
              (really-lazy-concat (rest s) r))))))

(defn create-resultset-seq
  "given a simple sql expression and queue, return lazy seq of vectors
  from queue."
  [sql queue]
  (let [{table :from [_ _ dtype] :where} (parse-statement sql)
        dtype (when dtype (.replace dtype "'" ""))
        [table-name schema-name] (-> table ;; for now, don't allow dots in table names
                                     (.replace "\"" "")
                                     (.split "\\.")
                                     reverse)
        s (->> (repeatedly #(let [_ (println "before taking")
                                  e (.take queue)
                                  _ (println "took" (count e) "events")]
                              e))
               (really-lazy-concat)
               (filter #(and (= (:table-name %) table-name) (= (:db-name %) schema-name)))
               (map rows-delta-type)
               (really-lazy-concat)
               (filter (if dtype
                         #(= (last %) dtype)
                         identity)))]
    s))

;; (defn test-create-resultset-seq []
;;   (let [[evts] (binlog/read-binlog "/var/log/mysql/binlog.001028")
;;         q (doto (LinkedBlockingQueue. 10)
;;             (.put evts))
;;         sql "select * from foo.auto where _delta_type = 'insert'"
;;         s (create-resultset-seq sql q)]
;;     (take 10 s)))

(defmacro getcol []
  `(if-let [v# (nth (first ~'rows) ~'i)]
     (do (set! ~'was-null? false)
         v#)
     (set! ~'was-null? true)))

;; please, single threaded access only!
;; nothing coordinated, step back!
(deftype ResultSet [#^{:volatile-mutable true} rows ;; a set of vectors of columnvalues
                    #^{:volatile-mutable true} was-null?
                    sql]
  java.sql.ResultSet
  ;; essential methods
  (close [t] (dosync (set-new-queue)
                     (alter jdbc-state assoc :resultset nil)))
  (isClosed [t])
  (next [t]
        ;; moves cursor one row forward
        (set! rows (next rows)))
  (wasNull [t] (boolean was-null?)) ;; whether the last access to any col was sql-null
  (getMetaData [t] (ResultSetMetaData. "" "" (first rows))) ;; ResultSetMetaData
  (findColumn [t label]) ;; returns the colnumber
  
  ;; data getters
  (^String getString [t ^int i] (getcol))
  (^String getString [t ^String c])
  (^boolean getBoolean [t ^int i])
  (^boolean getBoolean [t ^String c])
  (^byte getByte [t ^int i] (getcol))
  (^byte getByte [t ^String c])
  (^short getShort [t ^int i] (getcol))
  (^short getShort [t ^String c])
  (^int getInt [t ^int i]  (getcol))
  (^int getInt [t ^String c])
  (^long getLong [t ^int i]  (getcol))
  (^long getLong [t ^String c])
  (^float getFloat [t ^int i]  (getcol))
  (^float getFloat [t ^String c])
  (^double getDouble [t ^int i]  (getcol))
  (^double getDouble [t ^String c])
  (^BigDecimal getBigDecimal [t ^int i]  (getcol))
  (^BigDecimal getBigDecimal [t ^String c])
  (^BigDecimal getBigDecimal [t ^int i ^int scale])
  (^BigDecimal getBigDecimal [t ^String c ^int scale])
  (^bytes getBytes [t ^int i])
  (^bytes getBytes [t ^String c])
  (^java.sql.Date getDate [t ^int i])
  (^java.sql.Date getDate [t ^String c])
  (^java.sql.Date getDate [t ^int i ^java.util.Calendar cal])
  (^java.sql.Date getDate [t ^String c ^java.util.Calendar cal])
  (^java.sql.Time getTime [t ^int i])
  (^java.sql.Time getTime [t ^String c])
  (^java.sql.Time getTime [t ^int i ^java.util.Calendar cal])
  (^java.sql.Time getTime [t ^String c ^java.util.Calendar cal])
  (^java.sql.Timestamp getTimestamp [t ^int i])
  (^java.sql.Timestamp getTimestamp [t ^String c])
  (^java.sql.Timestamp getTimestamp [t ^int i ^java.util.Calendar cal])
  (^java.sql.Timestamp getTimestamp [t ^String c ^java.util.Calendar cal])
  (^java.io.InputStream getAsciiStream [t ^int i])
  (^java.io.InputStream getAsciiStream [t ^String c])
  (^java.io.InputStream getUnicodeStream [t ^int i])
  (^java.io.InputStream getUnicodeStream [t ^String c])
  (^java.io.InputStream getBinaryStream [t ^int i])
  (^java.io.InputStream getBinaryStream [t ^String c])
  (getObject [t ^int i])
  (getObject [t ^String c])
  (getObject [t ^int i ^java.util.Map m]) ;; get object with mapping
  (getObject [t ^String c ^java.util.Map m])
  (^java.io.Reader getCharacterStream [t ^int i])
  (^java.io.Reader getCharacterStream [t ^String c])
  (^java.sql.Ref getRef [t ^int i])
  (^java.sql.Ref getRef [t ^String c])
  (^java.sql.Blob getBlob [t ^int i])
  (^java.sql.Blob getBlob [t ^String c])
  (^java.sql.Clob getClob [t ^int i])
  (^java.sql.Clob getClob [t ^String c])
  (^java.sql.Array getArray [t ^int i])
  (^java.sql.Array getArray [t ^String c])
  (^java.net.URL getURL [t ^int i])
  (^java.net.URL getURL [t ^String c])
  (^java.sql.NClob getNClob [t ^int i])
  (^java.sql.NClob getNClob [t ^String c])
  (^java.sql.SQLXML getSQLXML [t ^int i])
  (^java.sql.SQLXML getSQLXML [t ^String c])
  (^String getNString [t ^int i])
  (^String getNString [t ^String c])
  (^java.io.Reader getNCharacterStream [t ^int i])
  (^java.io.Reader getNCharacterStream [t ^String c])
  
  ;; unsupported cursor methods
  (getCursorName [t] (throwf-sql-not-supported))  
  (absolute [t r] (throwf-sql-not-supported))
  (afterLast [t] (throwf-sql-not-supported))
  (beforeFirst [t] (throwf-sql-not-supported))
  (relative [t r] (throwf-sql-not-supported))
  (isBeforeFirst [t] (throwf-sql-not-supported))
  (isAfterLast [t] (throwf-sql-not-supported))
  (isFirst [t] (throwf-sql-not-supported))
  (isLast [t] (throwf-sql-not-supported))
  (first [t] (throwf-sql-not-supported))
  (last [t] (throwf-sql-not-supported))
  (previous [t] (throwf-sql-not-supported))
  (setFetchDirection [t dir] (throwf-sql-not-supported))
  (getFetchDirection [t] 0)
  (moveToInsertRow [t] (throwf-sql-not-supported))
  (moveToCurrentRow [t] (throwf-sql-not-supported))
  
  ;; misc
  (setFetchSize [t s])
  (getFetchSize [t] 0)
  (getType [t] java.sql.ResultSet/TYPE_FORWARD_ONLY)
  (getConcurrency [t] java.sql.ResultSet/CONCUR_READ_ONLY)
  (getRow [t] (throwf-sql-not-supported))
  (getWarnings [t])
  (clearWarnings [t])
  (^java.sql.RowId getRowId [t ^int i] (throwf-sql-not-supported))
  (^java.sql.RowId getRowId [t ^String c] (throwf-sql-not-supported))
  (getHoldability [t] java.sql.ResultSet/HOLD_CURSORS_OVER_COMMIT)
  
  ;; update functions
  (rowUpdated [t] (throwf-sql-not-supported))
  (rowDeleted [t] (throwf-sql-not-supported))
  (rowInserted [t] (throwf-sql-not-supported))

  (updateRow [t] (throwf-sql-not-supported))
  (deleteRow [t] (throwf-sql-not-supported))
  (insertRow [t] (throwf-sql-not-supported))
  (cancelRowUpdates [t] (throwf-sql-not-supported)))

(defn create-resultset [sql]
  (ResultSet. (create-resultset-seq sql (:queue @jdbc-state)) nil sql))

(deftype Statement []
  java.sql.Statement
  ;; essential methods
  (close [t] (dosync (when-let [rs (:resultset @jdbc-state)] (.close rs))
                     (alter jdbc-state assoc :statement nil)))
  (executeQuery [t sql]
                (dosync (if-let [rs (:resultset @jdbc-state)]
                          (throwf "Close Resultset %s first." rs)
                          (let [rs (create-resultset sql)]
                            (alter jdbc-state assoc :resultset rs)
                            rs))))
  (execute [t sql] (.executeQuery t sql))
  ;; we do not have autogenerated keys
  (^boolean execute [t ^String sql ^int _] (.execute t sql))
  (^boolean execute [t ^String sql ^ints _] (boolean (.execute t sql)))
  (^boolean execute [t ^String sql ^"[Ljava.lang.String;" _] (boolean (.execute t sql)))

  (getResultSet [t] (:resultset @jdbc-state))
  
  ;; misc, mostly unsupported or ignored
  (addBatch [t s] (throwf-unsupported))
  (executeBatch [t] (throwf-unsupported))
  (cancel [t] (throwf-unsupported))
  (clearBatch [t] (throwf-unsupported))
  (clearWarnings [t] (throwf-unsupported))
  (executeUpdate [t sql] (throwf-unsupported))
  (^int executeUpdate [t ^String sql ^int _] (throwf-unsupported))
  (^int executeUpdate [t ^String sql ^ints _] (throwf-unsupported))
  (^int executeUpdate [t ^String sql ^"[Ljava.lang.String;" _] (throwf-unsupported))
  
  (getConnection [t] (:connection @jdbc-state))
  (getFetchDirection [t] java.sql.ResultSet/FETCH_UNKNOWN)
  (getFetchSize [t] 1)
  (getGeneratedKeys [t] (throwf-sql-not-supported))
  (getMaxFieldSize [t] 0)     ;; unlimited
  (getMoreResults [t] false)  ;; only ever one rs
  (getMoreResults [t _] false)
  (getQueryTimeout [t] 0) ;; no timeout
  (getResultSetConcurrency [t] java.sql.ResultSet/CONCUR_READ_ONLY)
  (getResultSetHoldability [t] java.sql.ResultSet/HOLD_CURSORS_OVER_COMMIT)
  (getResultSetType [t] java.sql.ResultSet/TYPE_FORWARD_ONLY)
  (getUpdateCount [t] -1)
  (getWarnings [t] nil)
  (isClosed [t] false)
  (isPoolable [t] false)
  (setCursorName [t s] (throwf-sql-not-supported))
  (setEscapeProcessing [t flag]) ;; ignore
  (setFetchDirection [t dir])    ;; ignore
  (setFetchSize [t n])           ;; ignore
  (setMaxFieldSize [t s]) ;; ignore, may be useful to obey to for varchars
  (setMaxRows [t s])      ;; ignore
  (setPoolable [t flag])  ;; ignore
  (setQueryTimeout [t seconds]))

(deftype Connection [binlog-state]
  java.sql.Connection
  (close [_]
         (dosync (when-let [st (:statement @jdbc-state)] (.close st))
                 (alter jdbc-state assoc :closed true :connection nil)
                 (send-off (:binlog-state @jdbc-state) binlog/cdc-stop))
         ;;(when mysql-conn (.close mysql-conn))
         )
  (createStatement [t]
                   (dosync (if (:statement @jdbc-state)
                             (throwf "Close statement %s first" (:statement @jdbc-state))
                             (let [s (Statement.)]
                               (alter jdbc-state assoc :statement s)
                               s))))
  (createStatement [t rs-type rs-cncrcy])
  (createStatement [t rs-type rs-cncrcy rs-holdabilityb])
  (getMetaData [t])
  java.sql.Wrapper
  (isWrapperFor [_ iface])
  (unwrap [_ iface]))

(defn create-connection [binlog-index-file user pwd]
  (let [state (binlog/cdc-init (fn [_]))
        ;; mysql-conn (sql/get-connection
        ;;             {:classname "com.mysql.jdbc.Driver"
        ;;              :subprotocol "mysql"
        ;;              :subname "//localhost"
        ;;              ;;:username user
        ;;              :user user
        ;;              :password pwd})
        conn (Connection. state)]
    (dosync (alter jdbc-state assoc
                   :statement nil
                   :resultset nil
                   :connection conn
                   :binlog-state state
                   :closed false))
    (set-new-queue)
    (send state binlog/cdc-start binlog-index-file)
    conn))

(defn extract-url-param [url-string]
  (second (re-matches #"jdbc:mysql-cdc:(.*)"
                      url-string)))

;; URL: jdbc:mysql-cdc:*
(deftype CDCDriver []
  java.sql.Driver
  (acceptsURL [t url]
              (boolean (re-matches #"jdbc:mysql-cdc:.*" url)))
  (getMajorVersion [t] 0)
  (getMinorVersion [t] 1)
  (getPropertyInfo [t url info]
                   ;; there should be only one connection object
                   ;; or, the first connection object should setup the cdc-process
                   ;; and the last connection should shut it down
                   (into-array DriverPropertyInfo
                               ;; [(driver-property-info :name "user" :description "MySQL username" :required true :value "root")
                               ;;  (driver-property-info :name "password" :description "MySQL password" :required true :value "root")]
                               []
                               ))
  (jdbcCompliant [t] false)
  (connect [t url prop]
           (dosync (if-let [c (:connection @jdbc-state)]
                     c
                     (let [c (create-connection (extract-url-param url) nil nil)]
                       (alter jdbc-state assoc :connection c)
                       c)))))

(java.sql.DriverManager/registerDriver (CDCDriver.))



(comment
  (pprint @jdbc-state)

  (-> (:binlog-state @jdbc-state) agent-error throw)
  (-> (:binlog-state @jdbc-state) clear-agent-errors)
  )




;; (-> (com.mysql.jdbc.Driver.)
;;     (.getPropertyInfo "" (Properties.))
;;     first
;;     .value
;;     )
;; nil
;; true
;; nil
;; "HOST"