
;;; functions to read the contents of a binary log, starting at offset x
;;;   `read-binlog'
;;; and a way to set up an INotify listener to instantly read changes made
;;; by the database to the binlog file

(ns cdc.mysql-binlog
  (:use (clojure.contrib pprint except))
  (:import (java.io File FileInputStream)
           java.util.Arrays
           java.util.concurrent.LinkedBlockingQueue
           java.util.concurrent.BlockingQueue
           java.util.concurrent.atomic.AtomicReference
           (java.nio ByteBuffer
                     ByteOrder)
           (java.nio.channels FileChannel
                              FileChannel$MapMode)
           (net.contentobjects.jnotify JNotify
                                       JNotifyListener)
           cdc.mysql.Decimal))

(def binlog-magic (->> [0xfe 0x62 0x69 0x6e] (map #(.byteValue %)) byte-array))

(defn binlog-file? [f]
  (with-open [i (-> f File. FileInputStream.)]
    (let [b (byte-array 4)]
      (.read i b)
      (Arrays/equals binlog-magic b))))

;; symbol constants, the stuff that is #define'd in c sources
(def *constants* {}) ;; a map of symbols to literals (supposedly numbers)

(defmacro c
  "resolves a constant from *constants*"
  [constant-name]
  (or (*constants* constant-name)
      (throwf "unknown constant symbol: %s" constant-name)))

(defmacro defconstants
  "Define one or more constants.
  A constant is a pair of symbol, expr.
  Constants are resolved at compiletime using the (c <constant-symbol>) macro."
  [& name-value-pairs]
  `(do ~@(map (fn [[sym expr]]
             `(alter-var-root #'*constants*
                              (fn [c#]
                                (binding [*constants* c#] 
                                  (assoc *constants* '~sym ~expr)))))
           (partition 2 name-value-pairs))))

;; -- binlog bnf:
;; binlog file: binlog-magic, descriptor-event, event*, logrotation-event
;; descriptor-event: specifies binlog version
;; event: header, data
;; logrotation-event: specifies next logfile
;; -- binlog versions
;; v1,v3 and v4, the latter used in >=mysql5.0
;; -- little endian


;;; lowlevel tools

(defn strip-0s
  "Remove trailing 0s from bytes a beginning from the first 0."
  [#^bytes a]
  (let [cnt (->> a (take-while #(not (zero? %))) count)]
    (Arrays/copyOf a cnt)))

(defn cstring
  "Return a String from the array a, possibly ignoring the trailing 0."
  [#^bytes a]
  (if (-> a (aget (dec (alength a))) int (== 0))
    (String. a 0 (dec (alength a)))
    (String. a)))

(defn n-uint
  "returns a long value made up of (< 0 n 9) bytes from the array a
  in little endian format."
  [#^bytes a]
  (areduce a i res (long 0)
           (long (bit-or (int res)
                         (int (bit-shift-left (bit-and (int 0xff) (int (aget a i)))
                                               (int (* i 8))))))))

(defn bytes-required-for-bits
  "Return the numbers of bytes required to encode an n-bitfield."
  [n]
  (quot (+ 7 n) 8))

(defn bitfield-count-bits
  "count all set bits in the bitfield."
  [#^bytes bf]
  (areduce bf i ret (int 0)
           (unchecked-add ret (Integer/bitCount (bit-and (int (aget bf i)) 0xff)))))

;; (defn bit-seq
;;   "return a seq of 8 booleans from a byte beginning with
;;   the most significant bit."
;;   [b]
;;   (map #(= (bit-and 1 (bit-shift-right b %)) 1) (range 7 -1 -1)))

(defn nth-bit
  "Return the nth bit from an array of bytes using the following scheme:.
  nth:   7 6 5 4 3 2 1 0 | f e d c b a 9 8 | ...
  bytes:      byte0      |      byte1      | byteN"
  [#^bytes a n]
  (bit-and 1 (bit-shift-right (int (aget a (quot n 8)))
                              (int (rem n 8)))))

;; (defn bitfield-filter
;;   "Filter all elements in seq where the corresponding bit is set."
;;   [bf s]
;;   (filter identity (map #(and %2 %1) s (mapcat bit-seq bf))))

(defmacro ubyte2int
  "transforms unsigned byte b into an int."
  [b] `(bit-and ~b 0xff))

;;; byte buffer helpers

(defn get-ubyte
  "and return an Integer."
  ([#^ByteBuffer b] (bit-and (int (.get b)) 0xff))
  ([#^ByteBuffer b o] (bit-and (int (.get b (int o))) 0xff)))

(defn get-ushort
  "and return an Integer."
  ([#^ByteBuffer b] (bit-and (int (.getShort b)) 0xffff))
  ([#^ByteBuffer b o] (bit-and (int (.getShort b (int o))) 0xffff)))

(defn get-uint
  "and return a Long."
  ([#^ByteBuffer b] (bit-and (.getInt b) 0xffffffff))
  ([#^ByteBuffer b o] (bit-and (.getInt b (int o)) 0xffffffff)))

(defn get-array
  "return an Array of bytes, alters the Buffers position"
  ([#^ByteBuffer b len]
     (let [a #^bytes (make-array Byte/TYPE len)]
       (.get b a)
       a))
  ([#^ByteBuffer b o len]
     (let [a #^bytes (make-array Byte/TYPE len)]
       ;;(-> b (.position o) (.get a))
       (doto b
         (.position o)
         (.get a))
       a)))

(defn get-length-hinted-string
  "Read a string from b, including the trailing 0.
  Assume the first byte is the length of the string.
  Assume that length does NOT include the trailing 0.
  Remove the trailing 0 from the string."
  [#^ByteBuffer b]
  (let [len (get-ubyte b)
        s (if (< 0 len)
            (String. #^bytes (get-array b len))
            "")]
    (.get b) ;; trailing 0
    s))

(defn little-endian
  "Set byte ordering on ByteBuffer b to little-endian, return b."
  [#^ByteBuffer b]
  (.order b ByteOrder/LITTLE_ENDIAN))

(defn get-packed-int
  "Decode and return the packed-integer at the ByteBuffers current position."
  [#^ByteBuffer b]
  ;; Packed Integers:
  ;;   Value Of     # Of Bytes  Description
  ;; First Byte   Following
  ;; ----------   ----------- -----------
  ;; 0-250        0           = value of first byte
  ;; 251          0           column value = NULL
  ;;                          only appropriate in a Row Data Packet
  ;; 252          2           = value of following 16-bit word
  ;; 253          3           = value of following 24-bit word
  ;; 254          8           = value of following 64-bit word
  (let [x (get-ubyte b)]
    (cond (<= 0 x 250) (int x)
          (= x 252) (get-ushort b)
          (= x 253) (-> b (get-array 3) n-uint)
          (= x 254) (.getLong b)
          :else (throwf "invalid first byte in packed integer field: %d" x))))

(defn get-bitfield
  "Return an array of bytes representing a bitfield of bitcount bits.
  Read alinged to bytes."
  [b bitcount]
  (get-array b (bytes-required-for-bits bitcount)))


;;; mysql binlog events

(def log-event-types 
     '{0 UNKNOWN_EVENT,
       1 START_EVENT_V3, ;; written at the top of each logfile
       2 QUERY_EVENT, ;; "Written when an updating statement is done. "
       3 STOP_EVENT,
       4 ROTATE_EVENT,
       5 INTVAR_EVENT, ;;  INVALID_INT_EVENT = LAST_INSERT_ID_EVENT = INSERT_ID_EVENT = 2;; for auto increments, not written in rowlog mode
       6 LOAD_EVENT,
       7 SLAVE_EVENT,
       8 CREATE_FILE_EVENT,
       9 APPEND_BLOCK_EVENT,
       10 EXEC_LOAD_EVENT,
       11 DELETE_FILE_EVENT,
       ;; NEW_LOAD_EVENT is like LOAD_EVENT except that it has a longer
       ;; sql_ex, allowing multibyte TERMINATED BY etc; both types share the
       ;; same class (Load_log_event)
       12 NEW_LOAD_EVENT,
       13 RAND_EVENT, ;; seeds of rand function, before a query_event that uses rand()
       14 USER_VAR_EVENT,
       15 FORMAT_DESCRIPTION_EVENT,
       16 XID_EVENT,
       17 BEGIN_LOAD_QUERY_EVENT,
       18 EXECUTE_LOAD_QUERY_EVENT,

       19 TABLE_MAP_EVENT,
       ;; These event numbers were used for 5.1.0 to 5.1.15 and are therefore obsolete.
   
       20 PRE_GA_WRITE_ROWS_EVENT,
       21 PRE_GA_UPDATE_ROWS_EVENT,
       22 PRE_GA_DELETE_ROWS_EVENT,
       ;; These event numbers are used from 5.1.16 and forward
   
       23 WRITE_ROWS_EVENT,
       24 UPDATE_ROWS_EVENT,
       25 DELETE_ROWS_EVENT,
       26 INCIDENT_EVENT ;;     Something out of the ordinary happened on the master
       ;; ENUM_END_EVENT
       })

(defconstants 
  unknown-event 0
  start-event-v3 1
  query-event 2
  stop-event 3
  rotate-event 4
  intvar-event 5
  load-event 6
  slave-event 7
  create-file-event 8
  append-block-event 9
  exec-load-event 10
  delete-file-event 11
  new-load-event 12
  rand-event 13
  user-var-event 14
  format-description-event 15
  xid-event 16
  begin-load-query-event 17
  execute-load-query-event 18
  table-map-event 19
  pre-ga-write-rows-event 20
  pre-ga-update-rows-event 21
  pre-ga-delete-rows-event 22
  write-rows-event 23
  update-rows-event 24
  delete-rows-event 25
  incident-event 26)

(defn read-event-header
  "b is a nio ByteBuffer, o is the offset into this buffer.
  Check wether the event has been completely written to disk.
  Return nil if its not the case."
  [#^ByteBuffer b o]
  ;; +=====================================+
  ;; | event  | timestamp         0 : 4    | ;; UTC, number of seconds since 1970
  ;; | header +----------------------------+
  ;; |        | type_code         4 : 1    |
  ;; |        +----------------------------+
  ;; |        | server_id         5 : 4    |
  ;; |        +----------------------------+
  ;; |        | event_length      9 : 4    | ;; header+data 
  ;; |        +----------------------------+
  ;; |        | next_position    13 : 4    | ;; v4: to the end of this event
  ;; |        +----------------------------+
  ;; |        | flags            17 : 2    | ;; LOG_EVENT_BINLOG_IN_USE_F=0x1
  ;; |        +----------------------------+
  ;; |        | extra_headers    19 : x-19 |
  ;; +=====================================+
  ;; | event  | fixed part        x : y    | ;; also called "post-header"
  ;; | data   +----------------------------+
  ;; |        | variable part              | ;; "body", "payload"
  ;; +=====================================+
  ;; x is given by the header_length field in the format description event (FDE). Currently, x is 19, so the extra_headers field is empty.
  ;; y is specific to the event type, and is given by the FDE. The fixed-part length is the same for all events of a given type, but may vary for different event types.
  (when (<= (+ o 19) (.capacity b))
    ;; header seems ok, read the header data
    (let [data {:timestamp (get-uint b (+ o 0))
                :type (get log-event-types (int (get-ubyte b (+ o 4))) (get-ubyte b (+ o 4)))
                :server-id (get-uint b (+ o 5))
                :event-len (get-uint b (+ o 9))
                :next (get-uint b (+ o 13))
                :extra-headers nil ;; currently
                :flags {:in-use (= (.getShort b (+ o 17)) 1)}
                :offset o}]
      ;; check wether the rest of this event has been written to disk (probably)
      (when (<= (+ (:event-len data) o) (.capacity b))
        data))))

;; +=====================================+ note: no extra-headers field, because a fd-event must be backwards-compatible
;; | event  | binlog_version   19 : 2    | = 4
;; | data   +----------------------------+
;; |        | server_version   21 : 50   |
;; |        +----------------------------+
;; |        | create_timestamp 71 : 4    |
;; |        +----------------------------+
;; |        | header_length    75 : 1    |
;; |        +----------------------------+
;; |        | post-header      76 : n    | = array of n bytes, one byte per event
;; |        | lengths for all            |   type that the server knows about
;; |        | event types                |
;; +=====================================+
(defn read-v4-format-description [#^ByteBuffer b e]
  (let [o (:offset e)
        header-len (get-ubyte b 75)
        ev-len (:event-len e)]
    (merge e {:binlog-version (get-ushort b (+ o 19))
              :server_version (String. #^bytes (strip-0s (get-array b (+ o 21) 50)))
              :create-timestamp (get-uint b (+ o 71))
              :header-len header-len
              :extra-headers-len (- header-len (+ o 19))
              :post-header-len (vec (get-array b (+ o 76) (- (:event-len e) 76)))
              })))

(defn read-query [#^ByteBuffer b e] ;; QUERY_EVENT
  ;;; fixed:
  ;; * 4 bytes. The ID of the thread that issued this statement. Needed for temporary tables. This is also useful for a DBA for knowing who did what on the master.
  ;; * 4 bytes. The time in seconds that the statement took to execute. Only useful for inspection by the DBA.
  ;; * 1 byte. The length of the name of the database which was the default database when the statement was executed. This name appears later, in the variable data part. It is necessary for statements such as INSERT INTO t VALUES(1) that don't specify the database and rely on the default database previously selected by USE.
  ;; * 2 bytes. The error code resulting from execution of the statement on the master. Error codes are defined in include/mysqld_error.h. 0 means no error. How come statements with a non-zero error code can exist in the binary log? This is mainly due to the use of non-transactional tables within transactions. For example, if an INSERT ... SELECT fails after inserting 1000 rows into a MyISAM table (for example, with a duplicate-key violation), we have to write this statement to the binary log, because it truly modified the MyISAM table. For transactional tables, there should be no event with a non-zero error code (though it can happen, for example if the connection was interrupted (Control-C)). The slave checks the error code: After executing the statement itself, it compares the error code it got with the error code in the event, and if they are different it stops replicating (unless --slave-skip-errors was used to ignore the error).
  ;; * 2 bytes (not present in v1, v3). The length of the status variable block.
  ;;; variable
  ;; * Zero or more status variables (not present in v1, v3). Each status variable consists of one byte code identifying the variable stored, followed by the value of the variable. The format of the value is variable-specific, as described later.
  ;; * The default database name (null-terminated).
  ;; * The SQL statement. The slave knows the size of the other fields in the variable part (the sizes are given in the fixed data part), so by subtraction it can know the size of the statement. 
  (let [b (.position b (+ 19 (:offset e)))
        ;; fixed
        thread-id (get-uint b)
        exec-time (get-uint b)
        default-db-name-len (inc (get-ubyte b)) ;; + nullbyte
        error-code (get-ushort b)
        status-block-len (get-ushort b)
        ;; variable
        status-vars (get-array b status-block-len)
        default-db-name (-> b (get-array default-db-name-len) cstring)
        sql (-> b (get-array (- (+ (:offset e) (:event-len e)) ;;(:next e)
                                (.position b)))
                cstring)] ;;(- (.position b) (:offset e) (:event-len e))
    (merge e {:thread-id thread-id
              :execution-time exec-time
              :error-code error-code
              :status-vars status-vars
              :default-db-name default-db-name
              :sql sql})))

(defn read-rotate [b e] ;; ROTATE_EVENT
  ;; Fixed data part:
  ;;     * 8 bytes. The position of the first event in the next log file. Always contains the number 4 (meaning the next event starts at position 4 in the next binary log). This field is not present in v1; presumably the value is assumed to be 4. 
  ;; Variable data part:
  ;;     * The name of the next binary log. The filename is not null-terminated. Its length is the event size minus the size of the fixed parts.
  (merge e {:next-file (-> b (get-array (+ 27 (:offset e)) (- (:event-len e) 27)) cstring)}))

(defn read-xid [#^ByteBuffer b e] ;; XID_EVENT
  (merge e {:xid (.getLong b (+ 19 (:offset e)))}))

;;; row based replication events

;; reading row-based replication log:
;; (1) read a table event, remember columns, 

;; Name                Identifier  Size of metadata in bytes    Description of metadata
;; MYSQL_TYPE_DECIMAL   0       0       No column metadata.
;; MYSQL_TYPE_TINY      1       0       No column metadata.
;; MYSQL_TYPE_SHORT     2       0       No column metadata.
;; MYSQL_TYPE_LONG      3       0       No column metadata.
;; MYSQL_TYPE_FLOAT     4       1 byte  1 byte unsigned integer, representing the "pack_length", which is equal to sizeof(float) on the server from which the event originates.
;; MYSQL_TYPE_DOUBLE    5       1 byte  1 byte unsigned integer, representing the "pack_length", which is equal to sizeof(double) on the server from which the event originates.
;; MYSQL_TYPE_NULL      6       0       No column metadata.
;; MYSQL_TYPE_TIMESTAMP 7       0       No column metadata.
;; MYSQL_TYPE_LONGLONG  8       0       No column metadata.
;; MYSQL_TYPE_INT24     9       0       No column metadata.
;; MYSQL_TYPE_DATE      10      0       No column metadata.
;; MYSQL_TYPE_TIME      11      0       No column metadata.
;; MYSQL_TYPE_DATETIME  12      0       No column metadata.
;; MYSQL_TYPE_YEAR      13      0       No column metadata.
;; MYSQL_TYPE_NEWDATE   14      –       This enumeration value is only used internally and cannot exist in a binlog.
;; MYSQL_TYPE_VARCHAR   15      2 bytes         2 byte unsigned integer representing the maximum length of the string.
;; MYSQL_TYPE_BIT       16      2 bytes         A 1 byte unsigned int representing the length in bits of the bitfield (0 to 64), followed by a 1 byte unsigned int representing the number of bytes occupied by the bitfield. The number of bytes is either int((length+7)/8) or int(length/8).
;; MYSQL_TYPE_NEWDECIMAL  246   2 bytes         A 1 byte unsigned int representing the precision, followed by a 1 byte unsigned int representing the number of decimals.
;; MYSQL_TYPE_ENUM        247   –       This enumeration value is only used internally and cannot exist in a binlog.
;; MYSQL_TYPE_SET         248   –       This enumeration value is only used internally and cannot exist in a binlog.
;; MYSQL_TYPE_TINY_BLOB   249   –       This enumeration value is only used internally and cannot exist in a binlog.
;; MYSQL_TYPE_MEDIUM_BLOB 250   –       This enumeration value is only used internally and cannot exist in a binlog.
;; MYSQL_TYPE_LONG_BLOB   251   –       This enumeration value is only used internally and cannot exist in a binlog.
;; MYSQL_TYPE_BLOB        252   1 byte  The pack length, i.e., the number of bytes needed to represent the length of the blob: 1, 2, 3, or 4.
;; MYSQL_TYPE_VAR_STRING  253   2 bytes         This is used to store both strings and enumeration values. The first byte is a enumeration value storing the real type, which may be either MYSQL_TYPE_VAR_STRING or MYSQL_TYPE_ENUM. The second byte is a 1 byte unsigned integer representing the field size, i.e., the number of bytes needed to store the length of the string.
;; MYSQL_TYPE_STRING      254   2 bytes         The first byte is always MYSQL_TYPE_VAR_STRING (i.e., 253). The second byte is the field size, i.e., the number of bytes in the representation of size of the string: 3 or 4.
;; MYSQL_TYPE_GEOMETRY    255   1 byte  The pack length, i.e., the number of bytes needed to represent the length of the geometry: 1, 2, 3, or 4.

(defconstants
  type-decimal 0
  type-tiny 1
  type-short 2
  type-long 3
  type-float 4
  type-double 5
  type-null 6
  type-timestamp 7
  type-longlong 8
  type-int24 9
  type-date 10
  type-time 11
  type-datetime 12
  type-year 13
  type-newdate 14
  type-varchar 15
  type-bit 16
  type-newdecimal -10
  type-enum -9
  type-set -8
  type-tiny-blob -7
  type-medium-blob -6
  type-long-blob -5
  type-blob -4
  type-var-string -3
  type-string -2
  type-geometry -1)

(defn int-keys-to-bytes [m]
  (zipmap (map #(.byteValue %) (keys m)) (vals m)))

(def column-metadata-lengths
     ;; column metadata is used for length of field data
     ;; keys are bytes
     (int-keys-to-bytes {0x0 0,
                         0x1 0,
                         0x2 0,
                         0x3 0,
                         0x4 1,
                         0x5 1,
                         0x6 0,
                         0x7 0,
                         0x8 0,
                         0x9 0,
                         0xa 0,
                         0xb 0,
                         0xc 0,
                         0xd 0,
                         0xf 2,
                         0x10 2,
                         0xf6 2,
                         0xfc 1,
                         0xfd 2,
                         0xfe 2,
                         0xff 1}))

(defn read-table-map-metadata-array
  "Return an int-array which contains the metadata for each column or 0.
  The ByteBuffer b has to be positioned at the start of the metadata section."
  [b column-types]
  (let [ct #^bytes column-types]
    (areduce ct i ret (int-array (alength ct))
             (let [l (column-metadata-lengths (aget ct i))]
               (cond (zero? l) nil
                     (== 1 l) (aset-int ret i (get-ubyte b))
                     (== 2 l) (aset-int ret i (get-ushort b))
                     :else (throwf "invalid column-metadata length: %d" l))
               ret))))

(defn read-table-map [#^ByteBuffer b e] ;; TABLE_MAP_EVENT
  ;; !!!: For null-terminated strings that are preceded by a length field, the length does __not_include_the_terminating_null_byte__, unless otherwise indicated. 
  ;; In row-based mode, every row operation event is preceded by a Table_map_log_event which maps a table definition to a number. The table definition consists of database name, table name, and column definitions.
  ;; 6 byte table_id
  ;; 2 byte flags (unused, 0)
  ;; only maps a table_name to a table_id
  ;; column names are NOT mapped
  ;; -> read column names from schema tables via normal jdbc connection
  ;;  (problably at startup and when a schema update was detected)
  (let [b (-> b (.position (+ 19 (:offset e))))
        table-id (n-uint (get-array b 6))
        flags (get-ushort b)
        db-name (get-length-hinted-string b)
        table-name (get-length-hinted-string b)
        column-count (get-packed-int b)
        coltypes (get-array b column-count)
        metadata-len (get-packed-int b)
        column-metadata (read-table-map-metadata-array b coltypes)
        null-bits nil] ;; ignore for now
    (merge e {:table-id table-id
              :table-name table-name
              :db-name db-name
              :column-types coltypes
              :column-meta column-metadata})))


;;; xxx_rows_events:

(defn read-type [#^ByteBuffer b type-id len]
  ;; len == column-meta: is almost exclusively used for length parameters
  (cond (= type-id (c type-set)) ;; a bitset
          (get-array b len)
        ;; enum
        (= type-id (c type-enum))
          (cond (= len 1) (get-ubyte b)
                (= len 2) (get-uint b)
                :else (throwf "invalid enum len: %d" len))
        ;; strings
        (or (= type-id (c type-string))
            (= type-id (c type-var-string))
            (= type-id (c type-varchar)))
          (if (< len 256)
            (String. #^bytes (get-array b (get-ubyte b)))
            ;;(println (format "reading string (%d) '%s'" len  s))
            (String. #^bytes (get-array b (get-ushort b))))
        
        ;; numbers
        (= type-id (c type-long))
          ;; TODO: how to differenciate between unsigned and signed?
          (.getInt b)

        ;; decimal
        (= type-id (c type-newdecimal))
          (let [precision (bit-and len 0xff)
                scale (bit-shift-right (int len) 8)
                size (Decimal/decimalBinSize precision scale)]
            ;;(println "read-type: decimalBinSize" precision scale size)
            (Decimal/binToDecimal (get-array b size) precision scale))
        :else (throwf "Cannot read type %" type-id)))

(defn read-field
  "Read the given column-type using the column-meta information from
  the ByteBuffer b."
  ;; see sql/log_event.cc:log_event_print_value function in mysql src
  [#^ByteBuffer b column-meta column-type]
  (if (and (== column-type (c type-string))
           (<= 256 column-meta))
    ;; special encoding of char(UTF8) fields
    (let [byte0 (bit-shift-left (int column-meta) 8)
          byte1 (bit-and column-meta 0xff)]
      (if (not= (bit-and byte0 0x30) ;; actually two ints
                0x30)
        (let [clen (bit-or byte1 (bit-shift-right (bit-xor (bit-and byte0 0x30) 0x30) 4))
              ctype (bit-or byte0 0x30)]
          (read-type b ctype clen))
        (if (or (== byte0 (c type-string))
                (== byte0 (c type-set))
                (== byte0 (c type-enum)))
          (read-type b byte0 byte1)
          (throwf "cannot handle column type %d" byte0))))
    (read-type b column-type column-meta)))

(defn read-row
  "Read a single row from ByteBuffer."
  [b ;; the bytebuffer, assumed to be in positioned at the start of the rows section (the variable part of the XXX_ROWS_EVENT)
   #^bytes column-types ;; an array of bytes, serving as a map from col# -> coltype
   #^ints column-meta ;; an array of ints, serving as a map from col# -> colmeta (column metadata is at most 2bytes long, byte0=LSB, byte4=MSB)
   used-columns] ;; a bitfield, access with nth-bit, each nth bit indicates that the nth column is used in the row
  (let [nulls (get-bitfield b (bitfield-count-bits used-columns))]
    (loop [col-idx 0  ;; index into column-types, column-meta and used-columns(bitfield)
           null-idx 0 ;; index into the nulls (bitfield)
           data []]   ;; map of column-number to field-data (keys are numbers)
      (if (< col-idx (alength column-types))
        (if-not (zero? (nth-bit used-columns col-idx))
          ;; used field
          (recur (unchecked-inc col-idx)
                 (unchecked-inc null-idx)
                 (conj data
                       ;; non-null
                       (if (zero? (nth-bit nulls null-idx))
                         (read-field b
                                     (aget column-meta  col-idx)
                                     (aget column-types col-idx))
                         ;; null
                         nil)))
          ;; unused field
          (recur (unchecked-inc col-idx)
                 null-idx
                 (conj data '_)))
        data))))

(defn read-wud-rows [#^ByteBuffer b e table-map-event] ;; WRITE_ROWS_EVENT, DELETE_ROWS_EVENT, UPDATE_ROWS_EVENT
  ;; Write_rows_log_event/WRITE_ROWS_EVENT
  ;; Used for row-based binary logging beginning with MySQL 5.1.18.
  ;; [TODO: following needs verification; it's guesswork]

  ;; Fixed data part:
  ;;
  ;;     * 6 bytes. The table ID.
  ;;     * 2 bytes. Reserved for future use. 
  ;;
  ;; Variable data part:
  ;;     * Packed integer. The number of columns in the table.
  ;;     * Variable-sized. Bit-field indicating whether each column is
  ;;       used, one bit per column. For this field, the amount of
  ;;       storage required for N columns is INT((N+7)/8) bytes.
  ;;     * Variable-sized (for UPDATE_ROWS_LOG_EVENT only). Bit-field
  ;;       indicating whether each column is used in the
  ;;       UPDATE_ROWS_LOG_EVENT after-image; one bit per column. For
  ;;       this field, the amount of storage required for N columns is
  ;;       INT((N+7)/8) bytes.
  ;;     * Variable-sized. A sequence of zero or more rows. The end is
  ;;       determined by the size of the event. Each row has the
  ;;       following format:
  ;;           o Variable-sized. Bit-field indicating whether each
  ;;             field in the row is NULL. Only columns that are "used"
  ;;             according to the second field in the variable data part
  ;;             are listed here. If the second field in the variable
  ;;             data part has N one-bits, the amount of storage
  ;;             required for this field is INT((N+7)/8) bytes.
  ;;           o Variable-sized. The row-image, containing values of
  ;;             all table fields. This only lists table fields that are
  ;;             used (according to the second field of the variable
  ;;             data part) and non-NULL (according to the previous
  ;;             field). In other words, the number of values listed
  ;;             here is equal to the number of zero bits in the
  ;;             previous field (not counting padding bits in the last
  ;;             byte).
  ;;             The format of each value is described in the log_event_print_value() function in log_event.cc.
  ;;           o (for UPDATE_ROWS_EVENT only) the previous two fields are repeated, representing a second table row. 

  ;; For each row, the following is done:
  ;;     * For WRITE_ROWS_LOG_EVENT, the row described by the row-image is inserted.
  ;;     * For DELETE_ROWS_LOG_EVENT, a row matching the given row-image is deleted.
  ;;     * For UPDATE_ROWS_LOG_EVENT, a row matching the first row-image is removed, and the row described by the second row-image is inserted. 
  (let [b #^ByteBuffer (.position b (+ 19 (:offset e)))
        next-entry-offset (+ (:offset e) (:event-len e)) ;;(:next e)
        column-types (:column-types table-map-event)
        column-meta (:column-meta table-map-event)
        
        table-id (-> b (get-array 6) n-uint)
        reserved (.getShort b)
        column-count (get-packed-int b)
        used-cols (get-bitfield b column-count)
        ;; update has two rows per change one for `set' and one for `where'
        used-update-cols (when (= (:type e) 'UPDATE_ROWS_EVENT)
                           (get-bitfield b column-count))

        rows (loop [rows []]
               (if (< (.position b) next-entry-offset)
                 (recur (conj rows (if used-update-cols
                                     ;; UPDATE_ROWS_EVENT
                                     [(read-row b column-types column-meta used-cols)
                                      (read-row b column-types column-meta used-update-cols)]
                                     ;; WRITE_ROWS_EVENT and DELETE_ROWS_EVENT
                                     (read-row b column-types column-meta used-cols))))
                 rows))]
    (merge e {:table-id table-id
              ;; resolve table-name?
              :table-name (:table-name table-map-event)
              :db-name (:db-name table-map-event)
              :rows rows})))


;;; read all events

(defn all-events
  "Scan buf (ByteBuffer) and read all events starting at position 0
  in the byte buffer.
  Stop if the next event is outside the "
  ([buf] (all-events buf {:event-len 0 :offset 0}))
  ([#^ByteBuffer buf prev-evt]
     (lazy-seq (when-let [e (read-event-header buf (+ (:offset prev-evt) (:event-len prev-evt)))]
                 (cons e (all-events buf e))))))

;; (defn further-decode-event [b e]
;;   (case (:type e)
;;         FORMAT_DESCRIPTION_EVENT (read-v4-format-description b e)
;;         ROTATE_EVENT (read-rotate b e)
;;         QUERY_EVENT (read-query-log b e)
;;         XID_EVENT (read-xid b e)
;;         TABLE_MAP_EVENT (read-table-map b e)
;;         ;;[WRITE_ROWS_EVENT UPDATE_ROWS_EVENT DELETE_ROWS_EVENT] (read-wud-rows b e)
;;         e))

(defn with-open-binlog*
  "Try to open file-name and memory-map it to a ByteBuffer beginning at offset."
  ([file-name f] (with-open-binlog* file-name 0 f))
  ([file-name offset f]
     (when-not (binlog-file? file-name) (throwf "'%s' is not a mysql-binlog file!" file-name))
     (with-open [i (-> file-name File. FileInputStream.)
                 c (-> i .getChannel)]       
       (let [b (doto (.map c FileChannel$MapMode/READ_ONLY offset (- (.size c) offset))
                 (.order ByteOrder/LITTLE_ENDIAN))]
         (f b)))))

(defn read-binlog
  "Read the binary log, beginning at offset.
  Return: [event-vector, rotation-event]"
  ([fname] (read-binlog fname 4))
  ([fname start-offset] (read-binlog fname start-offset nil))
  ([fname start-offset table-map-evt]
     (println"with-open-binlog:" fname start-offset)
     (with-open-binlog* fname start-offset
       (fn [b]
         (loop [table-map-e table-map-evt
                [e & more :as es] (all-events b)
                decoded-events []]
           (cond (= (:type e) 'TABLE_MAP_EVENT)
                 (let [tm (read-table-map b e)]
                   (recur tm
                          more
                          (conj decoded-events tm)))
                 ('#{WRITE_ROWS_EVENT UPDATE_ROWS_EVENT DELETE_ROWS_EVENT} (:type e))
                 (recur table-map-e
                        more
                        (conj decoded-events
                              (if table-map-e
                                (read-wud-rows b e table-map-e)
                                (throwf "No TABLE_MAP event for read-wud-rows!"))))
                 (= (:type e) 'XID_EVENT)
                 (recur table-map-e
                        more
                        (conj decoded-events (read-xid b e)))
                 (= (:type e) 'QUERY_EVENT)
                 (recur table-map-e
                        more
                        (conj decoded-events (read-query b e)))
                 (nil? es)
                 (do ;; (println"read-binlog:" (count decoded-events) "events and NO rotation-event")
                   [decoded-events nil table-map-e])
                 (nil? e) ;; just filter
                 (do      ;; (println"event is nil!")
                   (recur table-map-e
                          more
                          decoded-events))
                 (= (:type e) 'ROTATE_EVENT) ;; stop when rotate event is read
                 (do ;; (println"read-binlog:" (count decoded-events) "events and rotation-event:" (:next-file (read-rotate b e)))
                   [decoded-events (read-rotate b e) table-map-e])
                 :else
                 (recur table-map-e
                        more
                        (conj decoded-events e))))))))

;; (def example-binlog-file "/home/timmy-turner/clj/tmp/binlog.000026")
;; (def rbr #(->> example-binlog-file read-binlog))

;; problem:
;; - jnotify will call a fn each time a given file is modified
;; - but I'd like to wait a second to let the server write
;;   more events in his binlog
;; - this translates to:
;;   "don't interrupt the already running modify-function"
;;   and maybe: "wait a few ms before running the modify-function
;;               again after it had finished"
(defn queuing-send-off-fn
  "Return a function F that wraps f. f should be a function to change
  the state of an agent.
  The returned function F, called with an agent will do: (send agent f)
  Successive calls to F, while f has not returned, will be queued up.
  After f returns, it will be called again if there were calls to F while
  f was still running."
  [f]
  (let [state (ref {:state nil :pending 0})]
    (def _xxx_state state)
    (fn [the-agent]
      ;; (println"queue function has been called:" state (:logname @the-agent) (Thread/currentThread))
      (try
       (let [run? (dosync (if (= :running (:state @state))
                            ;; -> count
                            (do (alter state update-in [:pending] inc)
                                false)
                            ;; -> run
                            (do (alter state assoc :pending 0 :state :running)
                                true)))]
         (when run? (send-off the-agent (fn this [s]
                                          ;; (println">>this f")
                                          (let [ret (f s)
                                                ;; _ (println"<<this f")
                                                run-again? (dosync (if (< 0 (:pending @state))
                                                                     (do (alter state assoc :pending 0)
                                                                         true)
                                                                     (do (alter state assoc :state nil :pending 0)
                                                                         false)))]
                                            ;; (println"queue f has finished:" run-again? state (:logname @the-agent))
                                            (when run-again? (send-off the-agent this))
                                            ret))))
         state)
       (catch Exception e (do ;; (println"Exception while executing queue-fn: " e)
                              (def _queue-fn-exception e)))))))


;;; the application, basically works but needs some cleanup here and there

(defn cdc-init [event-fn]
  "returns an agent containg the initial state"
  (agent {:watch-id nil
          :logname nil
          :offset nil
          :queue-fn nil ;; function which gets called on JNotify events
          :event-fn event-fn ;; function which is called with the event-data
          }))

(defn most-recent-binlog [binlog-index-file]
  ;; (most-recent-binlog "/var/log/mysql/binlog-files.index")
  ;; additionally: scan the most-recent binlog for an
  ;; rotate-event.
  (last (seq (.split #"\n" (slurp binlog-index-file)))))

;; (defn listen-for-modify
;;   "Add a modify watcher to file-name. Run f with the file-name on modification
;;   events."
;;   ;; note: INotify (linux) is able to coerce recent (unread) events into one event
;;   [file-name f]
;;   (println "cdc-add-watch" file-name (Thread/currentThread))
;;   (JNotify/addWatch
;;    file-name
;;    JNotify/FILE_MODIFIED
;;    false ;; watch-subtree
;;    (proxy [JNotifyListener] []
;;      (fileModified [watch-descriptor, path, name]
;;        (f)))))

(defn jnotify-swap-modify-watchers
  "Given the current watch-id and a new file and the action-function,
  add a modify-watcher to the new file and then remove the watcher
  from the old file, under the assumption that there will be no more
  write events to the old file.
  Watch-id may be ommited, in which case only a new file-watch is created.
  If only a watch-id is given, remove the watcher and return nil.
  Return the new watch-id."
  ;; this is necessary because for linux, removing one watch and
  ;; adding another watch for a file instantly will return the same
  ;; watch-id (the one the removed file had) and JNotify can't handle
  ;; this properly - if there is even a chance to handle this
  ;; situation coorect without breaking other JNotify uses.
  ([watch-id] (jnotify-swap-modify-watchers watch-id nil nil))
  ([new-filename action-f] (jnotify-swap-modify-watchers nil new-filename action-f))
  ([watch-id new-filename action-f]
     (let [new-watch-id (when (and new-filename action-f)
                          (JNotify/addWatch new-filename
                                            JNotify/FILE_MODIFIED
                                            false ;; watch subtree
                                            (proxy [JNotifyListener] []
                                              (fileModified [watch-descriptor, path, name]
                                                            (action-f)))))]
       (when watch-id (JNotify/removeWatch watch-id))
       new-watch-id)))

;; (defn cdc-add-watch-for-modification! ;; needs a better name
;;   "registers a modification listener using
;;   JNotify and the :queue-fn function."
;;   [state]
;;   (let [our-agent *agent*
;;         f (.submit (:jnotify-executor state)
;;                    (fn []
;;                      (send our-agent assoc
;;                            :watch-id
;;                            (listen-for-modify (:logname state) #((:queue-fn state) our-agent)))))]
;;     state))

;; (defn jnotify-remove-watch
;;   "remove jnotify watch watch-id, which may be nil"
;;   [state watch-id]
;;   (when (number? watch-id)
;;     ;; (println"removing watch" watch-id "from " (:logname state))
;;     (let [fut (.submit (:jnotify-executor state)
;;                        (fn []
;;                          (println "cdc-remove-watch" (:logname state) (Thread/currentThread))
;;                          (JNotify/removeWatch watch-id)))]
;;       ;; (println"remove-result:" (.get fut))
;;       ))
;;   state)

(defn cdc-swap-modify-watchers [{ln :logname af :queue-fn id :watch-id :as state}]
  (let [a *agent*]
    (assoc state :watch-id (jnotify-swap-modify-watchers id ln #(af a)))))

(defn cdc-log-rotation
  "Switch to a new log file"
  [state rotation-event]
  ;; (println"cdc-log-rotation:" (:logname state) (:next-file rotation-event))
  (def _cdc-log-rot [rotation-event state *agent*])
  ;; hook on new binlog
  ;; (send *agent* cdc-swap-modify-watchers)
  ;; trigger a first parse on the binlog, we might have missed some events
  (send *agent* cdc-swap-modify-watchers)
  (send *agent* (fn [s]
                  ;; (println "running queue function"
                  ((:queue-fn state) *agent*)
                  ;; )
                  s))
  (assoc state
    :offset 4
    :table-map nil
    :logname (str (File. (.getParentFile (File. (:logname state))) (:next-file rotation-event)))))

(defn cdc-turn
  "read the binlog from (state :offset) to end. Call (state :event-fn) with
  the resulting events."
  [state]
  (let [[events rotate-event table-map-e] (read-binlog (:logname state)
                                                       (:offset state)
                                                       (:table-map state))
        new-state (assoc state
                    :table-map table-map-e
                    :offset (or (:next (peek events)) (:offset state)))]
    (when rotate-event (send *agent* cdc-log-rotation rotate-event))
    (when-not (empty? events) ((:event-fn state) events))
    new-state))

(defn cdc-start
  "Sets up the cdc-mechanism to read events from the first binlog file found
  in index-file."
  [state index-file]
  (let [new-state {;; reset the offset
                   ;; ignore the binlog-magic at the beginning of every binlog file
                   :offset 4
                   :logname (most-recent-binlog index-file)
                   :queue-fn (queuing-send-off-fn cdc-turn)
                   ;; jnotify thread isolation
                   :jnotify-executor (java.util.concurrent.Executors/newSingleThreadExecutor)}]
    (when (= nil *agent*) (throwf "Must be called within an agent"))
    (send *agent* cdc-swap-modify-watchers)
    (merge state new-state)))

(defn cdc-stop [state]
  (when (:watch-id state) (jnotify-swap-modify-watchers (:watch-id state)))
  (assoc state :watch-id nil))




(comment
  ;;; example usage:
  ;; our queue, events are stored here
  (def *queue* (LinkedBlockingQueue. 10))
  ;; init state with an event-callback:
  (def *state* (cdc-init #(.put #^BlockingQueue %)))
  ;; start the cdc-mechanism
  (send-off *state* cdc-start "/var/log/mysql/binlog-files.index" )

  ;; pop sql-events off the *queue* 
  (.poll *queue* 200 TimeUnit/MILLISECONDS)
  
  ;; stop it
  (send *state* cdc-stop)
  
   
  ;; or read the binlog manually:
  (time
   (let [[events rot]
         (read-binlog
          ;; logfile
          "/var/log/mysql/binlog.000052"
          ;; offset
          ;;102771
          )]
     (count events)))
  )



;;; parsing strategy:
;; open the log, mmap it
;; scan for table-map events
;; group W/U/D-events to the table-events
;; alert on schema-update statements (create/alter/modify/destroy)

;; & listen for notification-events
;; rember positions in the logfile

;; testing:
;; create a setup
;; write tests: sql-query -> expected cdc-output
;; execute tests by connecting to mysql, issue the query and watch the binlog

;; how to identify broken events (events which are still written and are not fully written in the binlog yet)
;;   maybe by ignoring the last event?

;; idea for syncing db&cdc:
;; connect to the db, issue a statement, wait for the statement written to the binlog at offset -> that's then our starting point

;;;; what about using libdmysql??
;; together with chouser clojure-jna and libjna?
;;(use 'net.n01se.clojure-jna)
;;(jna-invoke Integer c/printf "My number: %d\n" 5)
;;(jna-invoke Integer mysqld/decimal_bin_size 10 10)
;;(com.sun.jna.Function/getFunction "mysql/mysqld" "printf")
;; in current mysql builds (as of 5.1.44) libmysqld is not built as a shared library!!! - nice try

;; (->> (read-binlog "/var/log/mysql/binlog.000102")
;;      second
;;      ;;(map :type) pprint
;;      (map #(str (:type %) " " (:sql %)))
;;      pprint)



