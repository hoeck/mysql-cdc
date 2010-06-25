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

(ns cdc.jdbc.CDCDriver
  (:require [cdc.jdbc :as jdbc])
  (:gen-class :implements [java.sql.Driver]))

(try (java.sql.DriverManager/registerDriver
      (.newInstance (Class/forName "cdc.jdbc.CDCDriver")))
     (catch Exception e nil))

(defn extract-url-param [url-string]
  (second (re-matches #"jdbc:mysql-cdc:(.*)"
                      url-string)))

;; URL: jdbc:mysql-cdc:*
  
(defn -acceptsURL [t url]
  (boolean (re-matches #"jdbc:mysql-cdc:.*" url)))

(defn -getMajorVersion [t] 0)
(defn -getMinorVersion [t] 1)

(defn -getPropertyInfo [t url info]
  ;; there should be only one connection object
  ;; or, the first connection object should setup the cdc-process
  ;; and the last connection should shut it down
  (into-array java.sql.DriverPropertyInfo
              ;; [(driver-property-info :name "user" :description "MySQL username" :required true :value "root")
              ;;  (driver-property-info :name "password" :description "MySQL password" :required true :value "root")]
              []))

(defn -jdbcCompliant [t] false)

(defn -connect [t url prop]
  (dosync (if-let [c (:connection @jdbc/jdbc-state)]
            c
            (let [c (jdbc/create-connection (extract-url-param url) nil nil)]
              (alter jdbc/jdbc-state assoc :connection c)
              c))))

