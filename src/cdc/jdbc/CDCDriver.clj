
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

