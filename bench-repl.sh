#!/bin/sh

rlwrap java -Xmx256m -server -cp /usr/share/java/mysql.jar:../criterium/src:lib/*:src/:classes/ clojure.main -e "(do (require 'cdc.mysql-bench) (in-ns 'cdc.mysql-bench))" -r
