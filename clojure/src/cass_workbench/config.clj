(ns cass-workbench.config
  (:require [qbits.alia :as alia]))

;; Note this will fail if cassandra server is not running
;; locally consider using a delay to prevent this.

(def cluster (alia/cluster {:contact-points ["localhost"]}))

(def session (alia/connect cluster))


(comment 
  session
  ;;
  )




