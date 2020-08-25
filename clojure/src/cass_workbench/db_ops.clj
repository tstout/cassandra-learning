(ns cass-workbench.db-ops
  (:require
   [clojure.core.async :refer [<!! chan to-chan pipeline-blocking] :as a]
   [cass-workbench.config :refer [session]]
   [qbits.alia :as alia]
   [qbits.alia.async :as alia_a]))

(defn rand-str
  "Generate a random string for faux data purposes."
  [len]
  (apply str (take len (repeatedly #(char (+ (rand 26) 65))))))

(defn uuid []
  (java.util.UUID/randomUUID))

(def insert-stmt
  (alia/prepare session
                "insert into simple_records.message_status_by_request 
                 (requestId, messageId, lastUpdatedDateTime, status, deliveryResponse)
                 values
                 (?, ?, dateOf(now()), ?, ?)"))

(def select-stmt
  (alia/prepare session
                "select * from simple_records.message_status_by_request where requestid = ?"))

(defn write-msg-row [m]
  (let [{:keys [request-id message-id status delivery-response]} m]
    (alia/execute session insert-stmt {:values [request-id message-id status delivery-response]})))

(defn next-state []
  (let [states ["PENDING" "DISPATCHED" "COMPLETE" "ERROR"]
        coll (cycle states)
        n (atom -1)]
    (fn []
      (swap! n inc)
      (nth coll @n))))

(defn status-counter [prev row]
  (let [{:keys [status]} row]
    ;;(prn (str "Counter fn invoked " row))
    (case status
      "COMPLETE" (update prev :complete inc)
      "ERROR" (update prev :error inc)
      prev)))

;; Note - could not get this to work - revist later.
(defn fetch-async [request-id f]
  (->>
   (alia_a/execute-chan-buffered
    session
    select-stmt
    {:values [request-id]})
   (a/reduce f {:error 0 :complete 0})))

(defn fetch-status
  "Stream the potentially large cassandra result set, while counting the error and complete
   states of each row's status. This is an experiment to determine the viability of potentially
   streaming millions of records at a time and performing some type of aggregate operation
   on the stream. Note that this does NOT read the entire resultset into memory."
  [request-id]
  (let [ch (alia_a/execute-chan-buffered
            session
            select-stmt
            {:values [request-id]})]
    (loop [counters {:error 0 :complete 0}]
      (if-let [row (<!! ch)]
        (recur (status-counter counters row))
        counters))))

(defn populate-db
  "Generate num-rows messages for a simulated request.
   Returns the new required-id. 
   Note: this is really slow, revisit to improve performance."
  ([num-rows]
   (let [request-id (uuid)
         status (next-state)]
     (populate-db num-rows request-id status)))
  
  ([num-rows request-id status]
   ;;(prn (str "UUID is " request-id))
   (dotimes [_ num-rows]
      ;;(prn (str "inserting..." n))
     (write-msg-row
      {:request-id request-id
       :message-id (uuid)
       :status (status)
       :delivery-response (rand-str 128)}))
   request-id))

(defn blocking-db-operation [arg]
  (let [[request-id status] arg]
    (populate-db 1 request-id status)))
  
(defn populate-db-p 
  "Populate rows in parallel. Parallel processing is needed to insert million(s) of rows
   in a reasonable amount of time."
  [num-rows]
  (let [request-id (uuid)
        input-coll (repeat num-rows [request-id (next-state)])
        output-chan (chan)]
    (pipeline-blocking 10  ;; concurrency level
                       output-chan
                       (map blocking-db-operation)
                       (to-chan input-coll))
    (loop []
      (if-let [_ (<!! output-chan)]
        (recur)
        request-id))))

;; (let [concurrent 10
;;       output-chan (chan)
;;       input-coll (range 0 10)]
;;   (pipeline-blocking concurrent
;;                      output-chan
;;                      (map blocking-operation)
;;                      (to-chan input-coll))
;;   (<!! (a/into [] output-chan)))

;; (let [concurrent 10
;;       output-chan (chan)
;;       input-coll (range 0 10)]
;;   (pipeline-blocking concurrent
;;                      output-chan
;;                      (map blocking-operation)
;;                      (to-chan input-coll))
;;   (<!! output-chan))


(comment

  
  (populate-db-p 1000000)
  
  (repeat 2 [(uuid) (next-state)])
  (reduce (fn [_ c] c) [1 2 3])
  
  (populate-db 1 (uuid) (next-state))
  
  
  (str (Thread/currentThread))

  (populate-db 1000000)

  ;;10^6
  (time
   (fetch-status #uuid "e6b8d568-85cd-46b8-95ff-8c791b391efd"))

  ;;10
  (fetch-status #uuid "cc280855-c55b-45e2-85d3-a42c995596fd")

  ;; 24000
  (fetch-status #uuid "ca394e46-84ab-4511-bc8f-a36a5c7b44e9")

  ;;10000
  (fetch-status #uuid "31a62a6f-b516-4627-9a11-c5ff9e8177ca")

  ;;100000
  (fetch-status #uuid "4a2b94d4-d4b8-493e-b858-926546e90827")

  ;;200000
  (fetch-status #uuid "8b1184ca-b2fe-4a91-9e67-afa386adcf52")

  (time
   (fetch-status #uuid "8b1184ca-b2fe-4a91-9e67-afa386adcf52"))


  (populate-db 200000)

  *e
  ;;
  )

