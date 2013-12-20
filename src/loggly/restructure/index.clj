(ns loggly.restructure.index
  (:require [clojure.data.priority-map :refer [priority-map]]
            [loggly.restructure.util :refer [map-of]])
  (:import [clojure.lang ExceptionInfo]
           [java.util.concurrent CountDownLatch
                                 LinkedBlockingQueue
                                 TimeUnit]
           [java.util PriorityQueue]
           [com.loggly.indexmanager.db.dao AssignmentDAO IndexDAO]))

(defn get-imdb-index [index-name]
  (IndexDAO/queryByName index-name))

(def ind-names ["000101.0000.cid288.a98499" "000101.0000.cid289.d37631" "000101.0000.cid290.ac2fe4" "131118.2340.cid230.585d2d" "131119.0050.cid230.3e29ef" "131119.0204.cid230.175ad8" "131119.0250.cid230.8eaa74" "131119.0350.cid230.7a6798" "131119.0350.cid230.d51ee8" "131119.0502.cid230.311560" "131119.0616.cid230.8f356d" "131126.1404.shared.574072-d" "131130.1431.shared.3e7529-d" "131201.0959.shared.e9ef92-d" "131205.1800.shared.87c6f8" "131206.0322.cid230.85972e" "131208.1059.shared.d1a264" "131209.1100.shared.dc9607" "131210.0600.cid288.3a311d" "131210.0606.cid289.0f7faa" "131210.1101.shared.f27bbb" "131211.0450.shared.9dd071-r.0" "131211.0450.shared.9dd071-r.1" "131211.0450.shared.9dd071-r.2" "131211.0450.shared.9dd071-r.3" "131217.1817.cid230.66dfa6" "131217.1817.cid230.c0cdeb" "131217.1852.shared.6a4d63" "131218.1128.cid290.104de1" "131218.1953.cid230.e144cf" "131218.2035.shared.1e5e6d" "131219.0159.cid230.88f66d" "131219.0835.cid230.d7b07b" "131219.1501.cid230.f5ae88" "131219.2036.shared.9ff0e4" "131219.2109.cid230.3c05d8" "barindex" "fooindex"])

(def first-ind (first (filter identity (map get-imdb-index ind-names))))
(def assn (first (get-imdb-assignments [first-ind])))

(identity assn)

(defn get-index-assigns [index]
  (for [assn (AssignmentDAO/queryByIndex (.iid index))]
    {:cid (.cid assn)
     :expiry (.expireTime assn)}))

(defn get-imdb-assignments [imdb-indexes]
  (mapcat #(AssignmentDAO/queryByIndex (.iid %)) imdb-indexes))

(defn expired-assn? [assn]
  ; XXX
  false)

(defn index-for-assn [assn]
  "someind")

(defn cust-for-assn [assn]
  35)

(defn get-cust-sets [assns]
  (let [inds (set (map index-for-assn assns))]
    (into {}
      (for [ind inds]
        [ind (->> assns
               (filter #(= ind (index-for-assn %)))
               (map cust-for-assn))]))))

(defn get-cust-routes [assns target-count])

(defmacro if-first [[v s] iform eform]
  `(if-let [s# (seq ~s)]
     (let [~v (first s#)]
       ~iform)
     ~eform))

(defn bytes-for-assn [assn]
  ; XXX
  2344556)

(defn expiry-for-assn [assn]
  ; XXX
  134656609)

(defn merge-customers [imdb-assigns]
  (let [cids (set (map cust-for-assn imdb-assigns))]
    (for [cid cids]
      (let [cust-assns (filter #(= cid (cust-for-assn %)) imdb-assigns)
            byte-count (->>
                         cust-assns
                         (map bytes-for-assn)
                         (reduce +))
            max-expire (->>
                         cust-assns
                         (map expiry-for-assn)
                         (reduce max))]
        (map-of cid byte-count max-expire)))))

(defn take-n-bytes [custs byte-limit]
  (loop [rem-custs custs
         bytes-taken 0
         taken-custs []]
    (if-first [cust rem-custs]
      (if (> (+ bytes-taken (/ (:byte-count cust) 2))
             byte-limit)
        [taken-custs bytes-taken rem-custs]
        (recur
          (rest rem-custs)
          (+ bytes-taken (:byte-count cust))
          (conj taken-custs cust)))
      [taken-custs bytes-taken rem-custs])))

(defn make-partitions [custs target-count]
  (loop [rem-custs (sort-by :max-expire custs)
         partitions-left target-count
         partitions-created []
         bytes-remaining (->> custs
                              (map :byte-count)
                              (reduce +))]
    (if (zero? partitions-left)
      (if (empty? rem-custs)
        partitions-created
        (throw (Exception. "wtf")))
      (let [byte-limit (/ bytes-remaining partitions-left)
            [new-part bytes-still-rem still-rem-custs]
              (take-n-bytes rem-custs byte-limit)]
        (recur
          still-rem-custs
          (dec partitions-left)
          (conj partitions-created new-part)
          bytes-still-rem)))
    ))

(defn get-retention [cid]
  ;XXX
  14)

(defn add-retentions [assns]
  (for [assn assns]
    (assoc assn
      :retention (get-retention (:cid assn)))))

(defn compare-by [ks]
  (fn [x y]
    (or (->>
          ks
          (map (fn [k] (compare (k x) (k y))))
          (remove #(= % 0))
          first)
        0)))

(defn get-src-assns [index-names]
  (->> index-names
       (map get-imdb-index)
       get-imdb-assignments
       merge-customers
       add-retentions
       (sort (compare-by [:retention :stats-event-count]))))

(defn build-dst-assns [source-index-names target-count]
  (let [src-assns (get-src-assns source-index-names)]
    (loop [assns src-assns
           dst-ixs (into (priority-map) (map vector
                                          (range target-count)
                                          (repeat 0)))
           dst-assns {}]
      (let [assn (first assns)
            dst-ix (peek dst-ixs)
            dst-ix-id (first dst-ix)
            dst-ix-count (second dst-ix)]
        (if (empty? assns)
            dst-assns
            (recur (rest assns)
                   (assoc dst-ixs dst-ix-id
                     (+ dst-ix-count (:stats-event-count assn)))
                   (assoc dst-assns (:cid assn) dst-ix-id)))))))

; order by (retention, stats_event_count) descending
(defn get-splitter-policy [{:keys [source-index-names target-count] :as opts}]
  (build-dst-assns source-index-names target-count))
