(ns loggly.restructure-tests
  (:use [clojure test]
        [loggly.restructure indexing splitter]))

(defn sleep [] (Thread/sleep 10))

(deftest pool-test
  (let [done (atom false)
        collector (atom #{})
        pool (start-index-worker-pool
               {:num-index-workers 3
                :bulks-queued 20
                :done-notifier #(reset! done true)
                :sink #(swap! collector conj %)
                :pool-name "test-pool"})]

    (dotimes [i 20]
      (pool i))

    (sleep)
    (is (= @collector (set (range 20))))  
    (is (not @done))

    (pool :stop)
    (sleep)
    (is @done)))

(defn setup-indexer []
  (let [stop-signalled (atom false)
        finished (atom false)
        collector (atom [])
        indexer (start-indexer
                  {:signal-stop #(reset! stop-signalled true)
                   :bulk-sink #(swap! collector conj %)
                   :process-name "indexer"
                   :finish #(reset! finished true)
                   :batch-size 3
                   :indexer-events-queued 20
                   :index-limit 5})]
    {:stop-signalled stop-signalled
     :collector collector
     :indexer indexer
     :finished finished}))

(deftest indexer-test
  (let [{:keys [stop-signalled collector
                indexer finished]} (setup-indexer)]
    (dotimes [i 5]
      (indexer i))
    (sleep)
    (is (not @stop-signalled))
    (doseq [i (range 5 10)]
      (indexer i))
    (sleep)
    (is @stop-signalled)
    (is (not @finished))

    (indexer :stop)
    (sleep)
    (is @finished)
    (is (= @collector
           [[0 1 2]
            [3 4 5]
            [6 7 8]
            [9]]))))

(deftest empty-bulk-test
  (let [{:keys [stop-signalled collector
                indexer finished]} (setup-indexer)]
    (indexer :stop)
    (sleep)
    (is @finished)
    (is (= @collector []))))

(defn setup-splitter [continue-fn]
  (let [last-index (atom nil)
        policy {:foo 0 :bar 1 :baz 2}
        collector (atom {})
        finished (atom false)
        indexers (for [i (range 3)]
                   (fn [x]
                     (swap! collector
                            update-in [i]
                            (fnil conj []) x)))
        splitter (start-splitter
                   {:splitter-events-queued 50
                    :transformer identity
                    :policy policy
                    :indexers indexers
                    :continue? continue-fn
                    :finish (fn [ind]
                              (reset! finished true)
                              (reset! last-index ind))})]
    {:last-index last-index
     :collector collector
     :finished finished
     :splitter splitter}))

(deftest splitter-test
  (let [{:keys [last-index
                collector
                finished
                splitter]} (setup-splitter (constantly true))]
    (splitter :foo)
    (splitter :bar)
    (sleep)
    (is (= @collector
           {0 [:foo] 1 [:bar]}))
    (is (not @finished))
    (splitter :end-index)
    (splitter "hello world")
    (splitter :baz)
    (splitter :stop)
    (sleep)
    (is @finished)
    (is (= @collector {0 [:foo :stop]
                       1 [:bar :stop]
                       2 [:baz :stop]}))
    (is (= @last-index :all))))

(deftest splitter-check-stop-on-index
  (let [{:keys [last-index
                collector
                finished
                splitter]} (setup-splitter (constantly false))]
    (splitter :foo)
    (splitter :bar)
    (splitter :end-index)
    (splitter "hello world")
    (splitter :baz)
    (sleep)
    (is @finished)
    (is (= @collector {0 [:foo :stop] 1 [:bar :stop] 2 [:stop]}))
    (is (= @last-index "hello world"))))


(comment

(run-tests))
