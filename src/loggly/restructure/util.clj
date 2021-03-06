(ns loggly.restructure.util
  (:require [loggly.restructure.log :refer :all])
  (:import [java.util.concurrent LinkedBlockingQueue]))

(deflogger logger)

(defn make-url [hostname index-name]
  (format "http://%s:9200/%s" hostname index-name))

(def refreshes (atom []))
(def perm-refreshes (atom []))

(defn replace!
  "Sets a to new-val while returning the very last value a held
  before resetting."
  [a new-val]
  (loop []
    (let [old-val @a]
      (if (compare-and-set! a old-val new-val)
        old-val
        (recur)))))

(defn refresh! []
  (let [refreshes-snap (replace! refreshes [])]
    (doseq [f refreshes-snap]
      (f)))
  (Thread/sleep 200)
  (doseq [f @perm-refreshes]
    (f))
  nil)

(defn register-refresh [f]
  (swap! refreshes conj f))

(defn register-perm-refresh [f]
  (swap! perm-refreshes conj f))

(defn in-thread* [thread-name f daemon]
  (let [t (Thread. f thread-name)]
    (.setDaemon t daemon)
    (.setUncaughtExceptionHandler t
      (reify Thread$UncaughtExceptionHandler
        (uncaughtException [_ t e]
          (when-not (or (instance? ThreadDeath e)
                        (instance? InterruptedException e))
            (fatal
              logger
              (str "thread " t " died unexpectedly")
              e)))))
    (register-refresh #(.stop t))
    (.start t)
    nil))

(defmacro in-daemon [thread-name & forms]
  `(in-thread*
     ~thread-name
     (fn [] ~@forms)
     true))

(defmacro in-thread [thread-name & forms]
  `(in-thread*
     ~thread-name
     (fn [] ~@forms)
     false))

(defn ^LinkedBlockingQueue get-queue [capacity q-name]
  (let [q (LinkedBlockingQueue. capacity)]
    (in-daemon (str q-name "-qmonitor")
      (while true
        (Thread/sleep 1000)
        (let [cap (.remainingCapacity q)]
          (when (zero? cap)
            (debug logger (str "queue " q-name " is full")))
          (when (= cap capacity)
            (debug logger (str "queue " q-name " is empty"))))))
    q))

(defn resetting-atom [i]
  (let [a (atom i)]
    (register-perm-refresh #(reset! a i))
    a))

(defn parse-int [s]
  (Integer. s))

(defmacro map-of [& names]
  (into {}
    (for [n names]
      [(keyword n) n])))
