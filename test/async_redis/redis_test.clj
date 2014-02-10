(ns async-redis.redis-test
  (:refer-clojure :exclude [get type keys set sort eval])
  (:import (clojure.core.async.impl.channels ManyToManyChannel))
  (:require [clojure.test :refer :all]
            [clojure.core.async :as async :refer [<!!]]
            [async-redis.redis :refer :all]))

(deftest can-connect
  (testing "can connect to redis"
           (is (not (= nil (connect nil nil))))))

(deftest test-wrap
  (testing "wrapping nil doesn't return nil"
           (is (= false (wrap nil))))
  (testing "wrapping non-nil is identity"
           (is (= true (wrap true))))
  (testing "wrapping less obvious non-nil is also identity"
           (is (= 77 (wrap 77)))))

(deftest test-with-chan
  (testing "with-chan returns a channel"
           (is (instance? ManyToManyChannel (with-chan (fn [] 7)))))
  (testing "simple with-chan"
           (is (= 8 (<!! (with-chan (fn [] 8)))))))

(deftest simple-roundtrip
  (let [local-client (connect nil nil)
        key "watevah"
        val (apply str (flatten (take 20 (repeatedly #(rand-nth "abcdefghijklmnopqrstuvwxyz")))))]
    (is (= false (<!! (exists local-client val)))) ;; random-enough key, as control
    (if (<!! (exists local-client key))
      (<!! (del local-client key)))
    (is (= false (<!! (exists local-client key))))
    (<!! (set local-client key val)) ;; doing the blocking read so that connection is clear for next op.
    (is (= true (<!! (exists local-client key))))
    (is (= val (<!! (get local-client key))))
    (<!! (del local-client key))
    ))
