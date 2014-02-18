(ns async-redis.redis-test
  (:refer-clojure :exclude [get type keys set sort eval])
  (:import (clojure.core.async.impl.channels ManyToManyChannel))
  (:require [clojure.test :refer :all]
            [clojure.core.async :as async :refer [<!!]]
            [async-redis.redis :refer :all]))

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

(deftest the-basics
  (let [client (connect nil nil)
        key "watevah"
        val (apply str (take 20 (repeatedly #(rand-nth "abcdefghijklmnopqrstuvwxyz"))))]

    ;(select client 9)

    (testing "control for exists" (is (= false (<!! (exists client val)))))

    (testing "control that our key isn't used yet"
             (if (<!! (exists client key)) (<!! (del client key)))

             (is (= false (<!! (exists client key)))))

    (<!! (set client key val)) ;; doing the blocking read so that connection is clear for next op.
    (testing "double-check exists" (is (= true (<!! (exists client key)))))
    (testing "set round-trip" (is (= val (<!! (get client key)))))
    (testing "type is as expected" (is (= "string" (<!! (type client key)))))

    (testing "deletion"
             (<!! (del client key))
             (is (= false (<!! (exists client val)))))
    ))
