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

(defn random-string [length]
  (apply str (take length (repeatedly #(rand-nth "abcdefghijklmnopqrstuvwxyz")))))

(deftest the-basics
  (with (connect nil nil)
        (let [key "watevah"
              val (random-string 20)]

          (just (select 9))

          (testing "control for exists" (is (= false (<!! (exists val)))))

          (testing "control that our key isn't used yet"
                   (if (<!! (exists key)) (just (del key)))

                   (is (= false (<!! (exists key)))))

          (<!! (set key val)) ;; doing the blocking read so that connection is clear for next op.
          (testing "double-check exists" (is (= true (<!! (exists key)))))
          (testing "set round-trip" (is (= val (<!! (get key)))))
          (testing "type is as expected" (is (= "string" (<!! (type key)))))

          (testing "deletion"
                   (just (del key))
                   (is (= false (<!! (exists val)))))
          )))

(deftest test-on-client
  (with (connect nil nil)
        (let [key (random-string 20)
              val (random-string 20)]
          (<!! (set key val))
          (testing "round-trip" (is (= val (<!! (get key))))))))
