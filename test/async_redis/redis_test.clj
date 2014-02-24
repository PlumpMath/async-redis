(ns async-redis.redis-test
  (:refer-clojure :rename {sort core-sort}
                  :exclude [get type keys set eval])
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

          (just (set key val))
          (testing "double-check exists" (is (= true (<!! (exists key)))))
          (testing "set round-trip" (is (= val (<!! (get key)))))
          (testing "type is as expected" (is (= "string" (<!! (type key)))))

          (testing "deletion"
                   (just (del key))
                   (is (= false (<!! (exists val)))))
          )))

(deftest the-keys
  (with (connect nil nil)
        (just (select 9))
        (just (flush-db))
        (testing "no keys after flush, control" (is (= '() (<!! (keys "*")))))

        (just (set "a-key" "abc"))
        (testing "now we have one key" (is (= '("a-key") (<!! (keys "*")))))
        (testing "random-key should give me that one" (is (= "a-key" (<!! (random-key)))))

        (just (rename "a-key" "another-key"))
        (testing "renamed it" (is (= "another-key" (<!! (random-key)))))
        (testing "and the original is gone" (is (= '("another-key") (<!! (keys "*")))))
        (testing "value has swapped properly too" (is (= "abc" (<!! (get "another-key")))))

        (just (set "a-key" "def"))
        (testing "now I have both" (is (= (core-sort '("another-key" "a-key"))
                                          (core-sort (<!! (keys "*"))))))
        (testing "paranoia" (is (= "def" (<!! (get "a-key")))))

        (testing "returns 0" (is (= 0 (<!! (renamenx "a-key" "another-key")))))
        (testing "the rename should have failed" (is (= "abc" (<!! (get "another-key")))))
        ))

(deftest test-on-client
  (with (connect nil nil)
        (let [key (random-string 20)
              val (random-string 20)]
          (<!! (set key val))
          (testing "round-trip" (is (= val (<!! (get key))))))))
