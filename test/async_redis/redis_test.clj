(ns async-redis.redis-test
  (:import (clojure.core.async.impl.channels ManyToManyChannel))
  (:require [clojure.test :refer :all]
            [clojure.core.async :as async :refer [<!!]]
            [async-redis.redis :as r]))

(r/configure "127.0.0.1" 6379 :db 9)

(deftest test-wrap
  (testing "wrapping nil doesn't return nil"
           (is (= false (r/wrap nil))))
  (testing "wrapping non-nil is identity"
           (is (= true (r/wrap true))))
  (testing "wrapping less obvious non-nil is also identity"
           (is (= 77 (r/wrap 77)))))

(deftest test-with-chan
  (testing "with-chan returns a channel"
           (is (instance? ManyToManyChannel (r/with-chan nil (fn [] 7)))))
  (testing "simple with-chan"
           (is (= 8 (<!! (r/with-chan nil (fn [] 8)))))))

(defn random-string [length]
  (apply str (take length (repeatedly #(rand-nth "abcdefghijklmnopqrstuvwxyz")))))

(deftest the-basics
  (r/just (r/flush-db!))

  (let [key "watevah"
        val (random-string 20)]

    (testing "control for exists" (is (= false (<!! (r/exists? val)))))

    (testing "control that our key isn't used yet"
             (if (<!! (r/exists? key)) (r/just (r/del! key)))

             (is (= false (<!! (r/exists? key)))))

    (r/just (r/set! key val))
    (testing "double-check exists" (is (= true (<!! (r/exists? key)))))
    (testing "set round-trip" (is (= val (<!! (r/get key)))))
    (testing "type is as expected" (is (= "string" (<!! (r/type key)))))

    (testing "deletion"
             (r/just (r/del! key))
             (is (= false (<!! (r/exists? val)))))
    ))

(deftest the-keys
  (testing "selected the right DB" (is (= 9 (<!! (r/db)))))
  (r/just (r/flush-db!))

  (testing "no keys after flush, control" (is (= '() (<!! (r/keys "*")))))

  (r/just (r/set! "a-key" "abc"))
  (testing "now we have one key" (is (= '("a-key") (<!! (r/keys "*")))))
  (testing "random-key should give me that one" (is (= "a-key" (<!! (r/random-key)))))

  (r/just (r/rename! "a-key" "another-key"))
  (testing "renamed it" (is (= "another-key" (<!! (r/random-key)))))
  (testing "and the original is gone" (is (= '("another-key") (<!! (r/keys "*")))))
  (testing "value has swapped properly too" (is (= "abc" (<!! (r/get "another-key")))))

  (r/just (r/set! "a-key" "def"))
  (testing "now I have both" (is (= (sort '("another-key" "a-key"))
                                    (sort (<!! (r/keys "*"))))))
  (testing "paranoia" (is (= "def" (<!! (r/get "a-key")))))

  (testing "returns 0" (is (= 0 (<!! (r/renamenx! "a-key" "another-key")))))
  (testing "the rename should have failed" (is (= "abc" (<!! (r/get "another-key")))))
  )

(deftest expiration
  (r/just (r/flush-db!))
  (let [key (random-string 20)]
    (testing "control" (is (= false (<!! (r/get key)))))

    (testing "that there is no TTL for an empty key" (is (< (<!! (r/ttl key)) 0)))
    (r/just (r/set! key "a value"))

    (testing "just checking" (is (= "a value" (<!! (r/get key)))))
    (testing "that there is no TTL for an new key" (is (< (<!! (r/ttl key)) 0)))

    (r/just (r/expire! key 1))
    (testing "that there is a TTL now" (is (not (= -2 (<!! (r/ttl key)))))))
  )

