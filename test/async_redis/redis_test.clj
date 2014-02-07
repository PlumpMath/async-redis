(ns async-redis.redis-test
  (:refer-clojure :exclude [get type keys set sort eval])
  (:import (clojure.core.async.impl.channels ManyToManyChannel))
  (:require [clojure.test :refer :all]
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
           (is (instance? ManyToManyChannel (with-chan (fn [] 7))))))
