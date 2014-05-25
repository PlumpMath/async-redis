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

(deftest db-override
  (testing "control for db-override" (is (= 9 (<!! (r/db)))))
  (testing "selected the right DB" (is (= 2 (r/on-db 2 (<!! (r/db)))))))

(deftest expiration
  (r/just (r/flush-db!))
  (let [key (random-string 20)
        key2 (random-string 20)]
    (testing "control" (is (= false (<!! (r/get key)))))

    (testing "that there is no TTL for an empty key" (is (< (<!! (r/ttl key)) 0)))
    (r/just (r/set! key "a value"))

    (testing "just checking" (is (= "a value" (<!! (r/get key)))))
    (testing "that there is no TTL for an new key" (is (< (<!! (r/ttl key)) 0)))

    (r/just (r/expire! key 5))
    (testing "that there is a TTL now" (is (> (<!! (r/ttl key)) 0)))

    (r/just (r/setex! key2 20 "1"))
    (testing "setex! is a shortcut" (is (> (<!! (r/ttl key2)) 0)))

    (let [time (+ 30 (int (/ (System/currentTimeMillis) 1000)))]
      (r/just (r/expire-at! key time))
      (let [seconds-from-now (<!! (r/ttl key))]
        (testing "expiration time goes through" (is (and (> seconds-from-now 5) (<= seconds-from-now 30))))))
    )
  )

(deftest move
  (let [key (random-string 20)]
    (r/just (r/set! key "hi"))
    (testing "set key in default DB" (is (= "hi" (<!! (r/get key)))))
    (r/just (r/move! key 7))
    (testing "key isn't in default DB anymore" (is (= false (<!! (r/get key)))))
    (testing "and key IS in the new DB now" (is (= "hi" (<!! (r/on-db 7 (r/get key))))))
    )
  )

(deftest getset
  (let [key (random-string 20)
        val1 (random-string 20)
        val2 (random-string 20)]
    (r/just (r/set! key val1))
    (testing "control" (is (= val1 (<!! (r/get key)))))
    (testing "getset returns right" (is (= val1 (<!! (r/getset! key val2)))))
    (testing "and getset wrote" (is (= val2 (<!! (r/get key)))))))

(deftest mget
  (let [keys (take 5 (repeatedly #(random-string 20)))
        vals (take 5 (repeatedly #(random-string 20)))]
    (doall (map (fn [k v] (r/just (r/set! k v))) keys vals))
    (testing "mget" (is (= vals (<!! (apply r/mget keys)))))
    ))

(deftest setnx
  (let [key (random-string 20)
        val1 (random-string 20)
        val2 (random-string 20)]
    (r/just (r/setnx! key val1))
    (testing "setnx first time should work" (is (= val1 (<!! (r/get key)))))
    (r/just (r/setnx! key val2))
    (testing "setnx should no-op second time" (is (= val1 (<!! (r/get key)))))
    ))

(deftest mset
  (let [keys (take 100 (repeatedly #(random-string 20)))
        vals (take 100 (repeatedly #(random-string 20)))]
    (r/just (apply r/mset! (interleave keys vals)))
    (testing "mset set them all" (is (= vals (<!! (apply r/mget keys)))))))

(deftest msetnx
  (let [keys (take 100 (repeatedly #(random-string 20)))
        vals (take 100 (repeatedly #(random-string 20)))
        vals2 (take 100 (repeatedly #(random-string 20)))]
    (r/just (apply r/msetnx! (interleave keys vals)))
    (testing "msetnx set them all the first time" (is (= vals (<!! (apply r/mget keys)))))
    (r/just (apply r/msetnx! (interleave keys vals2)))
    (testing "msetnx did not set them all afterward" (is (= vals (<!! (apply r/mget keys)))))
    ))

(deftest numbers
  (let [key (random-string 20)]
    (r/just (r/set! key "1"))
    (testing "control" (is (= "1" (<!! (r/get key)))))

    (testing "base incr" (is (= 2 (<!! (r/incr! key)))))
    (testing "base incr 2" (is (= 3 (<!! (r/incr! key)))))
    (testing "base incr doublecheck" (is (= "3" (<!! (r/get key)))))

    (testing "incr-by" (is (= 5 (<!! (r/incr-by! key 2)))))
    (testing "incr-by doublecheck" (is (= "5" (<!! (r/get key)))))
    (testing "incr-by doublecheck 2" (is (= 5 (<!! (r/get-int key)))))

    (testing "decr" (is (= 4 (<!! (r/decr! key)))))
    (testing "decr-by" (is (= 1 (<!! (r/decr-by! key 3)))))
    (testing "doublecheck" (is (= 1 (<!! (r/get-int key)))))
    )
  )

(deftest strings
  (let [key (random-string 20)
        value1 (random-string 10)
        value2 (random-string 10)]
    (r/just (r/set! key value1))
    (testing "control" (is (= value1 (<!! (r/get key)))))
    (r/just (r/append! key value2))
    (testing "appended" (is (= (str value1 value2) (<!! (r/get key)))))
    (testing "substr start" (is (= value1 (<!! (r/substr key 0 (- (count value1) 1))))))
    (testing "substr end" (is (= value2 (<!! (r/substr key (count value1) (+ (count value1) (count value2)))))))
    ))

(deftest hashes
  (let [key (random-string 20)
        hkey (random-string 10)
        hval (random-string 10)
        hkey2 (random-string 10)
        hval2 (random-string 10)]

    (r/just (r/hset! key hkey hval))
    (testing "hset/get" (is (= hval (<!! (r/hget key hkey)))))

    (r/just (r/hsetnx! key hkey2 hval2))
    (testing "hsetnx 1" (is (= hval2 (<!! (r/hget key hkey2)))))
    (r/just (r/hsetnx! key hkey2 (random-string 10)))
    (testing "hsetnx 2" (is (= hval2 (<!! (r/hget key hkey2)))))
    )

  (let [key (random-string 20)
        hash {"a" "foo"
              "b" "bing"
              "c" "bang"}]
    (r/just (r/hmset! key hash))
    (testing "hmset a" (is (= (get hash "a") (<!! (r/hget key "a")))))
    (testing "hmset b" (is (= (get hash "b") (<!! (r/hget key "b")))))
    (testing "hmset c" (is (= (get hash "c") (<!! (r/hget key "c")))))
    (testing "hgetall" (is (= hash (<!! (r/hgetall key)))))
    (testing "hkeys" (is (= (sort (seq (keys hash))) (sort (seq (<!! (r/hkeys key)))))))
    (testing "hmget" (is (= (list "bing" "foo") (sort (seq (<!! (r/hmget key "a" "b")))))))

    (testing "hexists 1" (is (= true (<!! (r/hexists? key "a")))))
    (testing "hlen 1" (is (= 3 (<!! (r/hlen key)))))

    (r/just (r/hdel! key "a"))
    (testing "hexists 2" (is (= false (<!! (r/hexists? key "a")))))
    (testing "hexists 3" (is (= true (<!! (r/hexists? key "b")))))
    (testing "hdel" (is (= {"b" "bing" "c" "bang"} (<!! (r/hgetall key)))))
    (testing "hlen 2" (is (= 2 (<!! (r/hlen key)))))
    )

  (let [key (random-string 20)
        hkey (random-string 10)]
    (r/just (r/hset! key hkey "1"))
    (testing "hincr-by" (is (= 2 (<!! (r/hincr-by! key hkey 1)))))
    (testing "hincr-by 2" (is (= 4 (<!! (r/hincr-by! key hkey 2)))))
    (testing "just checking" (is (= 4 (<!! (r/hget-int key hkey)))))
    )
  )

(deftest concurrent-gets-dont-clobber
  (let [keys (take 100 (repeatedly #(random-string 20))),
        values (take 100 (repeatedly #(random-string 20)))]
    (doall (map (fn [k v]
                  (r/just (r/set! k v))
                  (testing "I get back my value" (is (= v (<!! (r/get k))))))
                keys values))
    (testing "don't be lazy" (is (= (first values) (<!! (r/get (first keys))))))
    ))
