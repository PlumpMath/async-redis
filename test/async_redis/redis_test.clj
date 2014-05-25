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

    (testing "echo"
             (is (= "zingo" (<!! (r/echo "zingo")))))
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

    (testing "expiration by time"
             (let [time (+ 30 (int (/ (System/currentTimeMillis) 1000)))]
               (r/just (r/expire-at! key time))
               (let [seconds-from-now (<!! (r/ttl key))]
                 (is (and (> seconds-from-now 5) (<= seconds-from-now 30)))
                 (r/just (r/persist! key))
                 (is (= -1 (<!! (r/ttl key))))
                 )))
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
    (is (= (count value1) (<!! (r/strlen key))))
    (r/just (r/append! key value2))
    (testing "appended" (is (= (str value1 value2) (<!! (r/get key)))))
    (testing "substr and getrange"
             (is (= value1 (<!! (r/substr key 0 (- (count value1) 1)))))
             (is (= value2 (<!! (r/substr key (count value1) (+ (count value1) (count value2))))))
             (is (= value1 (<!! (r/getrange key 0 (- (count value1) 1)))))
             (is (= value2 (<!! (r/getrange key (count value1) (+ (count value1) (count value2))))))
             )
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

    (testing "hmset and lots of getters"
             (r/just (r/hmset! key hash))
             (is (= true (<!! (r/hexists? key "a"))))
             (is (= (get hash "a") (<!! (r/hget key "a"))))
             (is (= (get hash "b") (<!! (r/hget key "b"))))
             (is (= (get hash "c") (<!! (r/hget key "c"))))
             (is (= hash (<!! (r/hgetall key))))
             (is (= (sort (seq (keys hash))) (sort (seq (<!! (r/hkeys key))))))
             (is (= (sort (seq (vals hash))) (sort (seq (<!! (r/hvals key))))))
             (is (= (list "bing" "foo") (sort (seq (<!! (r/hmget key "a" "b"))))))
             (is (= 3 (<!! (r/hlen key)))))

    (testing "hdel"
             (r/just (r/hdel! key "a"))
             (is (= false (<!! (r/hexists? key "a"))))
             (is (= true (<!! (r/hexists? key "b"))))
             (is (= {"b" "bing" "c" "bang"} (<!! (r/hgetall key))))
             (is (= 2 (<!! (r/hlen key)))))
    )

  (let [key (random-string 20)
        hkey (random-string 10)]
    (r/just (r/hset! key hkey "1"))
    (testing "hincr-by 1" (is (= 2 (<!! (r/hincr-by! key hkey 1)))))
    (testing "hincr-by 2" (is (= 4 (<!! (r/hincr-by! key hkey 2)))))
    (testing "just checking" (is (= 4 (<!! (r/hget-int key hkey)))))
    )
  )

(deftest lists
  (let [key (random-string 20)
        key2 (random-string 20)
        val1 (random-string 10)
        val2 (random-string 10)]

    (testing "rpush"
             (r/just (r/rpush! key val1))
             (is (= val1 (<!! (r/lindex key 0))))
             (is (= [val1] (<!! (r/lrange key 0 -1)))))

    (testing "lpush"
             (r/just (r/lpush! key val2))
             (is (= 2 (<!! (r/llen key))))
             (is (= val2 (<!! (r/lindex key 0))))
             (is (= val1 (<!! (r/lindex key 1))))
             (is (= [val2 val1] (<!! (r/lrange key 0 -1)))))

    (testing "lpushx"
             (r/just (r/del! key))
             (r/just (r/lpushx! key val1))
             (is (= 0 (<!! (r/llen key))))
             (r/just (r/lpush! key val1))
             (is (= 1 (<!! (r/llen key))))
             (r/just (r/lpushx! key val2))
             (is (= 2 (<!! (r/llen key))))
             )

    (testing "rpushx"
             (r/just (r/del! key))
             (r/just (r/rpushx! key val1))
             (is (= 0 (<!! (r/llen key))))
             (r/just (r/rpush! key val1))
             (is (= 1 (<!! (r/llen key))))
             (r/just (r/rpushx! key val2))
             (is (= 2 (<!! (r/llen key))))
             )

    (testing "ltrim"
             (r/just (r/del! key))
             (r/just (r/lpush! key val1 val2))
             (is (= 2 (<!! (r/llen key))))
             (r/just (r/ltrim! key 0 0))
             (is (= 1 (<!! (r/llen key)))))

    (testing "lset"
             (r/just (r/lset! key 0 val1))
             (is (= val1 (<!! (r/lindex key 0)))))

    (testing "lrem"
             (r/just (r/rpush! key val2 val2 val2)) ;; thrice
             (is (= 4 (<!! (r/llen key))))
             (r/just (r/lrem! key 1 val2))
             (is (= 3 (<!! (r/llen key))))
             (r/just (r/lrem! key 2 val2))
             (is (= 1 (<!! (r/llen key)))))

    (testing "popping"
             (is (= val1 (<!! (r/lindex key 0))))
             (is (= 1 (<!! (r/llen key))))
             (is (= val1 (<!! (r/lpop! key))))
             (is (= 0 (<!! (r/llen key))))
             (r/just (r/rpush! key val1 val2))
             (is (= val2 (<!! (r/rpop! key))))
             (is (= 1 (<!! (r/llen key)))))

    (testing "rpoplpush"
             (is (= val1 (<!! (r/lindex key 0))))
             (r/just (r/rpoplpush! key key2))
             (is (= 0 (<!! (r/llen key))))
             (is (= 1 (<!! (r/llen key2))))
             (is (= val1 (<!! (r/lindex key2 0)))))

    (testing "linsert"
             (r/just (r/del! key))
             (r/just (r/lpush! key val1))
             (is (= 2 (<!! (r/linsert! key r/LIST_BEFORE val1 val2))))
             (is (= [val2 val1] (<!! (r/lrange key 0 1))))

             (r/just (r/del! key))
             (r/just (r/lpush! key val1))
             (is (= 2 (<!! (r/linsert! key r/LIST_AFTER val1 val2))))
             (is (= [val1 val2] (<!! (r/lrange key 0 1))))
             )
    )
  )

(deftest sorting
  (let [key (random-string 20)
        vals ["1" "2" "3"]]
    (testing "sort"
             (r/just (apply (partial r/lpush! key) vals))
             (is (= (reverse vals) (<!! (r/lrange key 0 3))))
             (is (= vals (<!! (r/sort key))))
             )
    )
  )

(deftest sets
  (let [key (random-string 20)
        key1 (random-string 20)
        key2 (random-string 20)
        key3 (random-string 20)
        val (random-string 10)
        vals1 (take 20 (repeatedly #(random-string 10)))
        vals2 (take 20 (repeatedly #(random-string 10)))]

    (testing "set basics"
             (r/just (r/sadd! key val val)) ;; deliberate dupe, to prove it's a set, just cuz
             (is (= 1 (<!! (r/scard key))))
             (is (= #{val} (<!! (r/smembers key))))
             (is (= val (<!! (r/srandmember key)))) ;; only, lol
             (r/just (r/srem! key val))
             (is (= 0 (<!! (r/scard key))))
             )

    (testing "multi-set ops"
             (r/just (apply r/sadd! (conj vals1 key1)))
             (r/just (apply r/sadd! (conj vals2 key2)))
             (is (= (count vals1) (<!! (r/scard key1))))
             (is (= (count vals2) (<!! (r/scard key2))))
             (r/just (r/smove! key1 key2 (first vals1)))
             (is (= (- (count vals1) 1) (<!! (r/scard key1))))
             (is (= (+ (count vals2) 1) (<!! (r/scard key2))))
             (is (= 0 (count (<!! (r/sinter key1 key2)))))

             (r/just (r/sadd! key1 (first vals2)))

             (is (= #{(first vals2)} (<!! (r/sinter key1 key2))))
             (r/just (r/sinterstore! key3 key1 key2))
             (is (= #{(first vals2)} (<!! (r/smembers key3))))

             (is (= (set (interleave vals1 vals2)) (<!! (r/sunion key1 key2))))
             (r/just (r/sunionstore! key3 key1 key2))
             (is (= (set (interleave vals1 vals2)) (<!! (r/smembers key3))))

             (r/just (r/del! key1 key2))
             (r/just (r/sadd! key1 (first vals1)))
             (r/just (r/sadd! key1 (first vals2)))
             (r/just (r/sadd! key2 (first vals2)))
             (is (= #{(first vals1)} (<!! (r/sdiff key1 key2))))
             (r/just (r/sdiffstore! key3 key1 key2))
             (is (= #{(first vals1)} (<!! (r/smembers key3))))
             )
    )
  )

(deftest heaps
  (let [key (random-string 20)
        key2 (random-string 20)
        key3 (random-string 20)
        val1 (random-string 10)
        val2 (random-string 10)
        vals1 (take 10 (repeatedly #(random-string 10)))
        scores1 (take 10 (repeatedly rand))
        vals2 (take 10 (repeatedly #(random-string 10)))
        scores2 (take 10 (repeatedly rand))]

    (testing "basics"
             (r/just (r/zadd! key 1.0 val1))
             (r/just (r/zadd! key 2.0 val2))
             (is (= 2 (<!! (r/zcard key))))
             (is (= #{val1} (<!! (r/zrange key 0 0))))
             (is (= #{val1 val2} (<!! (r/zrange key 0 1))))
             (is (= 0 (<!! (r/zrank key val1))))
             (is (= 1 (<!! (r/zrank key val2))))
             (is (= 1 (<!! (r/zrevrank key val1))))
             (is (= 0 (<!! (r/zrevrank key val2))))
             (is (= 1.0 (<!! (r/zscore key val1))))
             (is (= 2.0 (<!! (r/zscore key val2))))
             )

    (testing "ranges"
             (r/just (r/del! key))
             (r/just (r/zadd! key {1.0 val1 2.0 val2}))
             (is (= #{val1 val2} (<!! (r/zrange key 0 -1))))

             (is (= #{val1} (<!! (r/zrange key 0 0))))
             (is (= #{val2} (<!! (r/zrevrange key 0 0))))
             (is (= #{[val1 1.0]} (<!! (r/zrange-with-scores key 0 0))))
             (is (= #{[val2 2.0]} (<!! (r/zrevrange-with-scores key 0 0))))
             )

    (testing "zrem"
             (r/just (r/del! key))
             (r/just (r/zadd! key {1.0 val1 2.0 val2}))
             (is (= #{val1 val2} (<!! (r/zrange key 0 -1))))

             (r/just (r/zrem! key val1))
             (is (= 1 (<!! (r/zcard key))))
             (is (= #{val2} (<!! (r/zrange key 0 -1))))
             (is (= 0 (<!! (r/zrank key val2))))
             )

    (testing "zcount"
             (r/just (r/del! key))
             (r/just (r/zadd! key {1.0 val1 2.0 val2}))
             (is (= #{val1 val2} (<!! (r/zrange key 0 -1))))

             (is (= 2 (<!! (r/zcount key 0.0 3.0))))
             (is (= 2 (<!! (r/zcount key "0.0" "3.0"))))
             (is (= 1 (<!! (r/zcount key 0.0 1.0))))
             (is (= 1 (<!! (r/zcount key "0.0" "1.0"))))
             )

    (testing "zranges"
             (r/just (r/del! key))
             (r/just (r/zadd! key {1.0 val1 2.0 val2}))
             (is (= #{val1 val2} (<!! (r/zrange key 0 -1))))

             (is (= #{val1 val2} (<!! (r/zrange-by-score key 0.0 2.0))))
             (is (= #{val2} (<!! (r/zrange-by-score key 2.0 3.0))))

             (is (= #{val1 val2} (<!! (r/zrevrange-by-score key 2.0 0.0))))
             (is (= #{val2} (<!! (r/zrevrange-by-score key 3.0 2.0))))
             (is (= #{} (<!! (r/zrevrange-by-score key 3.0 2.0 1 1))))

             (is (= #{[val1 1.0] [val2 2.0]} (<!! (r/zrange-by-score-with-scores key 0.0 2.0))))
             (is (= #{[val2 2.0]} (<!! (r/zrange-by-score-with-scores key 2.0 3.0))))
             (is (= #{[val2 2.0]} (<!! (r/zrange-by-score-with-scores key 2.0 3.0 0 1))))
             (is (= #{} (<!! (r/zrange-by-score-with-scores key 2.0 3.0 1 1))))

             (is (= #{[val1 1.0] [val2 2.0]} (<!! (r/zrevrange-by-score-with-scores key 2.0 0.0))))
             (is (= #{[val2 2.0]} (<!! (r/zrevrange-by-score-with-scores key 3.0 2.0))))

             (is (= #{} (<!! (r/zrevrange-by-score-with-scores key 3.0 2.0 1 1))))
             )

    (testing "zremrange"
             (r/just (r/del! key))
             (r/just (r/zadd! key {1.0 val1 2.0 val2}))
             (is (= #{val1 val2} (<!! (r/zrange key 0 -1))))

             (r/just (r/zremrange-by-rank! key 0 0))
             (is (= #{val2} (<!! (r/zrange key 0 -1))))

             (r/just (r/zadd! key {1.0 val1 2.0 val2}))
             (is (= #{val1 val2} (<!! (r/zrange key 0 -1))))

             (r/just (r/zremrange-by-score! key 0 1))
             (is (= #{val2} (<!! (r/zrange key 0 -1))))
             )

    (testing "multi-heap operations"
             (r/just (r/del! key))
             (r/just (r/del! key2))
             (r/just (r/zadd! key (zipmap scores1 vals1)))
             (r/just (r/zadd! key2 (zipmap scores2 vals2)))

             (is (= (count vals1) (<!! (r/zcard key))))
             (is (= (set vals1) (<!! (r/zrange key 0 -1))))
             (is (= (count vals2) (<!! (r/zcard key2))))
             (is (= (set vals2) (<!! (r/zrange key2 0 -1))))

             (r/just (r/zunionstore! key3 key key2))
             (is (= (+ (count vals1) (count vals2)) (<!! (r/zcard key3))))

             (r/just (r/zinterstore! key3 key key2))
             (is (= 0 (<!! (r/zcard key3))))

             (r/just (r/zadd! key {(first scores2) (first vals2)}))

             (r/just (r/zinterstore! key3 key key2))
             (is (= 1 (<!! (r/zcard key3))))
             )
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
