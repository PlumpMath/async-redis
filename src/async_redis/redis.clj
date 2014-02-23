(ns async-redis.redis
  (:refer-clojure :exclude [get type keys set sort eval])
  (:import (java.net.URI)
           (java.util HashSet LinkedHashSet)
           (redis.clients.jedis Client JedisPubSub BuilderFactory
                                Tuple SortingParams Protocol)
           (redis.clients.util SafeEncoder Slowlog)
           (redis.clients.jedis.exceptions JedisDataException))
  (:require [clojure.core.async :as async :refer [go >! <! chan]]))

(def ^{:private true} local-host "127.0.0.1")
(def ^{:private true} default-port 6379)

(defn wrap [val]
  (if (nil? val) false val))

(defn with-chan [f]
  (let [c (chan)]
    (go (>! c (f)))
    c))

(defn get-status-code-reply [client]
  (let [bytes (.getBinaryBulkReply client)]
    (cond (nil? bytes) nil
          :else (SafeEncoder/encode bytes))))

(defn check-multi [client]
  (if (.isInMulti client)
    (throw (JedisDataException. "Can only use transactions when in multi mode"))))

(defmacro non-multi [client & body]
  `(let []
     (check-multi ~client)
     ~@body))

(defmacro ->string [client & body]
  `(with-chan #(non-multi ~client
                          ~@body
                          (wrap (.getBulkReply ~client)))))

(defmacro ->double [client & body]
  `(let [client# ~client]
      (with-chan #(non-multi client#
                             ~@body
                             (let [ret# (.getBulkReply client#)]
                               (if (nil? ret#) ret# (Double/valueOf ret#)))))))

(defmacro ->list [client & body]
  `(with-chan #(non-multi ~client
                          ~@body
                          (wrap (.getMultiBulkReply ~client)))))

(defmacro ->blocking:list [client & body]
  `(let [client# ~client]
     (with-chan #(non-multi client#
                            ~@body
                            (.setTimeoutInfinite client#)
                            (let [response# (wrap (.getMultiBulkReply client#))]
                              (.rollbackTimeout client#)
                              response#)))))

(defmacro ->blocking:string [client & body]
  `(let [client# ~client]
     (with-chan #(non-multi client#
                            ~@body
                            (.setTimeoutInfinite client#)
                            (let [response# (wrap (.getBulkReply client#))]
                              (.rollbackTimeout client#)
                              response#)))))

(defmacro ->int [client & body]
  `(with-chan #(non-multi ~client
                          ~@body
                          (wrap (.getIntegerReply ~client)))))

(defmacro ->boolean [client & body]
  `(with-chan #(non-multi ~client
                          ~@body
                          (= 1 (.getIntegerReply ~client)))))

(defmacro ->bytes [client & body]
  `(with-chan #(non-multi ~client
                          ~@body
                          (.getBinaryBulkReply ~client))))

(defmacro ->status [client & body]
  `(with-chan #(non-multi ~client
                          ~@body
                          (wrap (get-status-code-reply ~client)))))

(defmacro ->status-multi [client & body]
  `(with-chan (fn []
                ~@body
                (get-status-code-reply ~client))))

(defmacro ->set [client & body]
  `(with-chan #(non-multi ~client
                          ~@body
                          (.build BuilderFactory/STRING_SET (.getBinaryMultiBulkReply ~client)))))

(defmacro ->map [client & body]
  `(with-chan #(non-multi ~client
                          ~@body
                          (.build BuilderFactory/STRING_MAP (.getBinaryMultiBulkReply ~client)))))

(defmacro ->list>set [client & body]
  `(with-chan #(non-multi ~client
                          ~@body
                          (HashSet. (.getMultiBulkReply ~client)))))

(defmacro ->list>lset [client & body]
  `(with-chan #(non-multi ~client
                          ~@body
                          (LinkedHashSet. (.getMultiBulkReply ~client)))))

(defmacro ->tupled-set [client & body]
  `(let [client# ~client]
     (with-chan #(non-multi client#
                            ~@body
                            (let [members-with-scores# (.getMultiBulkReply client#)
                                  iterator# (.iterator members-with-scores#)
                                  tuple-set# (LinkedHashSet.)]
                              (while (.hasNext iterator#)
                                     (let [key# (.next iterator#)
                                           value# (.next iterator#)]
                                     (.add tuple-set# (Tuple. key# value#))))
                              tuple-set#)))))


(defn connect [host port]
  (let [client (Client. (or host local-host) (or port default-port))]
    (.connect client)
    client))

(defn disconnect* [client] (.disconnect client))
(def ^:dynamic disconnect disconnect*)

(defn select* [client db] (.select client db))
(def ^:dynamic select select*)

(defn db* [client] (.getDB client))
(def ^:dynamic db db*)

(defn get* [client key] (->string client (.get client key)))
(def ^:dynamic get get*)

(defn exists* [client key] (->boolean client (.exists client key)))
(def ^:dynamic exists exists*)

(defn del* [client & keys] (->int client (.del client (into-array String keys))))
(def ^:dynamic del del*)

(defn type* [client key] (->status client (.type client key)))
(def ^:dynamic type type*)

(defn keys* [client pattern] (->status client (.type client key)))
(def ^:dynamic keys keys*)

(defn random-key* [client] (->string client (.randomKey client)))
(def ^:dynamic random-key random-key*)

(defn rename* [client old-key new-key] (->status client (.rename client old-key new-key)))
(def ^:dynamic rename rename*)

(defn renamenx* [client old-key new-key] (->status client (.renamenx client old-key new-key)))
(def ^:dynamic renamenx renamenx*)

(defn expire* [client key seconds] (->int client (.expire client key seconds)))
(def ^:dynamic expire expire*)

(defn expire-at* [client key timestamp] (->int client (.expireAt client key timestamp)))
(def ^:dynamic expire-at expire-at*)

(defn ttl* [client key] (->int client (.ttl client key)))
(def ^:dynamic ttl ttl*)

(defn move* [client key db-index] (->int client (.move client key db-index)))
(def ^:dynamic move move*)

(defn getset* [client key value] (->string client (.getSet client key value)))
(def ^:dynamic getset getset*)

(defn mget* [client & keys] (->list client (.mget client keys)))
(def ^:dynamic mget mget*)

(defn setnx* [client key value] (->int client (.setnx client key value)))
(def ^:dynamic setnx setnx*)

(defn setex* [client key seconds value] (->status client (.setex client key seconds value)))
(def ^:dynamic setex setex*)

(defn mset* [client & keysvalues] (->status client (.mset client keysvalues)))
(def ^:dynamic mset mset*)

(defn msetnx* [client & keysvalues] (->int client (.msetnx client keysvalues)))
(def ^:dynamic msetnx msetnx*)

(defn decr-by* [client key inc] (->int client (.decrBy client key inc)))
(def ^:dynamic decr-by decr-by*)

(defn decr* [client key] (->int client (.decr client key)))
(def ^:dynamic decr decr*)

(defn incr-by* [client key inc] (->int client (.incrBy client key inc)))
(def ^:dynamic incr-by incr-by*)

(defn incr* [client key] (->int client (.incr client key)))
(def ^:dynamic incr incr*)

(defn append* [client key value] (->int client (.append client key value)))
(def ^:dynamic append append*)

(defn substr* [client key start end] (->string client (.substr client key start end)))
(def ^:dynamic substr substr*)


(defn hset* [client key field value] (->int client (.hset client key field value)))
(def ^:dynamic hset hset*)

(defn hget* [client key field] (->string client (.hget client key field)))
(def ^:dynamic hget hget*)

(defn hsetnx* [client key field value] (->int client (.hsetnx client key field value)))
(def ^:dynamic hsetnx hsetnx*)

(defn hmset* [client key hash] (->status client (.hmset client key hash)))
(def ^:dynamic hmset hmset*)

(defn hmget* [client key & fields] (->status client (.hmget client key fields)))
(def ^:dynamic hmget hmget*)

(defn hincr-by* [client key field inc] (->int client (.hincrBy client key field inc)))
(def ^:dynamic hincr-by hincr-by*)

(defn hexists* [client key field] (->boolean client (.hexists client key field)))
(def ^:dynamic hexists hexists*)

(defn hdel* [client key & fields] (->int client (.hdel client key fields)))
(def ^:dynamic hdel hdel*)

(defn hlen* [client key] (->int client (.hlen client key)))
(def ^:dynamic hlen hlen*)

(defn hkeys* [client key] (->set client (.hkeys client key)))
(def ^:dynamic hkeys hkeys*)

(defn hvals* [client key] (->list client (.hvals client key)))
(def ^:dynamic hvals hvals*)

(defn hgetall* [client key] (->map client (.hgetAll client key)))
(def ^:dynamic hgetall hgetall*)



(defn rpush* [client key & strings] (->int client (.rpush client key strings)))
(def ^:dynamic rpush rpush*)

(defn lpush* [client key & strings] (->int client (.lpush client key strings)))
(def ^:dynamic lpush lpush*)

(defn llen* [client key] (->int client (.llen client key)))
(def ^:dynamic llen llen*)

(defn lrange* [client key start end] (->list client (.lrange client key start end)))
(def ^:dynamic lrange lrange*)

(defn ltrim* [client key start end] (->status client (.ltrim client key start end)))
(def ^:dynamic ltrim ltrim*)

(defn lindex* [client key index] (->string client (.lindex client key index)))
(def ^:dynamic lindex lindex*)

(defn lset* [client key index value] (->status client (.lset client key index value)))
(def ^:dynamic lset lset*)

(defn lrem* [client key count value] (->int client (.lrem client key count value)))
(def ^:dynamic lrem lrem*)

(defn lpop* [client key] (->string client (.lpop client key)))
(def ^:dynamic lpop lpop*)

(defn rpop* [client key] (->string client (.rpop client key)))
(def ^:dynamic rpop rpop*)

(defn rpoplpush* [client src-key dest-key] (->string client (.rpoplpush client src-key dest-key)))
(def ^:dynamic rpoplpush rpoplpush*)



(defn sadd* [client key & members] (->int client (.sadd client key members)))
(def ^:dynamic sadd sadd*)

(defn smembers* [client key] (->list>set client (.smembers client key)))
(def ^:dynamic smembers smembers*)

(defn srem* [client key & members] (->int client (.srem client key members)))
(def ^:dynamic srem srem*)

(defn spop* [client key] (->string client (.spop client key)))
(def ^:dynamic spop spop*)

(defn smove* [client src-key dest-key member] (->int client (.smove client src-key dest-key member)))
(def ^:dynamic smove smove*)

(defn scard* [client key] (->int client (.scard client key)))
(def ^:dynamic scard scard*)

(defn sismember* [client key member] (->boolean client (.sismember client key member)))
(def ^:dynamic sismember sismember*)

(defn sinter* [client & keys] (->list>set client (.sinter client keys)))
(def ^:dynamic sinter sinter*)

(defn sinterstore* [client dest-key & keys] (->int client (.sinterstore client dest-key keys)))
(def ^:dynamic sinterstore sinterstore*)

(defn sunion* [client & keys] (->list>set client (.sunion client keys)))
(def ^:dynamic sunion sunion*)

(defn sunionstore* [client dest-key & keys] (->int client (.sunionstore client dest-key keys)))
(def ^:dynamic sunionstore sunionstore*)

(defn sdiff* [client & keys] (->set client (.sdiff client keys)))
(def ^:dynamic sdiff sdiff*)

(defn sdiffstore* [client dest-key & keys] (->int client (.sdiffstore dest-key keys)))
(def ^:dynamic sdiffstore sdiffstore*)

(defn srandmember*
  ([client key] (->string client (.srandmember client key)))
  ([client key count] (->list client (.srandmember client key count))))
(def ^:dynamic srandmember srandmember*)

(defn zadd*
  ([client key score member] (->int client (.zadd client key score member)))
  ([client key map] (->int client (.zadd client key map))))
(def ^:dynamic zadd zadd*)

(defn zrange* [client key start end] (->list>lset client (.zrange client key start end)))
(def ^:dynamic zrange zrange*)

(defn zrem* [client key & members] (->int client (.zrem client key members)))
(def ^:dynamic zrem zrem*)

(defn zincr-by* [client key score member] (->double client (.zincrby client key score member)))
(def ^:dynamic zincr-by zincr-by*)

(defn zrank* [client key member] (->int client (.zrank client key member)))
(def ^:dynamic zrank zrank*)

(defn zrevrank* [client key member] (->int client (.zrevrank client key member)))
(def ^:dynamic zrevrank zrevrank*)

(defn zrevrange* [client key start end] (->list>lset client (.zrevrange client key start end)))
(def ^:dynamic zrevrange zrevrange*)

(defn zrange-with-scores* [client key start end] (->tupled-set client (.zrangeWithScores client key start end)))
(def ^:dynamic zrange-with-scores zrange-with-scores*)

(defn zrevrange-with-scores* [client key start end] (->tupled-set client (.zrevrangeWithScores client key start end)))
(def ^:dynamic zrevrange-with-scores zrevrange-with-scores*)

(defn zcard* [client key] (->int client (.zcard client key)))
(def ^:dynamic zcard zcard*)

(defn zscore* [client key member] (->double client (.zscore client key member)))
(def ^:dynamic zscore zscore*)

(defn watch* [client & keys] (->status-multi client (.watch client keys)))
(def ^:dynamic watch watch*)

(defmulti sort* (fn [& args] [(nth args 2 false) (> (count args) 3)]))
(defmethod sort* [false false] sort-simple [client key] (->list client (.sort client key)))
(defmethod sort* [:SortingParams :false] sort-with-params [client key sort-params] (->list client (.sort client key #^SortParams sort-params)))
(defmethod sort* [:String :false] sort-to-dest [client key dest-key] (->int client (.sort client key #^String dest-key)))
(defmethod sort* [:SortingParams :true] sort-with-params-to-dest [client key sort-params dest-key] (->int client (.sort client key sort-params dest-key)))
(def ^:dynamic sort sort*)

(defmulti blpop* (fn [client & args] (first args)))
(defmethod blpop* :Integer
  blpop-timeout [client timeout & keys] (->blocking:list client (.blpop client (conj keys (str timeout)))))
(defmethod blpop* :String
  blpop-timeout [client & keys] (->blocking:list client (.blpop client keys)))
(def ^:dynamic blpop blpop*)

(defn brpop* [client & keys] (->blocking:list client (.brpop keys)))
(def ^:dynamic brpop brpop*)

(defmulti zcount* (fn [client key min max] [client key min max]))
(defmethod zcount* [:Object :String :Double :Double]
  zcount-doubles [client key min max]
  (->int client (.zcount client key #^Double min #^Double max)))
(defmethod zcount* [:Object :String :String :String]
  zcount-strings [client key min max]
  (->int client (.zcount client key #^String min #^String max)))
(def ^:dynamic zcount zcount*)

(defmulti zrange-by-score* (fn [client key min max & optional] [client key min max]))
(defmethod zrange-by-score* [:Object :String :Double :Double]
  zrange-by-score-doubles [client key min max & optional]
  (if (empty? optional)
    (->list>lset client (.zrangeByScore client key #^Double min #^Double max))
    (let [offset (first optional)
          count (second optional)]
      (->list>lset client (.zrangeByScore client key #^Double min #^Double max offset count)))))
(defmethod zrange-by-score* [:Object :String :String :String]
  zrange-by-score-strings [client key min max & optional]
  (if (empty? optional)
    (->list>lset client (.zrangeByScore client key #^String min #^String max))
    (let [offset (first optional)
          count (second optional)]
      (->list>lset client (.zrangeByScore client key #^String min #^String max offset count)))))
(def ^:dynamic zrange-by-score zrange-by-score*)

(defmulti zrange-by-score-with-scores* (fn [client key min max & optional] [client key min max]))
(defmethod zrange-by-score-with-scores* [:Object :String :Double :Double]
  zrange-by-score-with-score-doubles [client key min max & optional]
  (if (empty? optional)
    (->tupled-set client (.zrangeByScoreWithScores client key #^Double min #^Double max))
    (let [offset (first optional)
          count (second optional)]
      (->tupled-set client (.zrangeByScoreWithScores client key #^Double min #^Double max offset count)))))
(defmethod zrange-by-score-with-scores* [:Object :String :String :String]
  zrange-by-score-with-score-strings [client key min max & optional]
  (if (empty? optional)
    (->tupled-set client (.zrangeByScoreWithScores client key #^String min #^String max))
    (let [offset (first optional)
          count (second optional)]
      (->tupled-set client (.zrangeByScoreWithScores client key #^String min #^String max offset count)))))
(def ^:dynamic zrange-by-score-with-scores zrange-by-score-with-scores*)

(defmulti zrevrange-by-score* (fn [client key min max & optional] [client key min max]))
(defmethod zrevrange-by-score* [:Object :String :Double :Double]
  zrevrange-by-score-doubles [client key min max & optional]
  (if (empty? optional)
    (->list>lset client (.zrevrangeByScore client key #^Double min #^Double max))
    (let [offset (first optional)
          count (second optional)]
      (->list>lset client (.zrevrangeByScore client key #^Double min #^Double max offset count)))))
(defmethod zrevrange-by-score* [:Object :String :String :String]
  zrevrange-by-score-strings [client key min max & optional]
  (if (empty? optional)
    (->list>lset client (.zrevrangeByScore client key #^String min #^String max))
    (let [offset (first optional)
          count (second optional)]
      (->list>lset client (.zrevrangeByScore client key #^String min #^String max offset count)))))
(def ^:dynamic zrevrange-by-score zrevrange-by-score*)

(defmulti zrevrange-by-score-with-scores* (fn [client key min max & optional] [client key min max]))
(defmethod zrevrange-by-score-with-scores* [:Object :String :Double :Double]
  zrevrange-by-score-with-score-doubles [client key min max & optional]
  (if (empty? optional)
    (->tupled-set client (.zrevrangeByScoreWithScores client key #^Double min #^Double max))
    (let [offset (first optional)
          count (second optional)]
      (->tupled-set client (.zrevrangeByScoreWithScores client key #^Double min #^Double max offset count)))))
(defmethod zrevrange-by-score-with-scores* [:Object :String :String :String]
  zrevrange-by-score-with-score-strings [client key min max & optional]
  (if (empty? optional)
    (->tupled-set client (.zrevrangeByScoreWithScores client key #^String min #^String max))
    (let [offset (first optional)
          count (second optional)]
      (->tupled-set client (.zrevrangeByScoreWithScores client key #^String min #^String max offset count)))))
(def ^:dynamic zrevrange-by-score-with-scores zrevrange-by-score-with-scores*)

(defn zremrange-by-rank* [client key start end] (->int client (.zremrangeByRank key start end)))
(def ^:dynamic zremrange-by-rank zremrange-by-rank*)

(defmulti zremrange-by-score* (fn [client key start end] [client key start end]))
(defmethod zremrange-by-score* [:Object :String :Double :Double]
  zremrange-by-score-doubles [client key start end]
  (->int client (.zremrangeByScore client key #^Double start #^Double end)))
(defmethod zremrange-by-score* [:Object :String :String :String]
  zremrange-by-score-strings [client key start end]
  (->int client (.zremrangeByScore client key #^String start #^String end)))
(def ^:dynamic zremrange-by-score zremrange-by-score*)

(defmulti zunionstore* (fn [client dest-key & args] (first args)))
(defmethod zunionstore* :String
  zunionstore-normal [client dest-key & keys] (->int client (.zunionstore client dest-key keys)))
(defmethod zunionstore* :redis.clients.jedis.ZParams
  zunionstore-with-params [client dest-key params & keys] (->int client (.zunionstore client dest-key params keys)))
(def ^:dynamic zunionstore zunionstore*)

(defmulti zinterstore* (fn [client dest-key & args] (first args)))
(defmethod zinterstore* :String
  zinterstore-normal [client dest-key & keys] (->int client (.zinterstore client dest-key keys)))
(defmethod zinterstore* :redis.clients.jedis.ZParams
  zinterstore-with-params [client dest-key params & keys] (->int client (.zinterstore client dest-key params keys)))
(def ^:dynamic zinterstore zinterstore*)

(defn strlen* [client key] (->string client (.strlen client key)))
(def ^:dynamic strlen strlen*)

(defn lpushx* [client key & strings] (->int client (.lpushx client key strings)))
(def ^:dynamic lpushx lpushx*)

(defn persist* [client key] (->int client (.persist client key)))
(def ^:dynamic persist persist*)

(defn rpushx* [client key & strings] (->int (.rpushx client key strings)))
(def ^:dynamic rpushx rpushx*)

(defn echo* [client string] (->string (.echo client string)))
(def ^:dynamic echo echo*)

(defn linsert* [client key where pivot value] (->int (.linsert client key where pivot value)))
(def ^:dynamic linsert linsert*)

(defn brpoplpush* [client source dest timeout] (->blocking:string client (.brpoplpush client source dest timeout)))
(def ^:dynamic brpoplpush brpoplpush*)


(defmulti setbit* (fn [client offset value] value))
(defmethod setbit* :Boolean setbit-bool [client offset value] (->boolean client (.setbit client key offset #^Boolean value)))
(defmethod setbit* :String setbit-bool [client offset value] (->boolean client (.setbit client key offset #^String value)))
(def ^:dynamic setbit setbit*)

(defn getbit* [client key offset] (->boolean client (.getbit client key offset)))
(def ^:dynamic getbit getbit*)

(defn setrange* [client key offset value] (->int client (.setrange client key offset value)))
(def ^:dynamic setrange setrange*)

(defn getrange* [client key start-offset end-offset] (->string client (.getrange client key start-offset end-offset)))
(def ^:dynamic getrange getrange*)

(defn config-get* [client pattern] (->list client (.configGet client pattern)))
(def ^:dynamic config-get config-get*)

(defn config-set* [client param val] (->status client (.configSet client param val)))
(def ^:dynamic config-set config-set*)

(defn ^{:private true} get-eval-result [client]
  (let [result (.getOne client)]
        (wrap (cond (isa? result byte[]) (SafeEncoder/encode result)
                    (seq? result) (map (fn [x] (if (nil? x) x (SafeEncoder/encode x)))
                                       result)
                    :else result))))

(defn ^{:private true} *eval [client script key-count & params]
     (with-chan (fn []
                  (.setTimeoutInfinite client)
                  (.eval client script key-count params)
                  (let [result (get-eval-result client)]
                    (.rollbackTimeout client)
                    result))))

(defn ^{:private true} *evalsha [client sha1 key-count & params]
     (with-chan (fn []
                  (.setTimeoutInfinite client)
                  (.evalsha client sha1 key-count params)
                  (let [result (get-eval-result client)]
                    (.rollbackTimeout client)
                    result))))

(defn eval-params* [keys values] (into-array String (interleave keys values)))
(def ^:dynamic eval-params eval-params*)

(defmulti eval* (fn [client script & optional] (if (nil? optional) false (first optional))))
(defmethod eval* false [client script] (*eval client script 0))
(defmethod eval* :Integer [& params] (apply *eval params))
(defmethod eval* :List<String> [client script keys params] (*eval client script (count keys) (eval-params params)))
(def ^:dynamic eval eval*)


(defmulti evalsha* (fn [client sha1 & optional] (if (nil? optional) false (first optional))))
(defmethod evalsha* false [client sha1] (*evalsha client sha1 0))
(defmethod evalsha* :Integer [& params] (apply *evalsha params))
(defmethod evalsha* :List<String> [client sha1 keys params] (*evalsha client sha1 (count keys) (eval-params params)))
(def ^:dynamic evalsha evalsha*)

(defn scripts-exist?* [client & sha1s]
  (let [c (chan)]
    (go
     (.scriptExists client (into-array String sha1s))
     (>! c (map (fn [i] (= i 1)) (.getIntegerMultiBulkReply client))))
    c))
(def ^:dynamic scripts-exist? scripts-exist?*)

(defn script-exists?* [client sha1]
  (let [c (scripts-exist?* client sha1)
        c2 (chan)]
    (go (let [replies (<! c)]
          (>! c2 (first replies))))
    c2))
(def ^:dynamic script-exists? script-exists?*)

(defn script-load* [client script] (->string client (.scriptLoad client script)))
(def ^:dynamic script-load script-load*)

(defmulti slowlog-get* (fn [client & args] (empty? args)))
(defmethod slowlog-get* :false [client]
  (with-chan (fn []
               (.slowlogGet client)
               (Slowlog/from (.getObjectMultiBulkReply client)))))
(defmethod slowlog-get* :true [client & entries]
  (with-chan (fn []
               (.slowlogGet client entries)
               (Slowlog/from (.getObjectMultiBulkReply client)))))
(def ^:dynamic slowlog-get slowlog-get*)

(defn object-refcount* [client string] (->int client (.objectRefcount client string)))
(def ^:dynamic object-refcount object-refcount*)

(defn object-encoding* [client string] (->string client (.objectEncoding client string)))
(def ^:dynamic object-encoding object-encoding*)

(defn object-idletime* [client string] (->string client (.objectIdletime client string)))
(def ^:dynamic object-idletime object-idletime*)

(defn bitcount* [client key start end] (->int client (.bitcount client key start end)))
(def ^:dynamic bitcount bitcount*)

(defn bitop* [client op dest-key & src-keys] (->int client (.bitop op dest-key src-keys)))
(def ^:dynamic bitop bitop*)

(defn sentinel-masters* [client]
  (with-chan (fn []
               (.sentinel client Protocol/SENTINEL_MASTERS)
               (map (fn [o] (.build BuilderFactory/STRING_MAP o)) (.getObjectMultiBulkReply client)))))
(def ^:dynamic sentinel-masters sentinel-masters*)

(defn sentinel-get-master-addr-by-name* [client master-name]
  (with-chan (fn []
               (.sentinel client Protocol/SENTINEL_GET_MASTER_ADDR_BY_NAME master-name)
               (.build BuilderFactory/STRING_LIST (.getObjectMultiBulkReply client)))))
(def ^:dynamic sentinel-get-master-addr-by-name sentinel-get-master-addr-by-name*)

(defn sentinel-reset* [client pattern] (->int client (.sentinel client Protocol/SENTINEL_RESET pattern)))
(def ^:dynamic sentinel-reset sentinel-reset*)

(defn sentinel-slaves* [client master-name]
  (with-chan (fn []
               (.sentinel client Protocol/SENTINEL_SLAVES master-name)
               (map (fn [o] (.build BuilderFactory/STRING_MAP o)) (.getObjectMultiBulkReply client)))))
(def ^:dynamic sentinel-slaves sentinel-slaves*)

(defn dump* [client key] (->bytes client (.dump client key)))
(def ^:dynamic dump dump*)

(defn restore* [client key ttl serialized-value] (->status client (.restore client key ttl serialized-value)))
(def ^:dynamic restore restore*)

(defn pexpire* [client key millis] (->int client (.pexpire client key millis)))
(def ^:dynamic pexpire pexpire*)

(defn pexpire-at* [client key milli-ts] (->int client (.pexpireAt client key milli-ts)))
(def ^:dynamic pexpire-at pexpire-at*)

(defn pttl* [client key] (->int client (.pttl client key)))
(def ^:dynamic pttl pttl*)

(defn incr-by-float* [client key inc] (->double client (.incrByFloat client key inc)))
(def ^:dynamic incr-by-float incr-by-float*)

(defn psetex* [client key millis val] (->status client (.psetex client key millis val)))
(def ^:dynamic psetex psetex*)


;; key , value (->status (.set key val))
;; key, val, nxx
;; key, value, nxx, expr, (long) time
;; key, value, nxx, expr, (int) time
(defmulti set* (fn [client & args] [(count args) (nth args 4 false)]))
(defmethod set* [2 false] [client key val] (->status client (.set client key val)))
(defmethod set* [3 false] [client key val nxxx] (->status client (.set client key val nxxx)))
(defmethod set* [4 :Long] [client key val expr time] (->status client (.set client key val expr #^Long time)))
(defmethod set* [4 :Integer] [client key val expr time] (->status client (.set client key val expr #^Integer time)))
(def ^:dynamic set set*)

(defn client-kill* [client client-name] (->status client (.clientKill client client-name)))
(def ^:dynamic client-kill client-kill*)

(defn client-setname* [client name] (->status client (.clientSetname client name)))
(def ^:dynamic client-setname client-setname*)

(defn migrate* [client host port key dest-db timeout] (->status client (.migrate host port key dest-db timeout)))
(def ^:dynamic migrate migrate*)

(defn hincr-by-float* [client key field inc] (->double client (.hincrByFloat client key field inc)))
(def ^:dynamic hincr-by-float hincr-by-float*)


(defn subscribe* [client jedis-pub-sub & channels]
  (with-chan (fn []
               (.setTimeoutInfinite client)
               (.proceed jedis-pub-sub client channels)
               (.rollbackTimeout client))))
(def ^:dynamic subscribe subscribe*)

(defn publish* [client channel message] (->int client (.publish client channel message)))
(def ^:dynamic publish publish*)


(defn psubscribe* [client jedis-pub-sub & patterns]
  (with-chan #(non-multi client
                         (.setTimeoutInfinite client)
                         (.proceedWithPatterns jedis-pub-sub client patterns)
                         (.rollbackTimeout client))))
(def ^:dynamic psubscribe psubscribe*)


(defmacro with [client & body]
  `(let [client# ~client]
     (binding [disconnect #(disconnect* client#)
               get #(get* client# %)
               set #(set* client# %1 %2)
               exists #(exists* client# %)
               del (fn [& args#] (apply del* (cons client# args#)))
               type #(type* client# %)
               keys #(keys* client# %)
               random-key #(random-key* client#)
               rename #(rename* client# %1 %2)
               renamenx #(renamenx* client# %1 %2)
               expire-at #(expire-at* client# %1 %2)
               ttl #(ttl* client# %)
               move #(move* client# %1 %2)
               getset #(getset* client# %1 %2)
               mget (fn [& args#] (apply mget* (cons client# args#)))
               setnx #(setnx* client# %1 %2)
               setex #(setex* client# %1 %2 %3)
               mset (fn [& args#] (apply mset* (cons client# args#)))
               msetnx (fn [& args#] (apply msetnx* (cons client# args#)))
               decr-by #(decr-by* client# %1 %2)
               decr #(decr* client# %)
               incr-by #(incr-by* client# %1 %2)
               incr #(incr* client# %)
               append #(append* client# %1 %2)
               substr #(substr* client# %1 %2 %3)
               hset #(hset* client# %1 %2 %3)
               hget #(hget* client# %1 %2)
               hsetnx #(hsetnx* client# %1 %2 %3)
               hmset #(hmset* client# %1 %2)
               hmget (fn [& args#] (apply hmget* (cons client# args#)))
               hincr-by #(hincr-by* client# %1 %2 %3)
               hexists #(hexists* client# %1 %2)
               hdel (fn [& args#] (apply hdel* (cons client# args#)))
               hlen #(hlen* client# %)
               hkeys #(hkeys* client# %)
               hvals #(hvals* client# %)
               hgetall #(hgetall* client# %)
               rpush (fn [& args#] (apply rpush* (cons (client# args#))))
               lpush (fn [& args#] (apply lpush* (cons (client# args#))))
               llen #(llen* client# %)
               lrange #(lrange* client# %1 %2 %3)
               ltrim #(ltrim* client# %1 %2 %3)
               lindex #(lindex* client# %1 %2)
               lset #(lset* client# %1 %2 %3)
               lrem #(lrem* client# %1 %2 %3)
               lpop #(lpop* client# %)
               rpop #(rpop* client# %)
               rpoplpush #(rpoplpush* client# %1 %2)
               sadd (fn [& args#] (apply sadd* (cons client# args#)))
               smembers #(smembers* client# %)
               srem (fn [& args#] (apply srem* (cons client# args#)))
               spop #(spop* client# %)
               smove #(smove* client# %1 %2 %3)
               scard #(scard* client# %)
               sismember #(sismember* client# %1 %2)
               sinter (fn [& args#] (apply sinter* (cons client# args#)))
               sinterstore (fn [& args#] (apply sinterstore* (cons client# args#)))
               sunion (fn [& args#] (apply sunion* (cons client# args#)))
               sunionstore (fn [& args#] (apply sunionstore* (cons client# args#)))
               sdiff (fn [& args#] (apply sdiff* (cons client# args#)))
               sdiffstore (fn [& args#] (apply sdiffstore* (cons client# args#)))
               srandmember (fn [& args#] (apply srandmember* (cons client# args#)))
               zadd (fn [& args#] (apply zadd* (cons client# args#)))
               zrange #(zrange* client# %1 %2 %3)
               zrem (fn [& args#] (apply zrem* (cons client# args#)))
               zincr-by #(zincr-by* client# %1 %2 %3)
               zrank #(zrank* client# %1 %2)
               zrevrank #(zrevrank* client# %1 %2)
               zrevrange #(zrevrange* client# %1 %2 %3)
               zrange-with-scores #(zrange-with-scores* client# %1 %2 %3)
               zrevrange-with-scores #(zrevrange-with-scores* client# %1 %2 %3)
               zcard #(zcard* client# %)
               zscore #(zscore* client# %1 %2)
               watch (fn [& args#] (apply watch* (cons client# args#)))
               sort (fn [& args#] (apply sort* (cons client# args#)))
               blpop (fn [& args#] (apply blpop* (cons client# args#)))
               brpop (fn [& args#] (apply brpop* (cons client# args#)))
               zcount (fn [& args#] (apply zcount* (cons client# args#)))
               zrange-by-score (fn [& args#] (apply zrange-by-score* (cons client# args#)))
               zrange-by-score-with-scores (fn [& args#] (apply zrange-by-score-with-scores* (cons client# args#)))
               zrevrange-by-score (fn [& args#] (apply zrevrange-by-score* (cons client# args#)))
               zrevrange-by-score-with-scores (fn [& args#] (apply zrevrange-by-score-with-scores* (cons client# args#)))
               zremrange-by-rank #(zremrange-by-rank* client# %1 %2 %3)
               zremrange-by-score (fn [& args#] (apply zremrange-by-score* (cons client# args#)))
               zunionstore (fn [& args#] (apply zunionstore* (cons client# args#)))
               zinterstore (fn [& args#] (apply zinterstore* (cons client# args#)))
               strlen #(strlen client# %)
               lpushx (fn [& args#] (apply lpushx* (cons client# args#)))
               persist #(persist* client# %)
               rpushx (fn [& args#] (apply rpushx* (cons client# args#)))
               echo #(echo client# %)
               linsert #(linsert* client# %1 %2 %3 %4)
               brpoplpush #(brpoplpush* client# %1 %2 %3)
               setbit (fn [& args#] (apply setbit* (cons client# args#)))
               getbit #(getbit* client# %1 %2)
               setrange #(setrange* client# %1 %2 %3)
               getrange #(getrange* client# %1 %2 %3)
               config-get #(config-get* client# %)
               config-set #(config-set* client# %1 %2)
               eval-params #(eval-params* client# %1 %2)
               eval (fn [& args#] (apply eval* (cons client# args#)))
               evalsha (fn [& args#] (apply evalsha* (cons client# args#)))
               scripts-exist? (fn [& args#] (apply scripts-exist?* (cons client# args#)))
               script-exists? (fn [& args#] (apply script-exists?* (cons client# args#)))
               script-load #(script-load* client# %)
               slowlog-get (fn [& args#] (apply slowlog-get* (cons client# args#)))
               object-refcount #(object-refcount* client# %)
               object-encoding #(object-encoding* client# %)
               object-idletime #(object-idletime* client# %)
               bitcount #(bitcount client# %1 %2 %3)
               bitop (fn [& args#] (apply bitop* (cons client# args#)))
               sentinel-masters #(sentinel-masters* client#)
               sentinel-get-master-addr-by-name #(sentinel-get-master-addr-by-name* client# %)
               sentinel-reset #(sentinel-reset* client# %)
               sentinel-slaves #(sentinel-slaves* client# %)
               dump #(dump* client# %)
               restore #(restore* client# %1 %2 %3)
               pexpire #(pexpire* client# %1 %2)
               pexpire-at #(pexpire-at* client# %1 %2)
               pttl #(pttl* client# %1)
               incr-by-float #(incr-by-float* client# %1 %2)
               psetex #(psetex* client# %1 %2 %3)
               set (fn [& args#] (apply set* (cons client# args#)))
               client-kill #(client-kill* client# %1)
               client-setname #(client-setname* client# %1)
               migrate #(migrate* client# %1 %2 %3 %4 %5)
               hincr-by-float #(hincr-by-float* client# %1 %2 %3)
               subscribe (fn [& args#] (apply subscribe* (cons client# args#)))
               publish #(publish* client# %1 %2)
               psubscribe (fn [& args#] (apply psubscribe* (cons client# args#)))
               ]
       ~@body)))
