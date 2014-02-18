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

(defn exists [client key] (->boolean client (.exists client key)))
(defn del [client & keys] (->int client (.del client (into-array String keys))))
(defn type [client key] (->status client (.type client key)))
(defn keys [client pattern] (->status client (.type client key)))
(defn random-key [client] (->string client (.randomKey client)))
(defn rename [client old-key new-key] (->status client (.rename client old-key new-key)))
(defn renamenx [client old-key new-key] (->status client (.renamenx client old-key new-key)))
(defn expire [client key seconds] (->int client (.expire client key seconds)))
(defn expire-at [client key timestamp] (->int client (.expireAt client key timestamp)))
(defn ttl [client key] (->int client (.ttl client key)))
(defn move [client key db-index] (->int client (.move client key db-index)))
(defn getset [client key value] (->string client (.getSet client key value)))
(defn mget [client & keys] (->list client (.mget client keys)))
(defn setnx [client key value] (->int client (.setnx client key value)))
(defn setex [client key seconds value] (->status client (.setex client key seconds value)))
(defn mset [client & keysvalues] (->status client (.mset client keysvalues)))
(defn msetnx [client & keysvalues] (->int client (.msetnx client keysvalues)))
(defn decr-by [client key inc] (->int client (.decrBy client key inc)))
(defn decr [client key] (->int client (.decr client key)))
(defn incr-by [client key inc] (->int client (.incrBy client key inc)))
(defn incr [client key] (->int client (.incr client key)))
(defn append [client key value] (->int client (.append client key value)))
(defn substr [client key start end] (->string client (.substr client key start end)))


(defn hset [client key field value] (->int client (.hset client key field value)))
(defn hget [client key field] (->string client (.hget client key field)))
(defn hsetnx [client key field value] (->int client (.hsetnx client key field value)))
(defn hmset [client key hash] (->status client (.hmset client key hash)))
(defn hmget [client key & fields] (->status client (.hmget client key fields)))
(defn hincr-by [client key field inc] (->int client (.hincrBy client key field inc)))
(defn hexists [client key field] (->boolean client (.hexists client key field)))
(defn hdel [client key & fields] (->int client (.hdel client key fields)))
(defn hlen [client key] (->int client (.hlen client key)))
(defn hkeys [client key] (->set client (.hkeys client key)))
(defn hvals [client key] (->list client (.hvals client key)))
(defn hgetall [client key] (->map client (.hgetAll client key)))


(defn rpush [client key & strings] (->int client (.rpush client key strings)))
(defn lpush [client key & strings] (->int client (.lpush client key strings)))
(defn llen [client key] (->int client (.llen client key)))
(defn lrange [client key start end] (->list client (.lrange client key start end)))
(defn ltrim [client key start end] (->status client (.ltrim client key start end)))
(defn lindex [client key index] (->string client (.lindex client key index)))
(defn lset [client key index value] (->status client (.lset client key index value)))
(defn lrem [client key count value] (->int client (.lrem client key count value)))
(defn lpop [client key] (->string client (.lpop client key)))
(defn rpop [client key] (->string client (.rpop client key)))
(defn rpoplpush [client src-key dest-key] (->string client (.rpoplpush client src-key dest-key)))


(defn sadd [client key & members] (->int client (.sadd client key members)))
(defn smembers [client key] (->list>set client (.smembers client key)))
(defn srem [client key & members] (->int client (.srem client key members)))
(defn spop [client key] (->string client (.spop client key)))
(defn smove [client src-key dest-key member] (->int client (.smove client src-key dest-key member)))
(defn scard [client key] (->int client (.scard client key)))
(defn sismember [client key member] (->boolean client (.sismember client key member)))
(defn sinter [client & keys] (->list>set client (.sinter client keys)))
(defn sinterstore [client dest-key & keys] (->int client (.sinterstore client dest-key keys)))
(defn sunion [client & keys] (->list>set client (.sunion client keys)))
(defn sunionstore [client dest-key & keys] (->int client (.sunionstore client dest-key keys)))
(defn sdiff [client & keys] (->set client (.sdiff client keys)))
(defn sdiffstore [client dest-key & keys] (->int client (.sdiffstore dest-key keys)))
(defn srandmember
  ([client key] (->string client (.srandmember client key)))
  ([client key count] (->list client (.srandmember client key count))))


(defn zadd
  ([client key score member] (->int client (.zadd client key score member)))
  ([client key map] (->int client (.zadd client key map))))
(defn zrange [client key start end] (->list>lset client (.zrange client key start end)))
(defn zrem [client key & members] (->int client (.zrem client key members)))
(defn zincr-by [client key score member] (->double client (.zincrby client key score member)))
(defn zrank [client key member] (->int client (.zrank client key member)))
(defn zrevrank [client key member] (->int client (.zrevrank client key member)))
(defn zrevrange [client key start end] (->list>lset client (.zrevrange client key start end)))
(defn zrange-with-scores [client key start end] (->tupled-set client (.zrangeWithScores client key start end)))
(defn zrevrange-with-scores [client key start end] (->tupled-set client (.zrevrangeWithScores client key start end)))
(defn zcard [client key] (->int client (.zcard client key)))
(defn zscore [client key member] (->double client (.zscore client key member)))

(defn watch [client & keys] (->status-multi client (.watch client keys)))

(defmulti sort (fn [& args] [(nth args 2 false) (> (count args) 3)]))
(defmethod sort [false false] sort-simple [client key] (->list client (.sort client key)))
(defmethod sort [:SortingParams :false] sort-with-params [client key sort-params] (->list client (.sort client key #^SortParams sort-params)))
(defmethod sort [:String :false] sort-to-dest [client key dest-key] (->int client (.sort client key #^String dest-key)))
(defmethod sort [:SortingParams :true] sort-with-params-to-dest [client key sort-params dest-key] (->int client (.sort client key sort-params dest-key)))

(defmulti blpop (fn [client & args] (first args)))
(defmethod blpop :Integer
  blpop-timeout [client timeout & keys] (->blocking:list client (.blpop client (conj keys (str timeout)))))
(defmethod blpop :String
  blpop-timeout [client & keys] (->blocking:list client (.blpop client keys)))

(defn brpop [client & keys] (->blocking:list client (.brpop keys)))

(defmulti zcount (fn [client key min max] [client key min max]))
(defmethod zcount [:Object :String :Double :Double]
  zcount-doubles [client key min max]
  (->int client (.zcount client key #^Double min #^Double max)))
(defmethod zcount [:Object :String :String :String]
  zcount-strings [client key min max]
  (->int client (.zcount client key #^String min #^String max)))

(defmulti zrange-by-score (fn [client key min max & optional] [client key min max]))
(defmethod zrange-by-score [:Object :String :Double :Double]
  zrange-by-score-doubles [client key min max & optional]
  (if (empty? optional)
    (->list>lset client (.zrangeByScore client key #^Double min #^Double max))
    (let [offset (first optional)
          count (second optional)]
      (->list>lset client (.zrangeByScore client key #^Double min #^Double max offset count)))))
(defmethod zrange-by-score [:Object :String :String :String]
  zrange-by-score-strings [client key min max & optional]
  (if (empty? optional)
    (->list>lset client (.zrangeByScore client key #^String min #^String max))
    (let [offset (first optional)
          count (second optional)]
      (->list>lset client (.zrangeByScore client key #^String min #^String max offset count)))))

(defmulti zrange-by-score-with-scores (fn [client key min max & optional] [client key min max]))
(defmethod zrange-by-score-with-scores [:Object :String :Double :Double]
  zrange-by-score-with-score-doubles [client key min max & optional]
  (if (empty? optional)
    (->tupled-set client (.zrangeByScoreWithScores client key #^Double min #^Double max))
    (let [offset (first optional)
          count (second optional)]
      (->tupled-set client (.zrangeByScoreWithScores client key #^Double min #^Double max offset count)))))
(defmethod zrange-by-score-with-scores [:Object :String :String :String]
  zrange-by-score-with-score-strings [client key min max & optional]
  (if (empty? optional)
    (->tupled-set client (.zrangeByScoreWithScores client key #^String min #^String max))
    (let [offset (first optional)
          count (second optional)]
      (->tupled-set client (.zrangeByScoreWithScores client key #^String min #^String max offset count)))))

(defmulti zrevrange-by-score (fn [client key min max & optional] [client key min max]))
(defmethod zrevrange-by-score [:Object :String :Double :Double]
  zrevrange-by-score-doubles [client key min max & optional]
  (if (empty? optional)
    (->list>lset client (.zrevrangeByScore client key #^Double min #^Double max))
    (let [offset (first optional)
          count (second optional)]
      (->list>lset client (.zrevrangeByScore client key #^Double min #^Double max offset count)))))
(defmethod zrevrange-by-score [:Object :String :String :String]
  zrevrange-by-score-strings [client key min max & optional]
  (if (empty? optional)
    (->list>lset client (.zrevrangeByScore client key #^String min #^String max))
    (let [offset (first optional)
          count (second optional)]
      (->list>lset client (.zrevrangeByScore client key #^String min #^String max offset count)))))

(defmulti zrevrange-by-score-with-scores (fn [client key min max & optional] [client key min max]))
(defmethod zrevrange-by-score-with-scores [:Object :String :Double :Double]
  zrevrange-by-score-with-score-doubles [client key min max & optional]
  (if (empty? optional)
    (->tupled-set client (.zrevrangeByScoreWithScores client key #^Double min #^Double max))
    (let [offset (first optional)
          count (second optional)]
      (->tupled-set client (.zrevrangeByScoreWithScores client key #^Double min #^Double max offset count)))))
(defmethod zrevrange-by-score-with-scores [:Object :String :String :String]
  zrevrange-by-score-with-score-strings [client key min max & optional]
  (if (empty? optional)
    (->tupled-set client (.zrevrangeByScoreWithScores client key #^String min #^String max))
    (let [offset (first optional)
          count (second optional)]
      (->tupled-set client (.zrevrangeByScoreWithScores client key #^String min #^String max offset count)))))

(defn zremrange-by-rank [client key start end] (->int client (.zremrangeByRank key start end)))
(defn zremrange-by-score [client key start end] (->int client (.zremrangeByScore key start end)))

(defmulti zremrange-by-score (fn [client key start end] [client key start end]))
(defmethod zremrange-by-score [:Object :String :Double :Double]
  zremrange-by-score-doubles [client key start end]
  (->int client (.zremrangeByScore client key #^Double start #^Double end)))
(defmethod zremrange-by-score [:Object :String :String :String]
  zremrange-by-score-strings [client key start end]
  (->int client (.zremrangeByScore client key #^String start #^String end)))

(defmulti zunionstore (fn [client dest-key & args] (first args)))
(defmethod zunionstore :String
  zunionstore-normal [client dest-key & keys] (->int client (.zunionstore client dest-key keys)))
(defmethod zunionstore :redis.clients.jedis.ZParams
  zunionstore-with-params [client dest-key params & keys] (->int client (.zunionstore client dest-key params keys)))

(defmulti zinterstore (fn [client dest-key & args] (first args)))
(defmethod zinterstore :String
  zinterstore-normal [client dest-key & keys] (->int client (.zinterstore client dest-key keys)))
(defmethod zinterstore :redis.clients.jedis.ZParams
  zinterstore-with-params [client dest-key params & keys] (->int client (.zinterstore client dest-key params keys)))

(defn strlen [client key] (->string client (.strlen client key)))
(defn lpushx [client key & strings] (->int client (.lpushx client key strings)))
(defn persist [client key] (->int client (.persist client key)))
(defn rpushx [client key & strings] (->int (.rpushx client key strings)))
(defn echo [client string] (->string (.echo client string)))
(defn linsert [client key where pivot value] (->int (.linsert client key where pivot value)))
(defn brpoplpush [client source dest timeout] (->blocking:string client (.brpoplpush client source dest timeout)))

(defmulti setbit (fn [client offset value] value))
(defmethod setbit :Boolean setbit-bool [client offset value] (->boolean client (.setbit client key offset #^Boolean value)))
(defmethod setbit :String setbit-bool [client offset value] (->boolean client (.setbit client key offset #^String value)))

(defn getbit [client key offset] (->boolean client (.getbit client key offset)))

(defn setrange [client key offset value] (->int client (.setrange client key offset value)))
(defn getrange [client key start-offset end-offset] (->string client (.getrange client key start-offset end-offset)))

(defn config-get [client pattern] (->list client (.configGet client pattern)))
(defn config-set [client param val] (->status client (.configSet client param val)))

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

(defn eval-params [keys values] (into-array String (interleave keys values)))

(defmulti eval (fn [client script & optional] (if (nil? optional) false (first optional))))
(defmethod eval false [client script] (*eval client script 0))
(defmethod eval :Integer [& params] (apply *eval params))
(defmethod eval :List<String> [client script keys params] (*eval client script (count keys) (eval-params params)))

(defmulti evalsha (fn [client sha1 & optional] (if (nil? optional) false (first optional))))
(defmethod evalsha false [client sha1] (*evalsha client sha1 0))
(defmethod evalsha :Integer [& params] (apply *evalsha params))
(defmethod evalsha :List<String> [client sha1 keys params] (*evalsha client sha1 (count keys) (eval-params params)))

(defn scripts-exist? [client & sha1s]
  (let [c (chan)]
    (go
     (.scriptExists client (into-array String sha1s))
     (>! c (map (fn [i] (= i 1)) (.getIntegerMultiBulkReply client))))
    c))

(defn script-exists? [client sha1]
  (let [c (scripts-exist? client sha1)
        c2 (chan)]
    (go (let [replies (<! c)]
          (>! c2 (first replies))))
    c2))

(defn script-load [client script] (->string client (.scriptLoad client script)))

(defmulti slowlog-get (fn [client & args] (empty? args)))
(defmethod slowlog-get :false [client]
  (with-chan (fn []
               (.slowlogGet client)
               (Slowlog/from (.getObjectMultiBulkReply client)))))
(defmethod slowlog-get :true [client & entries]
  (with-chan (fn []
               (.slowlogGet client entries)
               (Slowlog/from (.getObjectMultiBulkReply client)))))

(defn object-refcount [client string] (->int client (.objectRefcount client string)))
(defn object-encoding [client string] (->string client (.objectEncoding client string)))
(defn object-idletime [client string] (->string client (.objectIdletime client string)))
(defn bitcount [client key start end] (->int client (.bitcount client key start end)))
(defn bitop [client op dest-key & src-keys] (->int client (.bitop op dest-key src-keys)))

(defn sentinel-masters [client]
  (with-chan (fn []
               (.sentinel client Protocol/SENTINEL_MASTERS)
               (map (fn [o] (.build BuilderFactory/STRING_MAP o)) (.getObjectMultiBulkReply client)))))

(defn sentinel-get-master-addr-by-name [client master-name]
  (with-chan (fn []
               (.sentinel client Protocol/SENTINEL_GET_MASTER_ADDR_BY_NAME master-name)
               (.build BuilderFactory/STRING_LIST (.getObjectMultiBulkReply client)))))

(defn sentinel-reset [client pattern] (->int client (.sentinel client Protocol/SENTINEL_RESET pattern)))
(defn sentinel-slaves [client master-name]
  (with-chan (fn []
               (.sentinel client Protocol/SENTINEL_SLAVES master-name)
               (map (fn [o] (.build BuilderFactory/STRING_MAP o)) (.getObjectMultiBulkReply client)))))

(defn dump [client key] (->bytes client (.dump client key)))
(defn restore [client key ttl serialized-value] (->status client (.restore client key ttl serialized-value)))
(defn pexpire [client key millis] (->int client (.pexpire client key millis)))
(defn pexpire-at [client key milli-ts] (->int client (.pexpireAt client key milli-ts)))
(defn pttl [client key] (->int client (.pttl client key)))
(defn incr-by-float [client key inc] (->double client (.incrByFloat client key inc)))
(defn psetex [client key millis val] (->status client (.psetex client key millis val)))

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

(defn client-kill [client client-name] (->status client (.clientKill client client-name)))
(defn client-setname [client name] (->status client (.clientSetname client name)))
(defn migrate [client host port key dest-db timeout] (->status client (.migrate host port key dest-db timeout)))
(defn hincr-by-float [client key field inc] (->double client (.hincrByFloat client key field inc)))

(defn subscribe [client jedis-pub-sub & channels]
  (with-chan (fn []
               (.setTimeoutInfinite client)
               (.proceed jedis-pub-sub client channels)
               (.rollbackTimeout client))))

(defn publish [client channel message] (->int client (.publish client channel message)))

(defn psubscribe [client jedis-pub-sub & patterns]
  (with-chan #(non-multi client
                         (.setTimeoutInfinite client)
                         (.proceedWithPatterns jedis-pub-sub client patterns)
                         (.rollbackTimeout client))))


(defmacro with [client & body]
  `(let [client# ~client]
     (binding [disconnect #(disconnect* client#)
               get #(get* client# %)
               set #(set* client# %1 %2)]
       ~@body)))
