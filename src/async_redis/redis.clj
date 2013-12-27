(ns async-redis.redis
  (:refer-clojure :exclude [get type keys set sort])
  (:import (java.net.URI)
           (java.util HashSet LinkedHashSet)
           (redis.clients.jedis Client JedisPubSub BuilderFactory Tuple)
           (redis.clients.jedis.exceptions JedisDataException))
  (:require [clojure.core.async :as async :refer [go >! <! chan]]))

(def ^{:private true} local-host "127.0.0.1")
(def ^{:private true} default-port 6379)

(defn- wrap [val]
  (if (nil? val) false val))

(defn- with-chan [f]
  (let [c (chan)]
    (go (>! c (f)))
    c))

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

(defmacro ->int [client & body]
  `(with-chan #(non-multi ~client
                          ~@body
                          (wrap (.getIntegerReply ~client)))))

(defmacro ->boolean [client & body]
  `(with-chan #(non-multi ~client
                          ~@body
                          (= 1 (.getIntegerReply ~client)))))

(defmacro ->status [client & body]
  `(with-chan #(non-multi ~client
                          ~@body
                          (wrap (.getStatusCodeReply ~client)))))

(defmacro ->status-multi [client & body]
  `(with-chan (fn []
                ~@body
                (.getStatusCodeReply ~client))))

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
  (Client. (or host local-host) (or port default-port)))


(defn get [client key] (->string client (.get client key)))
(defn exists [client key] (->boolean client (.exists client key)))
(defn del [client & keys] (->int client (.del client keys)))
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
(defn sort [client key] (->list client (.sort client key)))

(defn blpop [client timeout & keys]
  `(with-chan #(non-multi client
                          (.blpop client (conj keys (str timeout)))
                          (.setTimeoutInfinite client)
                          (let [response (.getBulkReply client)]
                            (.rollbackTimeout client)
                            response))))

(defn set [client key value] (->status client (.set client key value)))


