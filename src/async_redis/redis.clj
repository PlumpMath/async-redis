(ns async-redis.redis
  (:refer-clojure :exclude [get type keys set sort eval])
  (:import (java.net.URI)
           (java.util HashSet HashMap LinkedHashSet)
           (redis.clients.jedis Client BinaryClient JedisPubSub BuilderFactory
                                Tuple SortingParams Protocol JedisPool)
           (redis.clients.util SafeEncoder Slowlog)
           (redis.clients.jedis.exceptions JedisDataException))
  (:require [clojure.core.async :as async :refer [go >! <! <!! >!! chan go-loop]]))


(def ^{:private true} local-host "127.0.0.1")
(def ^{:private true} default-port 6379)
(def ^{:private true} default-db 1)

(def ^{:private true} *host (ref local-host))
(def ^{:private true} *port (ref default-port))
(def ^{:private true} *db (ref default-db))
(def ^{:private true} *pool (ref (chan)))

(def ^:dynamic ^{:private true} *db-override false)

(defn ^{:private true} ensure-db-selected [conn]
  (let [db-number (or *db-override (deref *db))]
    (when (not (= (.getDB conn) db-number))
      (.select conn db-number)
      (.getBinaryBulkReply conn)))
  conn)

(defmacro on-db [db & body]
  `(binding [*db-override ~db]
     ~@body))

(defn connect []
  (let [conn (Client. (deref *host) (deref *port))]
    (.connect conn)
    (ensure-db-selected conn)))

(defn fill-client-channel [channel num-connections]
  (go-loop [n num-connections]
           (when (> n 0)
             (>! channel (connect))
             (recur (- n 1)))))

(defn configure
  ([] (fill-client-channel (deref *pool) 5))
  ([host port &{:keys [db] :or {db default-db}}]
     (dosync (ref-set *host host)
             (ref-set *port port)
             (ref-set *db db)
             (ref-set *pool (chan 10))
             (fill-client-channel (deref *pool) 5))))

(defn <conn [] (ensure-db-selected (<!! (deref *pool))))
(defn >conn [conn] (>!! (deref *pool) conn))

(defmacro w-client
  [& body]
  `(let [~'client (<conn)]
     (++client ~'client)
     (let [ret# ((fn [] ~@body))]
       (--client ~'client)
       ret#)))

(defmacro defr
  "Creates a function which checks out a connection to use for the scope of the function"
  [fn-name & args]
  (if (vector? (first args))
    (let [fn-args (first args) body (rest args)]
       `(defn ~fn-name ~fn-args (w-client ~@body)))
    (if (= 2 (count args))
      (let [first-form (first args)
            second-form (second args)
            first-args (first first-form)
            first-body (rest first-form)
            second-args (first second-form)
            second-body (rest second-form)]
        `(defn ~fn-name
           (~first-args (w-client ~@first-body))
           (~second-args (w-client ~@second-body))))
      `(didnt-implement-this-case))))

(defmacro defmethod-r [base-name
                       function-pattern
                       specific-name
                       args
                       & body]
  `(defmethod ~base-name ~function-pattern
     ~specific-name ~args
     (w-client ~@body)))

;; channels can't take null values, so we're going to proxy with false

(defn wrap [val]
  (if (nil? val) false val))


;; the trick for putting results of operations onto channels

(def ^{:private true} *client-refcounts (HashMap.))

(defn ^{:private true} ++client
  [client]
  (dosync
   (.put *client-refcounts client (+ 1 (or (.get *client-refcounts client) 0)))))

(defn ^{:private true} --client
  [client]
  (dosync
   (let [current-count (or (.get *client-refcounts client) 0)]
     (if (= 1 current-count)
       (let [] (.remove *client-refcounts client) (>conn client))
       (.put *client-refcounts client (- current-count 1))))))

(defn with-chan [client f]
  (when client (++client client))
  (let [result-chan (chan 1)]
    (go (>! result-chan (f))
        (when client (--client client)))
    result-chan))

(defn get-status-code-reply [client]
  (let [bytes (.getBinaryBulkReply client)]
    (cond (nil? bytes) nil
          :else (SafeEncoder/encode bytes))))

(defn check-multi [client]
  (when (.isInMulti client)
    (throw (JedisDataException. "Can only use transactions when in multi mode"))))

(defmacro non-multi [client & body]
  `(let []
     (check-multi ~client)
     ~@body))

(defmacro ->string [client & body]
  `(let [client# ~client]
     (with-chan client#
                #(non-multi client#
                            ~@body
                            (wrap (.getBulkReply client#))))))

(defmacro ->string>int [client & body]
  `(let [client# ~client]
     (with-chan client#
                #(non-multi client#
                            ~@body
                            (let [ret# (.getBulkReply client#)]
                              (if ret# (Integer/valueOf ret#) 0))))))

(defmacro ->double [client & body]
  `(let [client# ~client]
     (with-chan client#
                #(non-multi client#
                            ~@body
                            (let [ret# (.getBulkReply client#)]
                              (if (nil? ret#) ret# (Double/valueOf ret#)))))))

(defmacro ->list [client & body]
  `(let [client# ~client]
     (with-chan client#
                #(non-multi client#
                            ~@body
                            (wrap (.getMultiBulkReply client#))))))

(defmacro ->blocking:list [client & body]
  `(let [client# ~client]
     (with-chan client#
                #(non-multi client#
                            ~@body
                            (.setTimeoutInfinite client#)
                            (let [response# (wrap (.getMultiBulkReply client#))]
                              (.rollbackTimeout client#)
                              response#)))))

(defmacro ->blocking:string [client & body]
  `(let [client# ~client]
     (with-chan client#
                #(non-multi client#
                            ~@body
                            (.setTimeoutInfinite client#)
                            (let [response# (wrap (.getBulkReply client#))]
                              (.rollbackTimeout client#)
                              response#)))))

(defmacro ->int [client & body]
  `(let [client# ~client]
     (with-chan client#
                #(non-multi client#
                            ~@body
                            (wrap (.getIntegerReply client#))))))

(defmacro ->boolean [client & body]
  `(let [client# ~client]
     (with-chan client#
                #(non-multi client#
                            ~@body
                            (= 1 (.getIntegerReply client#))))))

(defmacro ->bytes [client & body]
  `(let [client# ~client]
     (with-chan client#
                #(non-multi client#
                            ~@body
                            (.getBinaryBulkReply client#)))))

(defmacro ->status [client & body]
  `(let [client# ~client]
     (with-chan client#
                #(non-multi client#
                            ~@body
                            (wrap (get-status-code-reply client#))))))

(defmacro ->status-multi [client & body]
  `(let [client# ~client]
     (with-chan client#
                (fn []
                  ~@body
                  (get-status-code-reply client#)))))

(defmacro ->set [client & body]
  `(let [client# ~client]
     (with-chan client#
                #(non-multi client#
                            ~@body
                            (.build BuilderFactory/STRING_SET (.getBinaryMultiBulkReply client#))))))

(defmacro ->map [client & body]
  `(let [client# ~client]
     (with-chan client#
                #(non-multi client#
                            ~@body
                            (.build BuilderFactory/STRING_MAP (.getBinaryMultiBulkReply client#))))))

(defmacro ->list>set [client & body]
  `(let [client# ~client]
     (with-chan client#
                #(non-multi client#
                            ~@body
                            (HashSet. (.getMultiBulkReply client#))))))

(defmacro ->list>lset [client & body]
  `(let [client# ~client]
     (with-chan client#
                #(non-multi client#
                            ~@body
                            (LinkedHashSet. (.getMultiBulkReply client#))))))

(defmacro ->tupled-set [client & body]
  `(let [client# ~client]
     (with-chan client#
                #(non-multi client#
                            ~@body
                            (let [members-with-scores# (.getMultiBulkReply client#)
                                  iterator# (.iterator members-with-scores#)
                                  tuple-set# (LinkedHashSet.)]
                              (while (.hasNext iterator#)
                                     (let [key# (.next iterator#)
                                           value# (.next iterator#)]
                                     (.add tuple-set# (Tuple. key# value#))))
                              tuple-set#)))))


(defn disconnect [client] (.disconnect client))


;; a bit custom, because .getDB is actually just a simple blocking call.
(defr db []
  (let [result-chan (chan 1)
        db (.getDB client)]
    (>!! result-chan db)
    result-chan))

(defr flush-db! [] (->status client (.flushDB client)))

(defr exists? [key] (->boolean client (.exists client key)))
(defr del! [& keys] (->int client (.del client (into-array String keys))))
(defr type [key] (->status client (.type client key)))
(defr keys [pattern] (->list client (.keys client pattern)))
(defr random-key [] (->string client (.randomKey client)))
(defr rename! [old-key new-key] (->status client (.rename client old-key new-key)))
(defr renamenx! [old-key new-key] (->int client (.renamenx client old-key new-key)))

(defr get [key] (->string client (.get client key)))
(defr get-int [key] (->string>int client (.get client key)))
(defr expire! [key seconds] (->int client (.expire client key seconds)))
(defr expire-at! [key timestamp] (->int client (.expireAt client key timestamp)))
(defr ttl [key] (->int client (.ttl client key)))
(defr setex! [key seconds value] (->status client (.setex client key seconds value)))

(defr move! [key db-index] (->int client (.move client key db-index)))

(defr getset! [key value] (->string client (.getSet client key value)))

(defr mget [& keys] (->list client (.mget client (into-array String keys))))

(defr setnx! [key value] (->int client (.setnx client key value)))

(defr mset! [& keysvalues] (->status client (.mset client (into-array String keysvalues))))
(defr msetnx! [& keysvalues] (->int client (.msetnx client (into-array String keysvalues))))

(defr decr-by! [key inc] (->int client (.decrBy client key inc)))
(defr decr! [key] (->int client (.decr client key)))
(defr incr-by! [key inc] (->int client (.incrBy client key inc)))
(defr incr! [key] (->int client (.incr client key)))

(defr append! [key value] (->int client (.append client key value)))
(defr substr [key start end] (->string client (.substr client key start end)))

(defr hset! [key field value] (->int client (.hset client key field value)))
(defr hget [key field] (->string client (.hget client key field)))
(defr hget-int [key field] (->string>int client (.hget client key field)))
(defr hsetnx! [key field value] (->int client (.hsetnx client key field value)))
(defr hexists? [key field] (->boolean client (.hexists client key field)))
(defr hdel! [key & fields] (->int client (.hdel client key (into-array String fields))))
(defr hlen [key] (->int client (.hlen client key)))

(defr hincr-by! [key field inc] (->int client (.hincrBy client key field inc)))
(defr hincr-by-float! [key field inc] (->double client (.hincrByFloat client key field inc)))

(defr hmset! [key hash] (->status client (.hmset client key hash)))
(defr hmget [key & fields] (->list client (.hmget client key (into-array String fields))))
(defr hkeys [key] (->set client (.hkeys client key)))
(defr hvals [key] (->list client (.hvals client key)))
(defr hgetall [key] (->map client (.hgetAll client key)))

(defr rpush! [key & strings] (->int client (.rpush client key (into-array String strings))))
(defr lpush! [key & strings] (->int client (.lpush client key (into-array String strings))))
(defr llen [key] (->int client (.llen client key)))
(defr lrange [key start end] (->list client (.lrange client key start end)))
(defr ltrim! [key start end] (->status client (.ltrim client key start end)))
(defr lindex [key index] (->string client (.lindex client key index)))
(defr lset! [key index value] (->status client (.lset client key index value)))
(defr lrem! [key count value] (->int client (.lrem client key count value)))
(defr lpop! [key] (->string client (.lpop client key)))
(defr rpop! [key] (->string client (.rpop client key)))
(defr rpoplpush! [src-key dest-key] (->string client (.rpoplpush client src-key dest-key)))

(defr sadd! [key & members] (->int client (.sadd client key (into-array String members))))
(defr smembers [key] (->list>set client (.smembers client key)))
(defr srem! [key & members] (->int client (.srem client key (into-array String members))))
(defr spop! [key] (->string client (.spop client key)))
(defr smove! [src-key dest-key member] (->int client (.smove client src-key dest-key member)))
(defr scard [key] (->int client (.scard client key)))
(defr sismember? [key member] (->boolean client (.sismember client key member)))
(defr sinter [& keys] (->list>set client (.sinter client (into-array String keys))))
(defr sinterstore! [dest-key & keys] (->int client (.sinterstore client dest-key (into-array String keys))))
(defr sunion [& keys] (->list>set client (.sunion client (into-array String keys))))
(defr sunionstore! [dest-key & keys] (->int client (.sunionstore client dest-key (into-array String keys))))
(defr sdiff [& keys] (->set client (.sdiff client (into-array String keys))))
(defr sdiffstore! [dest-key & keys] (->int client (.sdiffstore client dest-key (into-array String keys))))
(defr srandmember
  ([key] (->string client (.srandmember client key)))
  ([key count] (->list client (.srandmember client key count))))

(defr zadd!
  ([key score member] (->int client (.zadd client key score member)))
  ([key map] (->int client (.zadd client key map))))

(defr zrange [key start end] (->list>lset client (.zrange client key start end)))
(defr zrem! [key & members] (->int client (.zrem client key members)))
(defr zincr-by! [key score member] (->double client (.zincrby client key score member)))
(defr zrank [key member] (->int client (.zrank client key member)))
(defr zrevrank [key member] (->int client (.zrevrank client key member)))
(defr zrevrange [key start end] (->list>lset client (.zrevrange client key start end)))
(defr zrange-with-scores [key start end] (->tupled-set client (.zrangeWithScores client key start end)))
(defr zrevrange-with-scores [key start end] (->tupled-set client (.zrevrangeWithScores client key start end)))
(defr zcard [key] (->int client (.zcard client key)))
(defr zscore [key member] (->double client (.zscore client key member)))
(defr watch [& keys] (->status-multi client (.watch client keys)))

(defr sort [key] (->list client (.sort client key)))
(defr sort-with-params [key sort-params](->list client (.sort client key #^SortParams sort-params)))
(defr sort-into-dest! [key dest-key] (->int client (.sort client key #^String dest-key)))
(defr sort-with-params-into-dest [key sort-params dest-key] (->int client (.sort client key sort-params dest-key)))

(defr blpop! [& keys] (->blocking:list client (.blpop client keys)))
(defr timeout-blpop! [timeout & keys] (->blocking:list client (.blpop client (conj keys (str timeout)))))

(defr brpop! [& keys] (->blocking:list client (.brpop keys)))

(defmulti zcount (fn [key min max] [key min max]))
(defmethod-r zcount [:String :Double :Double]
  zcount-doubles [client key min max]
  (->int client (.zcount client key #^Double min #^Double max)))
(defmethod-r zcount [:String :String :String]
  zcount-strings [client key min max]
  (->int client (.zcount client key #^String min #^String max)))

(defmulti zrange-by-score (fn [key min max & optional] [key min max]))
(defmethod-r zrange-by-score [:String :Double :Double]
  zrange-by-score-doubles
  [key min max & optional]
  (if (empty? optional)
    (->list>lset client (.zrangeByScore client key #^Double min #^Double max))
    (let [offset (first optional)
          count (second optional)]
      (->list>lset client (.zrangeByScore client key #^Double min #^Double max offset count)))))
(defmethod-r zrange-by-score [:String :String :String]
  zrange-by-score-strings
  [key min max & optional]
  (if (empty? optional)
    (->list>lset client (.zrangeByScore client key #^String min #^String max))
    (let [offset (first optional)
          count (second optional)]
      (->list>lset client (.zrangeByScore client key #^String min #^String max offset count)))))

(defmulti zrange-by-score-with-scores (fn [key min max & optional] [key min max]))
(defmethod-r zrange-by-score-with-scores
  [:String :Double :Double]
  zrange-by-score-with-score-doubles
  [key min max & optional]
  (if (empty? optional)
    (->tupled-set client (.zrangeByScoreWithScores client key #^Double min #^Double max))
    (let [offset (first optional)
          count (second optional)]
      (->tupled-set client (.zrangeByScoreWithScores client key #^Double min #^Double max offset count)))))
(defmethod-r zrange-by-score-with-scores
  [:String :String :String]
  zrange-by-score-with-score-strings
  [key min max & optional]
  (if (empty? optional)
    (->tupled-set client (.zrangeByScoreWithScores client key #^String min #^String max))
    (let [offset (first optional)
          count (second optional)]
      (->tupled-set client (.zrangeByScoreWithScores client key #^String min #^String max offset count)))))

(defmulti zrevrange-by-score (fn [key min max & optional] [key min max]))
(defmethod-r zrevrange-by-score
  [:String :Double :Double]
  zrevrange-by-score-doubles
  [key min max & optional]
  (if (empty? optional)
    (->list>lset client (.zrevrangeByScore client key #^Double min #^Double max))
    (let [offset (first optional)
          count (second optional)]
      (->list>lset client (.zrevrangeByScore client key #^Double min #^Double max offset count)))))
(defmethod-r zrevrange-by-score
  [:String :String :String]
  zrevrange-by-score-strings
  [key min max & optional]
  (if (empty? optional)
    (->list>lset client (.zrevrangeByScore client key #^String min #^String max))
    (let [offset (first optional)
          count (second optional)]
      (->list>lset client (.zrevrangeByScore client key #^String min #^String max offset count)))))

(defmulti zrevrange-by-score-with-scores (fn [key min max & optional] [key min max]))
(defmethod-r zrevrange-by-score-with-scores
  [:String :Double :Double]
  zrevrange-by-score-with-score-doubles
  [key min max & optional]
  (if (empty? optional)
    (->tupled-set client (.zrevrangeByScoreWithScores client key #^Double min #^Double max))
    (let [offset (first optional)
          count (second optional)]
      (->tupled-set client (.zrevrangeByScoreWithScores client key #^Double min #^Double max offset count)))))
(defmethod-r zrevrange-by-score-with-scores
  [:String :String :String]
  zrevrange-by-score-with-score-strings
  [key min max & optional]
  (if (empty? optional)
    (->tupled-set client (.zrevrangeByScoreWithScores client key #^String min #^String max))
    (let [offset (first optional)
          count (second optional)]
      (->tupled-set client (.zrevrangeByScoreWithScores client key #^String min #^String max offset count)))))

(defr zremrange-by-rank [key start end] (->int client (.zremrangeByRank key start end)))

(defmulti zremrange-by-score (fn [key start end] [key start end]))
(defmethod-r zremrange-by-score
  [:String :Double :Double]
  zremrange-by-score-doubles
  [key start end]
  (->int client (.zremrangeByScore client key #^Double start #^Double end)))
(defmethod-r zremrange-by-score
  [:String :String :String]
  zremrange-by-score-strings
  [key start end]
  (->int client (.zremrangeByScore client key #^String start #^String end)))

(defmulti zunionstore (fn [dest-key & args] (first args)))
(defmethod-r zunionstore
  :String
  zunionstore-normal
  [dest-key & keys]
  (->int client (.zunionstore client dest-key keys)))
(defmethod-r zunionstore
  :redis.clients.jedis.ZParams
  zunionstore-with-params
  [dest-key params & keys] (->int client (.zunionstore client dest-key params keys)))

(defmulti zinterstore (fn [dest-key & args] (first args)))
(defmethod-r zinterstore
  :String
  zinterstore-normal
  [dest-key & keys]
  (->int client (.zinterstore client dest-key keys)))
(defmethod-r zinterstore
  :redis.clients.jedis.ZParams
  zinterstore-with-params
  [dest-key params & keys]
  (->int client (.zinterstore client dest-key params keys)))

(defr strlen [key] (->string client (.strlen client key)))
(defr lpushx! [key & strings] (->int client (.lpushx client key strings)))
(defr persist! [key] (->int client (.persist client key)))
(defr rpushx! [key & strings] (->int (.rpushx client key strings)))
(defr echo [string] (->string (.echo client string)))
(defr linsert! [key where pivot value] (->int (.linsert client key where pivot value)))
(defr brpoplpush! [source dest timeout] (->blocking:string client (.brpoplpush client source dest timeout)))


(defmulti setbit! (fn [client offset value] value))
(defmethod-r setbit!
  :Boolean
  setbit-bool
  [offset value]
  (->boolean client (.setbit client key offset #^Boolean value)))
(defmethod-r setbit!
  :String
  setbit-bool
  [offset value]
  (->boolean client (.setbit client key offset #^String value)))

(defr getbit [key offset] (->boolean client (.getbit client key offset)))
(defr setrange! [key offset value] (->int client (.setrange client key offset value)))
(defr getrange [key start-offset end-offset] (->string client (.getrange client key start-offset end-offset)))
(defr config-get [pattern] (->list client (.configGet client pattern)))
(defr config-set! [param val] (->status client (.configSet client param val)))

(defn ^{:private true} get-eval-result [client]
  (let [result (.getOne client)]
        (wrap (cond (isa? result byte[]) (SafeEncoder/encode result)
                    (seq? result) (map (fn [x] (if (nil? x) x (SafeEncoder/encode x)))
                                       result)
                    :else result))))

(defn ^{:private true} *eval [client script key-count & params]
     (with-chan client
                (fn []
                  (.setTimeoutInfinite client)
                  (.eval client script key-count params)
                  (let [result (get-eval-result client)]
                    (.rollbackTimeout client)
                    result))))

(defn ^{:private true} *evalsha [client sha1 key-count & params]
     (with-chan client
                (fn []
                  (.setTimeoutInfinite client)
                  (.evalsha client sha1 key-count params)
                  (let [result (get-eval-result client)]
                    (.rollbackTimeout client)
                    result))))

(defn eval-params [keys values] (into-array String (interleave keys values)))

(defmulti eval (fn [script & optional] (if (nil? optional) false (first optional))))
(defmethod-r eval false eval-1 [script] (eval client script 0))
(defmethod-r eval :Integer eval-2 [& params] (apply eval params))
(defmethod-r eval :List<String> eval-3 [script keys params] (*eval client script (count keys) (eval-params params)))


(defmulti evalsha (fn [sha1 & optional] (if (nil? optional) false (first optional))))
(defmethod-r evalsha false evalsha-1 [sha1] (evalsha client sha1 0))
(defmethod-r evalsha :Integer evalsha-2 [& params] (apply evalsha params))
(defmethod-r evalsha :List<String> evalsha-3 [sha1 keys params] (evalsha sha1 (count keys) (eval-params params)))

(defr scripts-exist? [& sha1s]
  (let [c (chan)]
    (go
     (w-client
      (.scriptExists client (into-array String sha1s))
      (>! c (map (fn [i] (= i 1)) (.getIntegerMultiBulkReply client)))))
     c))

(defr script-exists? [sha1]
  (let [c (scripts-exist? client sha1)
        c2 (chan)]
    (go (with-chan
         (let [replies (<! c)]
           (>! c2 (first replies)))))
        c2))

(defr script-load [script] (->string client (.scriptLoad client script)))

(defmulti slowlog-get (fn [& args] (empty? args)))
(defmethod-r slowlog-get :true slowlog-empty []
  (with-chan client
             (fn []
               (.slowlogGet client)
               (Slowlog/from (.getObjectMultiBulkReply client)))))
(defmethod-r slowlog-get :false slowlog-entries [& entries]
  (with-chan client
             (fn []
               (.slowlogGet client entries)
               (Slowlog/from (.getObjectMultiBulkReply client)))))

(defr object-refcount [string] (->int client (.objectRefcount client string)))
(defr object-encoding [string] (->string client (.objectEncoding client string)))
(defr object-idletime [string] (->string client (.objectIdletime client string)))
(defr bitcount [key start end] (->int client (.bitcount client key start end)))
(defr bitop! [op dest-key & src-keys] (->int client (.bitop op dest-key src-keys)))

(defr sentinel-masters []
  (with-chan client
             (fn []
               (.sentinel client Protocol/SENTINEL_MASTERS)
               (map (fn [o] (.build BuilderFactory/STRING_MAP o)) (.getObjectMultiBulkReply client)))))

(defr sentinel-get-master-addr-by-name [master-name]
  (with-chan client
             (fn []
               (.sentinel client Protocol/SENTINEL_GET_MASTER_ADDR_BY_NAME master-name)
               (.build BuilderFactory/STRING_LIST (.getObjectMultiBulkReply client)))))

(defr sentinel-reset! [pattern] (->int client (.sentinel client Protocol/SENTINEL_RESET pattern)))

(defr sentinel-slaves [master-name]
  (with-chan client
             (fn []
               (.sentinel client Protocol/SENTINEL_SLAVES master-name)
               (map (fn [o] (.build BuilderFactory/STRING_MAP o)) (.getObjectMultiBulkReply client)))))

(defr dump [key] (->bytes client (.dump client key)))
(defr restore! [key ttl serialized-value] (->status client (.restore client key ttl serialized-value)))
(defr pexpire! [key millis] (->int client (.pexpire client key millis)))
(defr pexpire-at! [key milli-ts] (->int client (.pexpireAt client key milli-ts)))
(defr pttl [key] (->int client (.pttl client key)))
(defr incr-by-float! [key inc] (->double client (.incrByFloat client key inc)))
(defr psetex! [key millis val] (->status client (.psetex client key millis val)))


;; key , value (->status (.set key val))
;; key, val, nxx
;; key, value, nxx, expr, (long) time
;; key, value, nxx, expr, (int) time
(defmulti set! (fn [& args] [(count args) (nth args 4 false)]))
(defmethod-r set! [2 false] set-simple [key val] (->status client (.set client key val)))
(defmethod-r set! [3 false] set-nxxx [key val nxxx] (->status client (.set client key val nxxx)))
(defmethod-r set! [4 :Long] set-time-long [key val expr time] (->status client (.set client key val expr #^Long time)))
(defmethod-r set! [4 :Integer] set-time-int [key val expr time] (->status client (.set client key val expr #^Integer time)))

(defr client-kill! [client-name] (->status client (.clientKill client client-name)))
(defr client-setname! [name] (->status client (.clientSetname client name)))
(defr migrate! [host port key dest-db timeout] (->status client (.migrate host port key dest-db timeout)))

;; note: deliberately not doing special connection handling, as pubsub is special
(defn subscribe [client jedis-pub-sub & channels]
  (with-chan client
             (fn []
               (.setTimeoutInfinite client)
               (.proceed jedis-pub-sub client channels)
               (.rollbackTimeout client))))

(defn publish [client channel message] (->int client (.publish client channel message)))

(defn psubscribe [client jedis-pub-sub & patterns]
  (with-chan client
             #(non-multi client
                         (.setTimeoutInfinite client)
                         (.proceedWithPatterns jedis-pub-sub client patterns)
                         (.rollbackTimeout client))))

(defmacro just [statement] `(<!! ~statement))


