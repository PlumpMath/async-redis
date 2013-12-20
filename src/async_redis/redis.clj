(ns async-redis.redis
  (:refer-clojure :exclude [get type keys set])
  (:import (java.net.URI)
           (redis.clients.jedis Client JedisPubSub)
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

(defmacro ->bulk [client & body]
  `(non-multi ~client
              ~@body
              (wrap (.getBulkReply ~client))))

(defmacro ->int [client & body]
  `(non-multi ~client
              ~@body
              (wrap (.getIntegerReply ~client))))

(defmacro ->boolean [client & body]
  `(non-multi ~client
              ~@body
              (wrap (= 1 (.getIntegerReply ~client)))))

(defmacro ->status [client & body]
  `(non-multi ~client
              ~@body
              (wrap (.getStatusCodeReply ~client))))

(defn connect [host port]
  (Client. (or host local-host) (or port default-port)))

(defn get [client key] (->bulk client (.get client key)))
(defn exists [client key] (->boolean client (.exists client key)))
(defn del [client & keys] (->int client (.del client keys)))
(defn type [client key] (->status client (.type client key)))
(defn keys [client pattern] (->status client (.type client key)))
(defn random-key [client] (->bulk client (.randomKey client)))
(defn rename [client old-key new-key] (->status client (.rename client old-key new-key)))
(defn renamenx [client old-key new-key] (->status client (.renamenx client old-key new-key)))
(defn expire [client key seconds] (->int client (.expire client key seconds)))
(defn expire-at [client key timestamp] (->int client (.expireAt client key timestamp)))
(defn ttl [client key] (->int client (.ttl client key)))
(defn move [client key db-index] (->int client (.move client key db-index)))
(defn getset [client key value] (->bulk client (.getSet client key value)))

(defn set [client key value] (->status client (.set client key value)))


