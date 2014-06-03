(defproject org.clojars.kiril/async-redis "0.1.1-SNAPSHOT"
  :description "a core.async wrapper for the Redis driver Jedis"
  :url "http://github.com/kiril/async-redis"
  :license {:name "MIT License"
            :url "http://opensource.org/licenses/MIT"}
  :dependencies [[org.clojure/clojure "1.5.1"]
                 [redis.clients/jedis "2.2.1"]
                 [org.clojure/core.async "0.1.267.0-0d7780-alpha"]]
  :aot :all)
