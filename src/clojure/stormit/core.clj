(ns stormit.core
  (:refer-clojure :exclude [filter pop push peek])
  (:import [stormit BoltFilter SpoutFilter StreamItBolt StreamItSpout])
  (:require [backtype.storm.clojure :as stormclj])
  (:require [backtype.storm.thrift :as stormthrift])
  (:require [stormit.thrift :as thrift])
  (:use [backtype.storm config])
  (:use [clojure.tools.macro]))


; 1st Design of StormIt
; ~~~~~~~~~~~~~~~~~

; (stormit/filter int-source [] [[] -> ["int"]]
;   (init [max 1000])
;   (work {:push 1}
;     (let [i (rand-int max)]
;       (Thread/sleep 100)
;       (stormit/push [i]))))

; (stormit/filter incr-and-print [] [["int"] -> []]
;   (init [increment 2])
;   (work {:pop 1 :push 0 :peek 0}
;     (let [i (nth (stormit/pop) 0)]
;       (info (+ i increment)))))
;
; Above vs pure one-to-one mapping of StreamIt to Storm
; But when trying to figure out how to design the language to support time based
; sliding windows, I read the CQL paper (http://dl.acm.org/citation.cfm?id=1146463).
; This gives the idea of using something similar to range, slide and rows discussed
; in CQL paper  instead of using push, pop, peek counts which doesn't mean much in
; Storm based distributed real-time computing.
;
; 2nd Design of StormIt
; ~~~~~~~~~~~~~~~~~~~~~

; (stormit/filter incr-and-print [] [["int"] -> []]
;   (init [increment 2])
;   (work {:rows num_rows or :range seconds :slide num_slides}
;     ))
;
; In above work scope context contains the real computation. Map passed to the work
; macro as the first argument contains the work function configuration. ':rows' define
; the size of tuple based sliding window. ':range' defines the time span of the time
; based sliding window. We can use one or the other in work configuration but not both.
; ':slide' define how we move the sliding window. If ':range' is used ':slide' defaults
; to seconds, otherwise number of elements. ':range' value is alsways in seconds.
; If ':range' is 'now' that means we don't maintain a sliding window. If ':rows' is used
; and equal to 1, that also means we don't maintain sliding window.
;
; Then next problem is how we give access to the sliding window for the computations.


(defn streamit-bolt* [name prep-fn-var output-spec input-spec peek-cnt pop-cnt push-cnt args]
  {:type :bolt :name name :input-spec input-spec :output-spec output-spec :spout nil :bolt (StreamItBolt. (stormclj/to-spec prep-fn-var) args (stormthrift/mk-output-spec output-spec) (stormthrift/mk-output-spec input-spec) pop-cnt peek-cnt push-cnt)})

(defmacro streamit-bolt [name prep-fn-sym output-spec input-spec peek-cnt pop-cnt push-cnt args]
  `(streamit-bolt* (str ~name) (var ~prep-fn-sym) ~output-spec ~input-spec ~peek-cnt ~pop-cnt ~push-cnt ~args))

(defn streamit-spout* [name prep-fn-var output-spec push-cnt args]
  {:type :spout :name name :output-spec output-spec :input-spec nil :bolt nil :spout (StreamItSpout. (stormclj/to-spec prep-fn-var) args (stormthrift/mk-output-spec output-spec) push-cnt)})

(defmacro streamit-spout [name prep-fn-sym output-spec push-cnt args]
  `(streamit-spout* (str ~name) (var ~prep-fn-sym) ~output-spec ~push-cnt ~args))

(defmacro push [tuple]
  `(proxy-super ~'push ~tuple))

(defmacro pop []
  `(proxy-super ~'pop))

(defmacro peek [i]
  `(proxy-super ~'peek ~i))

(defmacro spout [& body]
  `(proxy [SpoutFilter] []
     ~@body))

(defmacro bolt [& body]                                     ;; We may need to look at how bolt works in original Storm DSL.
  `(proxy [BoltFilter] []
     ~@body))

(defmacro filter [name params type & body]
  (let [prepare-fn-name (symbol (str name "__prep"))
        n (str name)
        [input-spec _arrow_ output-spec] type
        [init work] body
        [i init-defs] init
        [w conf & impl-body] work
        fn-body (if (empty? input-spec)
                  (if (empty? init-defs)
                    `(fn [] (spout (~'nextTuple [] ~@impl-body)))
                    `(fn [] (let [~@init-defs] (spout (~'nextTuple [] ~@impl-body)))))
                  (if (empty? init-defs)
                    `(fn [] (bolt (~'execute [] ~@impl-body)))
                    `(fn [] (let [~@init-defs] (bolt (~'execute [] ~@impl-body))))))
        definer (if (empty? input-spec)
                  (if (empty? params)
                    `(def ~name
                       (streamit-spout ~n ~prepare-fn-name ~output-spec ~(:push conf) []))
                    `(defn ~name [& args#]
                       (streamit-spout ~n ~prepare-fn-name ~output-spec ~(:push conf) args#)))
                  (if (empty? params)
                    `(def ~name
                       (streamit-bolt ~n ~prepare-fn-name ~output-spec ~input-spec ~(:peek conf) ~(:pop conf) ~(:push conf) []))
                    `(defn ~name [& args#]
                       (streamit-bolt ~n ~prepare-fn-name ~output-spec ~input-spec ~(:peek conf) ~(:pop conf) ~(:push conf) args#))))]
    `(do
       (defn ~prepare-fn-name ~(if (empty? params) [] params)
         ~fn-body)
       ~definer)))

;; Pipeline supports extra arguments. But still not sure about how to support types.
;; Types will allow us to combine pipelines together. This means pipeline should not emit clojure topology directly.
;; Most simple way to test is have pipeline emit clojure topology.
;;
;; How about just using vector
(comment (def sample-pipeline
           [{:type :spout _ _} {:type :bolt _ _} {:type :split-join :split _ :join _ :bolt {:b :p} :or-bolt [:b :b :b _ _]}]))

;; TODO: Modify this to just use a def when params are empty
(defmacro pipeline [name params & body]
  (let [n (str name)]
    `(macrolet [(~'add [~'sf] `(~'~'swap! ~'pipeline# (fn [~'p#] (~'~'into ~'p# [~~'sf]))))]
               (~'defn ~name ~(if params params []) (~'let [pipeline# (~'atom [])]
                                                     ~@body
                                                     {:name (~'str ~n) :sapp (~'deref pipeline#)}))))) ;; This doesn't work due to immutability

(comment
  (ssplit-join bpf-core [rate low taps]
               (split :roundrobin)
               (add wrod-count)
               (add word-count)
               (add word-count)
               (join :field)))                              ;; How join happens. What is round robin join. How about field based join.

(comment (defmacro split-join [name params body]
           (let [n (str name)]
             )))

(defn local-cluster []
  ;; do this to avoid a cyclic dependency of
  ;; LocalCluster -> testing -> nimbus -> bootstrap -> clojure -> LocalCluster
  (eval '(new backtype.storm.LocalCluster)))

(defn run-local! [topology id]
  (let [cluster (local-cluster)]
    (.submitTopology cluster id {TOPOLOGY-DEBUG true} topology)
    (Thread/sleep 30000)
    (.shutdown cluster)))

(defn make-topology [app]
  (thrift/mk-topology (:sapp app)))

(defn submit-local-stormit-app [app]
  (let [topology (thrift/mk-topology (:sapp app))]
    (run-local! topology (:name app))))

(defn submit-remote-stormit-app [app])
