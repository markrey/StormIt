(ns stormit.thrift
  (:import [java.util HashMap])
  (:import [backtype.storm.generated JavaObject Grouping Nimbus StormTopology StormTopology$_Fields
            Bolt Nimbus$Client Nimbus$Iface ComponentCommon Grouping$_Fields SpoutSpec NullStruct StreamInfo
            GlobalStreamId ComponentObject ComponentObject$_Fields ShellComponent])
  (:import [backtype.storm.utils Utils])
  (:import [backtype.storm Constants])
  (:import [backtype.storm.grouping CustomStreamGrouping])
  (:import [backtype.storm.topology TopologyBuilder])
  (:import [backtype.storm.clojure RichShellBolt RichShellSpout])
  (:import [org.apache.thrift.protocol TBinaryProtocol TProtocol])
  (:import [org.apache.thrift.transport TTransport TFramedTransport TSocket]))


(defn is-spout? [f]
  (identical? (:type f) :spout))

(defn is-bolt? [f]
  (identical? (:type f) :bolt))

(defn is-splitjoin? [f]
  (identical? (:type f) :splitjoin))

(comment  (def sample-pipeline
            [{:type :spout _ _} {:type :bolt _ _} {:type :split-join :split _ :join _ :bolt {:b :p} :or-bolt [:b :b :b _ _]}]))

;; From Storm Clojure DSL
(defn mk-shuffle-grouping []
  (Grouping/shuffle (NullStruct.)))

(defn mk-local-or-shuffle-grouping []
  (Grouping/local_or_shuffle (NullStruct.)))

(defn mk-fields-grouping [fields]
  (Grouping/fields fields))

(defn mk-global-grouping []
  (mk-fields-grouping []))

(defn mk-direct-grouping []
  (Grouping/direct (NullStruct.)))

(defn mk-all-grouping []
  (Grouping/all (NullStruct.)))

(defn mk-none-grouping []
  (Grouping/none (NullStruct.)))

(defn- route-pipeline [declarer prev-filter]
  (let [name (:name prev-filter)]
    (.grouping declarer (GlobalStreamId. name Utils/DEFAULT_STREAM_ID) (mk-local-or-shuffle-grouping))))

;; No error handling as of now.
(defn mk-topology [prg]
  (if (identical? (:type (first prg)) :spout)
    (let [builder (TopologyBuilder.)]
      (loop [p (rest prg)
             curr-f (first prg)
             prev-f nil]
        (if curr-f
          (cond
           (is-spout? curr-f) (let [spout (:spout curr-f)
                                    name (:name curr-f)]
                                (-> builder (.setSpout name spout nil) (.addConfigurations {})))
           (is-bolt? curr-f) (let [bolt (:bolt curr-f)
                                   name (:name curr-f)
                                   inputs (:input-spec curr-f)]
                               (-> builder (.setBolt name bolt nil) (.addConfigurations {}) (route-pipeline prev-f)))
           (is-splitjoin? curr-f) (let [])
           :else (throw (RuntimeException. (str "Invalid filter type " curr-f "!"))))
          (recur (rest p)
                 (first p)
                 curr-f)))
      (.createTopology builder))
    (throw (RuntimeException. "Invalid StormIt application. There should be a source filter at the begining!"))))
