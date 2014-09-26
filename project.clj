(defproject stormit "0.1.0-SNAPSHOT"
  :description "StormIt is a Clojure DSL for Apache Storm which allows programming Apache Storm topologies using StreamIt like constructs"
  :url "http://github.com/milinda/StormIt"
  :license {:name "Apache License, Version 2.0"
            :url "http://www.apache.org/licenses/LICENSE-2.0"}
  :source-paths      ["src/clojure"]
  :java-source-paths ["src/java"]
  :dependencies [[org.clojure/clojure "1.5.1"]
                 [org.apache.storm/storm-core "0.9.2-incubating"]
                 [org.clojure/tools.macro "0.1.2"]
                 [org.apache.thrift/libthrift "0.7.0" :exclusions [org.slf4j/slf4j-api]]]
  :target-path "target/%s"
  :main stormit.examples.simple
  :profiles {:uberjar {:aot :all}})
