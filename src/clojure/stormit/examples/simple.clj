(ns stormit.examples.simple
  (:require [stormit.core :as stormit])
  (:use [clojure.tools.logging :only (info error)]))

(stormit/filter int-source [] [[] -> ["int"]]
         (init [max 1000])
         (work {:push 1}
               (let [i (rand-int max)]
                 (Thread/sleep 100)
                 (stormit/push [i]))))

(stormit/filter incr-and-print [] [["int"] -> []]
         (init [increment 2])
         (work {:pop 1 :push 0 :peek 0}
               (let [i (nth (stormit/pop) 0)]
                 (info (+ i increment)))))

(stormit/pipeline simple-app []
           (add int-source)
           (add incr-and-print))

(defn submit-app []
  (stormit/submit-local-stormit-app (simple-app)))
(defn -main []
  (submit-app))
