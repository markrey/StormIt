(ns stormit.examples.simple
  (:use [stormit.core])
  (:use [clojure.tools.logging :only (info error)]))

(sfilter int-source [] [[] -> ["int"]]
         (init [max 1000])
         (work {:push 1}
               (let [i (rand-int max)]
                 (Thread/sleep 100)
                 (spush [i]))))

(sfilter incr-and-print [] [["int"] -> []]
         (init [increment 2])
         (work {:pop 1 :push 0 :peek 0}
               (let [i (.getInteger (spop) 0)]
                 (info (+ i increment)))))

(spipeline simple-app []
           (add int-source)
           (add incr-and-print))

(defn submit-app []
  (submit-local-stormit-app (simple-app)))
(defn -main []
  (submit-app))
