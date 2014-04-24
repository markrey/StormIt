(ns stormit.examples.simple
  (:use [stormit.core]))

(sfilter int-source [] [[] -> ["int"]]
         (init [max 1000])
         (work {:push 1}
               (let [i (rand-int max)]
                 (Thread/sleep 100)
                 (spush [i]))))

(sfilter incr-and-print [] [["int"] -> []]
         (init [increment 2])
         (work {:pop 1 :push 0 :peek 0}
               (let [i (spop)]
                 (print (+ i increment)))))

(spipeline simple-app []
           (add int-source)
           (add incr-and-print))


(defn -main []
  (submit-local-stormit-app (simple-app)))
