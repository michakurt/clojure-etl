;  [{:a 1 :b 2 :c 3}, {:a 1 :b 2 :c 5}, {:a 1 :b 1 :c 6}] [:a :b] -> {{:a 1 :b 2} [{:c 3} {:c 5}] {:a 1 :b 1} [{:c 6}]}

;(def tst [{:a 1 :b 2 :c 3}, {:a 1 :b 2 :c 5}, {:a 1 :b 1 :c 6}])

(ns etl-misc)

(defn submap
  "Projection of m on the keys ks"
  [m ks]
  (reduce (fn [subm x]
	    (assoc subm x (m x)))
	  {} ks))


;{ {:a 1 :b 2} [{:c 3} {:c 5}],  {:a 1 :b 1} [{:c 6}]}

(defn concentrate
  "Returns a map where the keys are the distinct submaps of elements from
   seqm and the values are the elemens of seqm that which agree on the key"
  [seqm ks]
  (reduce (fn [resmap x]
	    (let [s (submap x ks)]
	      (assoc resmap s (conj (resmap s) x))))
	  {} seqm))






