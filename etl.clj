(ns etl
    (use (clojure.contrib [except :only [throwf]])
	 (clojure.contrib.sql [internal :only [get-connection]])
	 (cloure.contrib.logging)))

(defn connect
  "Holt die connection zu der Definition"
  [def]
  (clojure.contrib.sql.internal/get-connection def))


(defmacro sql
  [con sql sql-params]
  `(do
     (let [whole-sql-for-logging# (str "SQL: " ~sql ":" ~sql-params)]
       (clojure.contrib.logging/debug whole-sql-for-logging#)
       (try
	(with-open [stmt# (.prepareStatement ~con ~sql)]
	    (doseq [[index# value#] (map vector (iterate inc 1) ~sql-params)]
		(.setObject stmt# index# value#))
	  (.executeQuery stmt#))
	(catch Exception e#
	  (do
	    (clojure.contrib.logging/error whole-sql-for-logging# e#)
	    (throw e#)))))))



(defmacro with-query-results-all
  "Fuehrt query auf connection conn aus, evaluiert body mit var *etl-query-results* als
   Ergebnis von resultset-seq"
  [con query body]
  `(with-open [stmt# (.prepareStatement ~con ~query)]
    (with-open [rset# (.executeQuery stmt#)]
      (let [~'*etl-query-results* (resultset-seq rset#)]
	~body))))

(defmacro with-query-results
  "Fuehrt query auf connection conn aus, evaluiert body mit var *etl-query-results* als
   Ergebnis von resultset-seq"
  [con query body]
  `(with-open [stmt# (.prepareStatement ~con ~query)]
    (with-open [rset# (.executeQuery stmt#)]
      (doseq [~'*rs* (resultset-seq rset#)]
	~body))))

(defn apply-dvm
  "Domainumrechung. Benutzung:
  (apply-dvm dvm-meh :SYSTEMA :SYSTEMB 'MTR')"
  [dvm from to txt]
  (let [res (filter #(= txt (from %)) dvm)]
    (cond
     (= 0 (count res)) (throwf "Kein Treffer: DVM=%s, From=%s, Text=%s" dvm from txt)
     (> 1 (count res)) (throwf "Mehr als ein Treffer: DVM=%s, From=%s, Text=%s" dvm from txt)
     true (to (first res)))))

(defmacro v?
  "Gibt den Wert der uebergebenen Tabellenspalte aus"
  [t]
  `(~(keyword t) ~'*rs*))
