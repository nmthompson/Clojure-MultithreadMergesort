(ns my-project.core
  (:gen-class :main true))
(use 'clojure.java.io)

(defn read-file[path]
  (with-open [reader (input-stream path)]
  (let [length (.length (file path))
    buffer (byte-array length)]
    (.read reader buffer 0 length)
    buffer)))


;Assign file to variable
;Ref: https://www.tutorialspoint.com/clojure/clojure_variables.htm
(def var-integer-list (read-file "numbers_bin.dat"))

;Retrieve current system time
(defn getCurrentTime[] (System/currentTimeMillis))

;Merge Sort Algorithm
;Ref: https://rosettacode.org/wiki/Sorting_algorithms/Merge_sort#Clojure
;     http://stackoverflow.com/questions/36731188/multithreaded-merge-sort-algorithm-in-clojure
;     https://gist.github.com/ChrisWphoto/c6f86f34cd7ade5055aec743ec990765
(defn merge-sort-algo [left right]
  (loop [head [] L left R right]
    (if (empty? L) (concat head R)
    (if (empty? R) (concat head L)
    (if (> (first L) (first R))
      (recur (conj head (first R)) L (rest R))
      (recur (conj head (first L)) (rest L) R))))))

(defn merge-sort [integer-list]
  (if (< (count integer-list) 2)
    integer-list
    (apply merge-sort-algo
      (map merge-sort
        (split-at (/ (count integer-list) 2)
        integer-list)))))

(defn sort-parallel-2 [integer-list]
  (if (< (count integer-list) 2)
    integer-list
    (apply merge-sort-algo
      (pmap merge-sort
        (split-at (/ (count integer-list) 2)
        integer-list)))))

(defn sort-parallel-4 [integer-list]
  (if (< (count integer-list) 2)
    integer-list
    (apply merge-sort-algo
      (pmap sort-parallel-2
        (split-at (/ (count integer-list) 2)
        integer-list)))))

(defn sort-parallel-8 [integer-list]
  (if (< (count integer-list) 2)
    integer-list
    (apply merge-sort-algo
      (pmap sort-parallel-4
        (split-at (/ (count integer-list) 2)
        integer-list)))))

(defn sort-parallel-16 [integer-list]
  (if (< (count integer-list) 2)
    integer-list
    (apply merge-sort-algo
      (pmap sort-parallel-8
        (split-at (/ (count integer-list) 2)
        integer-list)))))

(defn sort-parallel-32 [integer-list]
  (if (< (count integer-list) 2)
    integer-list
    (apply merge-sort-algo
      (pmap sort-parallel-16
        (split-at (/ (count integer-list) 2)
        integer-list)))))

;define respective parallel threads
(defn runParallel []
      (def start (getCurrentTime))
        (merge-sort var-integer-list)
          (def end (getCurrentTime))
            (- end start))

(defn runParallel2 []
      (def start (getCurrentTime))
        (sort-parallel-2 var-integer-list)
          (def end (getCurrentTime))
            (- end start))

(defn runParallel4 []
      (def start (getCurrentTime))
        (sort-parallel-4 var-integer-list)
          (def end (getCurrentTime))
            (- end start))

(defn runParallel8 []
      (def start (getCurrentTime))
        (sort-parallel-8 var-integer-list)
          (def end (getCurrentTime))
            (- end start))

(defn runParallel16 []
      (def start (getCurrentTime))
        (sort-parallel-16 var-integer-list)
          (def end (getCurrentTime))
            (- end start))

(defn runParallel32 []
      (def start (getCurrentTime))
        (sort-parallel-32 var-integer-list)
          (def end (getCurrentTime))
            (- end start))

;Ref: https://gist.github.com/fadelakin/7058608
(defn average
  [numbers]
    (quot (apply + numbers) (count numbers)))

;Run through each partition the requested 5 times and record results
;Ref: http://stackoverflow.com/questions/9981943/how-to-implement-a-for-loop-in-clojure
;     https://clojuredocs.org/clojure.core/spit
(defn -main []
  ;JK, I can't figure out for loops and parameter passing properly, I like the idea that I found on stack
  ;but id like to clean up the implementation if I had time

  ;Special thanks to Zach Brown for providing some reference material listed above after I couldn't get my for loops 
  ; and passes to work.
     (spit "run_pass_results.txt" "Results - Average Seconds for Sort:: " :append true)

    (spit "run_pass_results.txt" "\nSingle Thread: " :append true)
    (spit "run_pass_results.txt" "\nAverage Time - Single Thread: " :append true)
    (def run-pass-1 (runParallel))
    (def run-pass-2 (runParallel))
    (def run-pass-3 (runParallel))
    (def run-pass-4 (runParallel))
    (def run-pass-5 (runParallel))
    (def run-pass[run-pass-1, run-pass-2, run-pass-3, run-pass-4, run-pass-5])
    (spit "run_pass_results.txt" (float(/ (average run-pass) 1000)) :append true)


	(spit "run_pass_results.txt" "\n2 Threads " :append true)	
	(spit "run_pass_results.txt" "\nAverage Time - 2 Threads: " :append true)
    (def run-pass-1 (runParallel2))
    (def run-pass-2 (runParallel2))
    (def run-pass-3 (runParallel2))
    (def run-pass-4 (runParallel2))
    (def run-pass-5 (runParallel2))
    (def run-pass[run-pass-1, run-pass-2, run-pass-3, run-pass-4, run-pass-5])
    (spit "run_pass_results.txt" (float(/ (average run-pass) 1000)) :append true)

	(spit "run_pass_results.txt" "\n4 Threads " :append true)	
	(spit "run_pass_results.txt" "\nAverage Time - 4 Threads: " :append true)
    (def run-pass-1 (runParallel4))
    (def run-pass-2 (runParallel4))
    (def run-pass-3 (runParallel4))
    (def run-pass-4 (runParallel4))
    (def run-pass-5 (runParallel4))
    (def run-pass[run-pass-1, run-pass-2, run-pass-3, run-pass-4, run-pass-5])
    (spit "run_pass_results.txt" (float(/ (average run-pass) 1000)) :append true)

	(spit "run_pass_results.txt" "\n8 Threads " :append true)	
	(spit "run_pass_results.txt" "\nAverage Time - 8 Threads: " :append true)
    (def run-pass-1 (runParallel8))
    (def run-pass-2 (runParallel8))
    (def run-pass-3 (runParallel8))
    (def run-pass-4 (runParallel8))
    (def run-pass-5 (runParallel8))
    (def run-pass[run-pass-1, run-pass-2, run-pass-3, run-pass-4, run-pass-5])
    (spit "run_pass_results.txt" (float(/ (average run-pass) 1000)) :append true)

	(spit "run_pass_results.txt" "\n16 Threads " :append true)	
	(spit "run_pass_results.txt" "\nAverage Time - 16 Threads: " :append true)
    (def run-pass-1 (runParallel16))
    (def run-pass-2 (runParallel16))
    (def run-pass-3 (runParallel16))
    (def run-pass-4 (runParallel16))
    (def run-pass-5 (runParallel16))
    (def run-pass[run-pass-1, run-pass-2, run-pass-3, run-pass-4, run-pass-5])
    (spit "run_pass_results.txt" (float(/ (average run-pass) 1000)) :append true)

	(spit "run_pass_results.txt" "\n32 Threads " :append true)	
	(spit "run_pass_results.txt" "\nAverage Time - 32 Threads: " :append true)
    (def run-pass-1 (runParallel32))
    (def run-pass-2 (runParallel32))
    (def run-pass-3 (runParallel32))
    (def run-pass-4 (runParallel32))
    (def run-pass-5 (runParallel32))
    (def run-pass[run-pass-1, run-pass-2, run-pass-3, run-pass-4, run-pass-5])
    (spit "run_pass_results.txt" (float(/ (average run-pass) 1000)) :append true)
    )

