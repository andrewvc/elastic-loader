(ns elastic-loader.core
  (require [clj-http.client :as client]
           [cheshire.core :as chesh]
           [clojure.string :as string]
           [clojure.java.io :as java-io]))

;; These tokenize line by line
(def line-tokenizers
  [{:name :index-type 
    :match  #"import-docs:\s*(\w+)/(\w+)"
    :format (fn [line matches] (rest matches))}
   {:name :http-req
    :match  #"(GET|POST|PUT|DELETE|HEAD)\s+(/[^ ]+)\s*(.*)"
    :format (fn [line matches] (rest matches))}
   {:name :document
    :match #"\{.*"
    :format (fn [line match] (list (chesh/parse-string line)))}
   ])

(defn line-tokenizer-compiler
  "Turns a line tokenizer into a function that returns either
   nil if no match, or the parsed line if there is one"
  [{n :name m :match f :format} line]
  (if-let [matches (re-matches m line)]
    (concat [n] (f line matches))
    nil))

;; Statement tokenizers
(def statement