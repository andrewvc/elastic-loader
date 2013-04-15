(ns elastic-loader.core
  (:require [clj-http.client :as http]
           [cheshire.core :as chesh]
           [clojure.tools.logging :as log]
           [clojure.string :as string]           
           [clojure.java.io :as java-io])
  (:use [slingshot.slingshot :only [throw+ try+]]))
;;
;; OVERVIEW
;;
;; First the file is turned into a lazy seq of tokens, one per line
;; Second, it the sequence of tokens is parsed into another lazy seq ofstatements. The
;;   statements are not nestable or recursive, in any sense, this is here because they
;;   can span multiple lines
;; Finally, the sequence of statements is executed
;;

(defn create-es-client
  [base-url]
  (fn [opts]
    (http/request (update-in opts [:url] #(str base-url "/" %)))))

(def line-tokenizers
  [{:name :index-docs 
    :match  #"BULK INDEX\s*(\w+)/(\w+)"
    :format (fn [line matches] (rest matches))}
   {:name :http-req
    :match  #"(TRY |)(GET|POST|PUT|DELETE|HEAD)\s+(/[^ ]+)\s*(.*)"
    :format (fn [line matches] (rest matches))}
   {:name :document
    :match #"\{.*"
    :format (fn [line match]
              (list
               (chesh/parse-string line)))}
   {:name :blank
    :match #"\s*(#.*)?"
    :format (fn [_ __] nil )}
   ])


(defn tokenizer-matcher
  "Match a tokenizer definition to a line"
  [{n :name m :match f :format} line]
  (if-let [matches (re-matches m line)]
    (concat [n] (f line matches))
    nil))

;; Create a seq of parsing functions we can iterate through
(def line-tokenizers
  (filter (comp not nil?) (map #(partial tokenizer-matcher %) line-tokenizers)))

(defn attempt-tokenize
  [tokenizer line-str line-num]
  (try
    (tokenizer line-str)
    (catch Exception e
      (log/fatal (format "Could not tokenize line %d of input! '%s'" line-num (.getMessage e)))
      (throw+ {:level :fatal :exception e}))))

(defn tokenize-line
  "Check if the given line is a document or an index declaration"
  [{:keys [line-num line-str]}]
  ; Use a loop here for fine control over how many tokenizers we match
  ; We could use say 'filter' but lazy seqs chunk, and that's
  ; inefficient
  (loop [tokenizers line-tokenizers]
      (if-let [res (attempt-tokenize (first tokenizers) line-str line-num)]
        (with-meta res {:line-num line-num})
        (if (empty? (rest tokenizers))
          (throw+ {:level :fatal :message (format "Input Line %s un-tokenizable: '%s'" line-num line-str)})
          (recur (rest tokenizers)))) ))

(defn tag-line-num
  [l]
  (let [counter (atom 0)]
    {:line-num (swap! counter inc) :line-str l}))

(defn tokenize
  [lines]
  (->> lines
       (map tag-line-num)       
       (map tokenize-line)))

(defn parse-group-tokens
  "Group a token stream into tokens needed for creating statements"
  [tokens]
  (let [last-start (atom [nil 0])]
    (partition-by
     (fn [[token-type & _]]
       (if (#{:index-docs :http-req} token-type)
         (swap! last-start (fn [[_ gen]] [token-type (inc gen)]))
         @last-start))
     tokens)))

(defn index-docs-statementizer
  [[index-name type-name] r-tokens]
  {:index index-name
   :type type-name
   :documents (flatten (map rest r-tokens))})

(defn http-req-statementizer
  [[is-try-str method path inline-body] r-tokens]
  (let [is-try (not (string/blank? is-try-str))
        base {:method method :path path :is-try is-try}]
    (if inline-body
      (assoc base :body inline-body)
      r-tokens)))

(def statementizers
  {:index-docs index-docs-statementizer
   :http-req http-req-statementizer})

(defn parse-statementize-tokens
  [[[s-type & s-args :as first-token] & r-tokens]]
  (if-let [statementizer (statementizers s-type)]
    (with-meta
      (statementizer s-args r-tokens)
      {:statement-type s-type
       :line-num (:line-num (meta first-token))})
    (throw+ {:level :fatal
             :message (format "No statementizer for: %s" s-type)})))

(defn parse
  [tokens]
  (map parse-statementize-tokens
       (parse-group-tokens tokens)))

(defn exec-http-req
  [{:keys [client]} {:keys [method path body is-try]}]
  (log/info (format "%s %s (bytes %d)" method path (if body (alength (.getBytes body "UTF-8")) 0)))
  (try
    (client {:method (keyword (string/lower-case method))
             :url path
             :body body})
    (catch Exception e
      (if is-try
        (throw+ {:level :warn
                 :type :mystuff
                 :message (format "Ignoring error due to TRY. Error: %s" (.getMessage e))})
        (do (log/fatal (str "ABORTING: " (.getMessage e)))
            (throw+ {:level :fatal :exception e}))))))

(defn exec-index-docs
  [{:keys [client]} {:keys [documents index type]}]
  (let [tupleize (fn [d]
                   (let [id (d "_id")
                         base {:index {:_index index :_type type}}
                         es-cmd (if id
                                  (assoc-in base [:index :_id] id)
                                  base)]
                     [es-cmd d]))
        lines (map chesh/generate-string (flatten (map tupleize documents)))
        body (string/join "\n" lines)]
    (log/info (format "BULK INDEX /%s/%s" index type))
    [client {:method :post :url "/_bulk" :body body}]))

(def statement-executors
  {:http-req exec-http-req
   :index-docs exec-index-docs
   :blank (fn [_ __])})

(defn exec-statement
  [env statement]
  (let [{st-type :statement-type line-num :line-num} (meta statement)
        st-executor (statement-executors st-type)]
    (try+
     (st-executor env statement)
     (catch [:level :warn] {:keys [message]}
       (log/warn (format "Could not execute statement at line %d: %s"
                         line-num message))))))

(defn execute
  [env statements]
  (try+
    (dorun (map (partial exec-statement env) statements))
    (catch [:level :fatal] {:keys [message exception] :as k}
      (log/fatal (or message exception))
      (when exception (.printStackTrace exception))
      (System/exit 1))))

(defn -main   
  [filename base-url & argv]
  (let [client (create-es-client base-url)
        env {:client client}
        env-exec (partial execute env)]
    (with-open [rdr (java-io/reader filename)]
      (-> (line-seq rdr)
          (tokenize)
          (parse)
          (env-exec)))))

;(-main "test_import" "http://localhost:9200")