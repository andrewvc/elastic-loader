(defproject elastic-loader "0.3.2"
  :description "A tool for importing docs into elasticsearch"
  :url "https://github.com/andrewvc/elastic-loader"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :dependencies [[org.clojure/clojure "1.5.0"]
                 [cheshire "5.1.0"]
                 [slingshot "0.10.3"]
                 [log4j/log4j "1.2.16" :exclusions [javax.mail/mail
                                                    javax.jms/jms
                                                    com.sun.jdmk/jmxtools
                                                    com.sun.jmx/jmxri]]                 
                 [org.clojure/tools.logging "0.2.6"]
                 [clj-http "0.7.1"]]
  :javac-options ["-target" "1.6" "-source" "1.6"],
  :main elastic-loader.core)
