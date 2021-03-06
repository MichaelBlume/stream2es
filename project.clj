(defproject org.elasticsearch/stream2es "1.0-SNAPSHOT"
  :description "Index streams into ES."
  :url "http://github.com/elasticsearch/elasticsearch/stream2es"
  :license {:name "Apache 2"
            :url "http://www.apache.org/licenses/LICENSE-2.0.html"}
  :resource-paths ["etc" "resources"]
  :dependencies [[cheshire "5.0.1"]
                 [clj-http "0.6.3"]
                 [org.clojure/clojure "1.5.1"]
                 [org.clojure/tools.cli "0.3.0"]
                 [slingshot "0.10.3"]
                 [org.tukaani/xz "1.3"]
                 [log4j "1.2.17"]
                 [criterium "0.4.2"]
                 [loggly/loggly-indexmanager "1.0-SNAPSHOT"]
                 [org.clojure/data.priority-map "0.0.4"]]
  :plugins  [[lein-bin "0.3.2"]]
  :bin {:name "deck-chairs"
        :bootclasspath true}
  :main loggly.restructure.main
  :aot [loggly.restructure.main]
  :repl-options {:init-ns loggly.restructure.interactive
                 :host "0.0.0.0"})
