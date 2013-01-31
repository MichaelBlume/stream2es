(defproject org.elasticsearch/wiki2es
  (-> "etc/version.txt" slurp .trim)
  :description "Index wikipedia dump into ES."
  :url "http://github.com/elasticsearch/elasticsearch/wiki2es"
  :license {:name "Apache 2"
            :url "http://www.apache.org/licenses/LICENSE-2.0.html"}
  :resource-paths ["etc" "resources"]
  :dependencies [[org.clojure/clojure "1.5.0-RC2"]
                 [org.elasticsearch/elasticsearch-river-wikipedia
                  "1.2.0-SNAPSHOT"]
                 [cheshire "5.0.1"]
                 [clj-http "0.6.3"]]
  :plugins [[lein-bin "0.3.0"]]
  :main wiki2es.main)
