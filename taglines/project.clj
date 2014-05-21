(defproject metafilter-taglines "0.1.0-SNAPSHOT"
  :description "Mines Common Crawl data for 'Metafilter:' taglines."
  :url "http://github.com/wiseman/metafilter/taglines"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :repositories {"conjars" "http://conjars.org/repo"}
  :dependencies [[cascalog "2.1.0"]
                 [clojure-opennlp "0.3.2"]
                 [clojurewerkz/crawlista "1.0.0-alpha18"]
                 [com.lemonodor/xio "0.2.0"]
                 [enlive "1.1.5"]
                 [me.raynes/fs "1.4.4"]
                 [org.clojure/clojure "1.5.1"]
                 [org.commoncrawl/libcommoncrawl "1.1"
                  :exclusions [org.slf4j/slf4j-api]]]
  :profiles {:provided
             {:dependencies [[org.apache.hadoop/hadoop-core "1.1.2"]]}
             :dev
             {:dependencies [[midje "1.5.1"]]
              :plugins [[lein-midje "3.0.1"]]}}
  :java-source-paths ["src/java"]
  :jar-name "metafilter-taglines.jar"
  :uberjar-name "metafilter-taglines-standalone.jar"
  :javac-options ["-target" "1.6" "-source" "1.6"]
  :jvm-opts ^:replace ["-Xmx2g" "-server"]
  :main com.lemonodor.metafilter.taglines-tool
  :aot [com.lemonodor.metafilter.taglines
        com.lemonodor.metafilter.taglines-tool])
