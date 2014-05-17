(ns com.lemonodor.metafilter.taglines
  (:require [cascalog.api :refer :all]
            [cascalog.cascading.tap :as tap]
            [cascalog.logic.def :as def]
            [cascalog.logic.ops :as c]
            [cascalog.more-taps :refer [hfs-wrtseqfile]]
            [clojure.java.io :as io]
            [clojure.string :as string]
            [clojurewerkz.crawlista.extraction.content :as content]
            [net.cgrand.enlive-html :as html]
            [opennlp.nlp :as opennlp])
  (:import (com.lemonodor.cascading.scheme ARC)
           (org.apache.commons.httpclient URI)
           (org.apache.hadoop.io BytesWritable Text)))


(defn hfs-arc
  [path & opts]
  (let [scheme (ARC.)]
    (apply tap/hfs-tap scheme path opts)))


;; Obtained from:
;; https://s3.amazonaws.com/aws-publicdatasets/common-crawl/parse-output/valid_segments.txt
;; and
;; env/bin/cci_lookup --print_metadata com.metafilter.www |
;;   awk -F '\t' '{print $2;}' |
;;   jq '.arcSourceSegmentId' | sort | uniq

(def valid-segments
  [1346823845675])
   ;; 1346823846039
   ;; 1346823846125
   ;; 1346823846176
   ;; 1346876860445
   ;; 1346876860454
   ;; 1346876860493
   ;; 1346876860567
   ;; 1346876860609
   ;; 1346876860611
   ;; 1346876860614
   ;; 1346876860648
   ;; 1346876860765
   ;; 1346876860774
   ;; 1346876860777
   ;; 1346876860779
   ;; 1346876860782
   ;; 1346876860795
   ;; 1346876860798
   ;; 1346876860804
   ;; 1346876860807
   ;; 1346876860817
   ;; 1346876860819
   ;; 1346876860828
   ;; 1346876860838
   ;; 1346876860840
   ;; 1346876860843
   ;; 1346876860877])


(defn ^String text-path
  "Produces the glob of paths to text files organized
   according to CommonCrawl docs: http://tinyurl.com/common-crawl-about-dataset
   Here's an example (assuming valid segments are 1, 2 and 3):
   (text-path 's3://path/to/commoncrawl/segments')
   => 's3://path/to/commoncrawl/segments/{1,2,3}/textData*'"
  [prefix]
  (->> valid-segments
       (string/join ",")
       (format "%s/{%s}/textData*" prefix)))


(defn ^String parse-hostname
  "Extracts the hostname from the specified URL."
  [^Text url]
  (-> url str URI. .getHost (or "")))


(defn summarize [html]
  (str html))


(defn is-metafilter? [^String hostname]
  (re-find #"www\.metafilter\.com$" hostname))


(def/defmapcatfn get-comments [html]
  (let [s (-> html
              str
              (string/replace #"<br>" " ")
              (html/html-snippet)
              (html/select [:div.comments]))]
    (let [c (map (fn [com] (assoc com :content (butlast (:content com)))) s)]
      (map html/text c))))


(def sentences (opennlp/make-sentence-detector (io/resource "models/en-sent.bin")))


(def tokenize
  (opennlp/make-tokenizer (io/resource "models/en-token.bin")))


(defn is-tagline? [sentence]
  (let [tokens (tokenize sentence)]
    (and (= (string/lower-case (first tokens)) "metafilter")
         (= (second tokens) ":")
         (> (count tokens) 2))))


(defn canonicalize-text [text]
  (string/join
   " "
   (string/split text #" +")))


(defn metafilter-taglines [comment]
  (map
   canonicalize-text
   (filter is-tagline? (sentences comment))))


(def/defmapcatfn get-metafilter-taglines [html]
  (metafilter-taglines html))


;; (defn query-taglines
;;   "Counts site URLs from the metadata corpus grouped by TLD of each URL."
;;   [text-tap trap-tap]
;;   (<- [?tagline ?count]
;;       (text-tap :> ?url ?html)
;;       (parse-hostname ?url :> ?host)
;;       (is-metafilter? ?host)
;;       (get-comments :< ?html :> ?comment)
;;       (get-metafilter-taglines :< ?comment :> ?tagline)
;;       (c/count :> ?count)
;;       (:trap trap-tap)))

(defn query-taglines
  "Counts site URLs from the metadata corpus grouped by TLD of each URL."
  [text-tap trap-tap]
  (<- [?url ?html]
      (text-tap :> ?url ?html)
      (:trap trap-tap)))


(defmain MetafilterTaglinesExe
  "Defines 'main' method that will execute our query."
  [prefix output-dir]
  (let [text-tap (hfs-wrtseqfile (text-path prefix)
                                 Text Text
                                 :outfields ["key" "value"])
        trap-tap (hfs-seqfile (str output-dir ".trap"))]
    (?- (hfs-textline output-dir)
        (query-taglines text-tap trap-tap))))
