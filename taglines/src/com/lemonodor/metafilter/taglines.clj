(ns com.lemonodor.metafilter.taglines
  (:require [cascalog.api :refer :all]
            [cascalog.logic.def :as def]
            [cascalog.logic.ops :as c]
            [clojure.java.io :as io]
            [clojure.string :as string]
            [com.lemonodor.commoncrawl :as cc]
            [net.cgrand.enlive-html :as html]
            [opennlp.nlp :as opennlp])
  (:import (org.apache.commons.httpclient URI)
           (org.apache.hadoop.io Text))
  (:gen-class))





;; Obtained from:
;; https://s3.amazonaws.com/aws-publicdatasets/common-crawl/parse-output/valid_segments.txt
;; and
;; env/bin/cci_lookup --print_metadata com.metafilter.www |
;;   awk -F '\t' '{print $2;}' |
;;   jq '.arcSourceSegmentId' | sort | uniq

(def valid-segments
  [1346823845675
   1346823846039
   1346823846125
   1346823846176
   1346876860445
   1346876860454
   1346876860493
   1346876860567
   1346876860609
   1346876860611
   1346876860614
   1346876860648
   1346876860765
   1346876860774
   1346876860777
   1346876860779
   1346876860782
   1346876860795
   1346876860798
   1346876860804
   1346876860807
   1346876860817
   1346876860819
   1346876860828
   1346876860838
   1346876860840
   1346876860843
   1346876860877])


(defn ^String parse-hostname
  "Extracts the hostname from the specified URL."
  [^Text url]
  (-> url str URI. .getHost (or "")))


(defn summarize [html]
  (str html))


(defn is-metafilter? [^String hostname]
  (re-find #"www\.metafilter\.com$" hostname))



(defn comments [html]
  (let [s (-> html
              str
              (string/replace #"<br>" " ")
              (html/html-snippet)
              (html/select [:div.comments]))]
    (->> s
         (map (fn [com] (assoc com :content (butlast (:content com)))))
         (map html/text)
         (map #(string/replace % #"([^\.\!\?\"])\n\n" "$1.\n")))))


(def/defmapcatfn get-comments [html]
  (comments html))

(def sentences
  (opennlp/make-sentence-detector (io/resource "models/en-sent.bin")))


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


(defn query-arc-taglines
  "Counts site URLs from the metadata corpus grouped by TLD of each URL."
  [arc-item-tap trap-tap]
  (<- [?tagline ?count]
      (arc-item-tap :> ?url ?item)
      (parse-hostname ?url :> ?host)
      (is-metafilter? ?host)
      (cc/item-text :< ?item :> ?html)
      (get-comments :< ?html :> ?comment)
      (get-metafilter-taglines :< ?comment :> ?tagline)
      (c/count :> ?count)
      (:trap trap-tap)))

(defmain MetafilterTaglinesExe
  "Defines 'main' method that will execute our query."
  [prefix output-dir]
  (let [arc-item-tap (cc/hfs-arc-item-tap (cc/arc-path prefix valid-segments))
        trap-tap (hfs-seqfile (str output-dir ".trap"))]
    (?- (hfs-textline output-dir)
        (query-arc-taglines arc-item-tap trap-tap))))
