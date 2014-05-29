(ns com.lemonodor.metafilter.taglines-tool
  (:require [clojure.java.io :as io]
            [clojure.string :as string]
            [me.raynes.fs :as fs]
            [net.cgrand.enlive-html :as html]
            [opennlp.nlp :as opennlp])
  (:import (java.io ByteArrayOutputStream)
           (java.util.zip GZIPInputStream))
  (:gen-class))


(def sentences
  (opennlp/make-sentence-detector (io/resource "models/en-sent.bin")))


(def tokenize
  (opennlp/make-tokenizer (io/resource "models/en-token.bin")))


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


(defn is-tagline? [sentence]
  (let [tokens (tokenize sentence)]
    (and (= (string/lower-case (first tokens)) "metafilter")
         (= (second tokens) ":")
         (> (count tokens) 2))))


(defn canonicalize-text [text]
  (string/join
   " "
   (string/split text #" +")))


(defn comment-taglines [comment]
  (map
   canonicalize-text
   (filter is-tagline? (sentences comment))))


(defn post-taglines [html]
  (mapcat comment-taglines (comments html)))


(defn content [s]
  (string/replace s #".*\r\n\r\n" ""))


(defn read-mefi-post-file [path]
  (binding [*out* *err*]
    (println path))
  (with-open [in (-> path fs/file io/input-stream)]
    (let [out (ByteArrayOutputStream.)]
      (io/copy (GZIPInputStream. in) out)
      (.toString out "UTF8"))))


(defn -main
  [& args]
  (let [files (mapcat fs/glob args)
        tagline-seqs (pmap #(->> %
                                 read-mefi-post-file
                                 content
                                 comments
                                 (mapcat comment-taglines))
                           files)]

    (doseq [taglines tagline-seqs]
      (doseq [tagline taglines]
        (println tagline))))
  (System/exit 0))
