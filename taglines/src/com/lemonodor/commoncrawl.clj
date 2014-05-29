(ns com.lemonodor.commoncrawl
  (:require [cascalog.cascading.tap :as tap]
            [clojure.pprint :as pprint]
            [clojure.reflect :as reflect]
            [clojure.string :as string]
            [com.lemonodor.xio :as xio]
            [me.raynes.fs :as fs])
  (:import (com.lemonodor.cascading.scheme ARC ARCItem)
           (java.text SimpleDateFormat)
           (java.util Arrays)
           (org.apache.hadoop.io BytesWritable Text)
           (org.commoncrawl.crawl.common.shared Constants)
           (org.commoncrawl.io.shared NIOHttpHeaders)
           (org.commoncrawl.protocol.shared ArcFileItem ArcFileHeaderItem)
           (org.commoncrawl.util.shared ByteArrayUtils FlexBuffer
                                        ImmutableBuffer TextBytes)))

(set! *warn-on-reflection* true)

;; Discussion about valid segments:
;; https://groups.google.com/forum/#!topic/common-crawl/QYTmnttZZyo/discussion
;;
;; I was led to this discussion because when naively globbing accross
;; all segments, my hadoop job failed with: 2012-10-10 21:44:09,895
;; INFO org.apache.hadoop.io.retry.RetryInvocationHandler
;; (pool-1-thread-1): Exception while invoking retrieveMetadata of
;; class org.apache.hadoop.fs.s3native.Jets3tNativeFileSystemStore.
;; Not retrying.Status Code: 403, AWS Service: Amazon S3, AWS Request
;; ID: ..., AWS Error Code: null, AWS Error Message: Forbidden, S3
;; Extended Request ID: ...
;;
;; I found this discussion related to such errors:
;; https://groups.google.com/d/topic/common-crawl/e07aej71GLU/discussion

;; Obtained from:
;; https://s3.amazonaws.com/aws-publicdatasets/common-crawl/parse-output/valid_segments.txt
(def valid-segments
  (sorted-set
   1346823845675 1346823846036 1346823846039 1346823846110 1346823846125
   1346823846150 1346823846176 1346876860445 1346876860454 1346876860467
   1346876860493 1346876860565 1346876860567 1346876860596 1346876860609
   1346876860611 1346876860614 1346876860648 1346876860765 1346876860767
   1346876860774 1346876860777 1346876860779 1346876860782 1346876860786
   1346876860789 1346876860791 1346876860795 1346876860798 1346876860804
   1346876860807 1346876860817 1346876860819 1346876860828 1346876860835
   1346876860838 1346876860840 1346876860843 1346876860877 1346981172137
   1346981172142 1346981172155 1346981172184 1346981172186 1346981172229
   1346981172231 1346981172234 1346981172239 1346981172250 1346981172253
   1346981172255 1346981172258 1346981172261 1346981172264 1346981172266
   1346981172268))


(defn ^String text-path
  "Produces the glob of paths to text files organized according to
   CommonCrawl docs:
   https://commoncrawl.atlassian.net/wiki/display/CRWL/About+the+Data+Set

   Here's an example (assuming valid segments are 1, 2 and 3):

   (text-path \"s3://path/to/commoncrawl/segments\")
   => \"s3://path/to/commoncrawl/segments/{1,2,3}/textData*\""
  ([prefix segments]
     (->> segments
          (string/join ",")
          (format "%s/{%s}/textData*" prefix)))
  ([prefix]
     (text-path prefix valid-segments)))


(defn ^String arc-path
  "Produces the glob of paths to ARC raw content files organized
   according to CommonCrawl docs:
   https://commoncrawl.atlassian.net/wiki/display/CRWL/About+the+Data+Set

   Here's an example (assuming valid segments are 1, 2 and 3):

   (arc-path \"s3://path/to/commoncrawl/segments\")
   => \"s3://path/to/commoncrawl/segments/{1,2,3}/*.arc.gz\""
  ([prefix segments]
     (->> segments
          (string/join ",")
          (format "%s/{%s}/*.arc.gz" prefix)))
  ([prefix]
     (arc-path prefix valid-segments)))


(def ^SimpleDateFormat timestamp14 (SimpleDateFormat. "yyyyMMddHHmmss"))

(defn afi-bytes-metadata [^bytes bytes]
  (let [newline-index (ByteArrayUtils/indexOf
                       bytes 0 (count bytes) (.getBytes "\n"))
        line (.toString (TextBytes. bytes 0 newline-index true))
        pieces (string/split line #" ")]
    (update-in
     (apply assoc {}
            (interleave [:url :host-ip :timestamp :mime-type :size] pieces))
     [:timestamp]
     #(.getTime (.parse timestamp14 %)))))



(defn print-methods [item]
  (pprint/print-table
   (sort-by
    :name
    (filter :exception-types (:members (reflect/reflect item)))))
  true)


(defn headers-map [^NIOHttpHeaders headers]
  (into {}
        (for [^int i (range (.getKeyCount headers))]
          [(.getKey headers i) (.getValue headers i)])))

;; See https://github.com/commoncrawl/commoncrawl/blob/master/src/main/java/org/commoncrawl/util/shared/ArcFileItemUtils.java#L44

(defn arc-file-item-from-bytes
  "Reconstitutes a single ARC file item from raw bytes."
  [^bytes bytes]
  (let [metadata (afi-bytes-metadata bytes)
        ^ArcFileItem item (ArcFileItem.)
        crlf-index
        (ByteArrayUtils/indexOf bytes 0 (count bytes) (.getBytes "\r\n\r\n"))
        header-len (+ crlf-index 4)
        content-len (- (count bytes) header-len)
        header-str (.toString (TextBytes. bytes 0 header-len true))
        ^NIOHttpHeaders headers (NIOHttpHeaders/parseHttpHeaders header-str)]
    (.clear item)
    (.set (.getUriAsTextBytes item) (Text. ^String (:url metadata)) true)
    (.setHostIP item (:host-ip metadata))
    (.setMimeType item (:mime-type metadata))
    (.setTimestamp item (:timestamp metadata))
    (.setRecordLength item (count bytes))
    (doseq [[k v] (headers-map headers)]
      (.add (.getHeaderItems item)
            (doto (ArcFileHeaderItem.)
              (.setItemKey (or k ""))
              (.setItemValue (or v "")))))
    (.setContent
     item
     (FlexBuffer.
      (Arrays/copyOfRange bytes header-len content-len)
      0 (- content-len header-len) true))
    item))


(defn file-item [^bytes bytes]
  (let [metadata (afi-bytes-metadata bytes)
        crlf-index
        (ByteArrayUtils/indexOf bytes 0 (count bytes) (.getBytes "\r\n\r\n"))
        header-len (+ crlf-index 4)
        content-len (- (count bytes) header-len)
        ^NIOHttpHeaders headers (NIOHttpHeaders/parseHttpHeaders
                                 (.toString
                                  (TextBytes. bytes 0 header-len true)))]
    (assoc metadata
      :record-length (count bytes)
      :headers (headers-map headers)
      :content (Arrays/copyOfRange bytes header-len content-len))))


(defn hfs-arc-item-tap
  "Tap that sources ARC file items from Common Crawl segment files."
  [path & opts]
  (let [scheme (ARCItem.)]
    (apply tap/hfs-tap scheme path opts)))


(defn file-item-tap
  [path]
  (->> (fs/glob path)
       (map xio/binary-slurp)
       (map file-item)))


 (defn arc-file-item-mime-type [^ArcFileItem item]
   (.getMimeType item))

 (defn arc-file-item-host-ip [^ArcFileItem item]
   (.getHostIP item))

 (defn arc-file-item-record-length [^ArcFileItem item]
   (.getRecordLength item))

 (defn arc-file-item-timestamp [^ArcFileItem item]
   (.getTimestamp item))

(defn guess-afi-encoding [^ArcFileItem item]
  (first
   (map (fn [^ArcFileHeaderItem h]
          (.getItemValue h))
        (filter
         (fn [^ArcFileHeaderItem h]
           (= (.getItemKey h) "x-commoncrawl-DetectedCharset"))
         (.getHeaderItems item)))))

(defn arc-file-item-text
  ([^ArcFileItem item ^String encoding]
     (let [^ImmutableBuffer content (.getContent item)]
       (String. (.getReadOnlyBytes content) 0 (.getCount content) encoding)))
  ([^ArcFileItem item]
     (arc-file-item-text
      item
      (or (guess-afi-encoding item) "UTF8"))))

(defn file-item-text
  ([item ^String encoding]
     (String. ^bytes (:content item) encoding))
  ([item]
     (file-item-text
      item
      (or ((:headers item) "x-commoncrawl-DetectedCharset") "UTF8"))))
