(ns com.lemonodor.commoncrawl
  (:require [cascalog.cascading.tap :as tap]
            [clojure.string :as string])
  (:import (com.lemonodor.cascading.scheme ARC ARCItem)
           (java.util Arrays)
           (org.apache.hadoop.io BytesWritable Text)
           (org.commoncrawl.crawl.common.shared Constants)
           (org.commoncrawl.io.shared NIOHttpHeaders)
           (org.commoncrawl.protocol.shared ArcFileItem)
           (org.commoncrawl.util.shared ArcFileItemUtils ByteArrayUtils FlexBuffer ImmutableBuffer TextBytes)))


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
   CommonCrawl docs: http://tinyurl.com/common-crawl-about-dataset
   Here's an example (assuming valid segments are 1, 2 and 3):
   (text-path 's3://path/to/commoncrawl/segments')
   => 's3://path/to/commoncrawl/segments/{1,2,3}/textData*'"
  ([prefix segments]
     (->> segments
          (string/join ",")
          (format "%s/{%s}/textData*" prefix)))
  ([prefix]
     (text-path prefix valid-segments)))

(defn ^String arc-path
  ([prefix segments]
     (->> segments
          (string/join ",")
          (format "%s/{%s}/*.arc.gz" prefix)))
  ([prefix]
     (arc-path prefix valid-segments)))


(defn arc-file-item-from-bytes [^String url ^bytes bytes]
  (let [^ArcFileItem item (ArcFileItem.)
        ^BytesWritable bw (BytesWritable. bytes)
        crlf-index
        (ByteArrayUtils/indexOf bytes 0 (count bytes) (.getBytes "\r\n\r\n"))
        header-len (+ crlf-index 4)
        content-len (- (count bytes) header-len)
        header-str (.toString (TextBytes. bytes 0 header-len true))
        _ (println "AHH" header-str)
        ^NIOHttpHeaders headers (NIOHttpHeaders/parseHttpHeaders header-str)]
    (.clear item)
    (.set (.getUriAsTextBytes item) (Text. url) true)
    (when-let [host-ip (.findValue headers Constants/ARCFileHeader_HostIP)]
      (.setHostIP item host-ip))
    (if-let [mime-type (.findValue headers Constants/ARCFileHeader_ARC_MimeType)]
      (.setMimeType item mime-type)
      (.setMimeType item "text/html"))
    (.setRecordLength item (count bytes))
    ;; arcFileItem.setContent(new FlexBuffer(rawArcPayload.getBytes(),headerLen,contentLen,true));
    (println "NAH"(count bytes) header-len content-len)
    (.setContent
     item
     (FlexBuffer.
      (Arrays/copyOfRange bytes header-len content-len)
      0 (- content-len header-len) true))
    item))


(defn arc-item-tap
  [])

(defn hfs-arc-tap
  [path & opts]
  (let [scheme (ARC.)]
    (apply tap/hfs-tap scheme path opts)))

(defn hfs-arc-item-tap
  [path & opts]
  (let [scheme (ARCItem.)]
    (apply tap/hfs-tap scheme path opts)))

 (defn item-mime-type [^ArcFileItem item]
   (.getMimeType item))

(defn item-text
  ([^ArcFileItem item ^String encoding]
     (let [^ImmutableBuffer content (.getContent item)]
       (String. (.getReadOnlyBytes content) 0 (.getCount content) encoding)))
  ([^ArcFileItem item]
     (item-text item "UTF8")))

(defn bytes-to-string [^BytesWritable bytes]
  (String. (.getBytes bytes) "UTF8"))
