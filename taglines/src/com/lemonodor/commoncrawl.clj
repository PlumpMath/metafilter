(ns com.lemonodor.commoncrawl
  (:require [cascalog.cascading.tap :as tap]
            [clojure.string :as string])
  (:import (com.lemonodor.cascading.scheme ARC ARCItem)
           (org.apache.hadoop.io BytesWritable)
           (org.commoncrawl.protocol.shared ArcFileItem)
           (org.commoncrawl.util.shared ImmutableBuffer)))


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
  [1346823845675 1346823846036 1346823846039 1346823846110 1346823846125
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
   1346981172268])

(defn ^String text-path
  "Produces the glob of paths to text files organized according to
   CommonCrawl docs: http://tinyurl.com/common-crawl-about-dataset
   Here's an example (assuming valid segments are 1, 2 and 3):
   (text-path 's3://path/to/commoncrawl/segments')
   => 's3://path/to/commoncrawl/segments/{1,2,3}/textData*'"
  [prefix]
  (->> valid-segments
       (string/join ",")
       (format "%s/{%s}/textData*" prefix)))


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

(defn item-text [^ArcFileItem item]
  (let [^ImmutableBuffer content (.getContent item)]
    (String. (.getReadOnlyBytes content) 0 (.getCount content))))

;; ByteArrayInputStream inputStream = new ByteArrayInputStream(
;;           value.getContent().getReadOnlyBytes(), 0,
;;           value.getContent().getCount());
;;       // Converts InputStream to a String.
;;       String content = new Scanner(inputStream).useDelimiter("\\A").next();


(defn bytes-to-string [^BytesWritable bytes]
  (String. (.getBytes bytes) "UTF8"))
