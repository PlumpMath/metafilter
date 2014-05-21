(ns com.lemonodor.commoncrawl-test
  (:require [cascalog.api :refer :all]
            [cascalog.logic.ops :as c]
            [cascalog.logic.testing :refer :all]
            [clojure.java.io :as io]
            [clojure.pprint :as pprint]
            [clojure.reflect :as reflect]
            [clojure.test :refer :all]
            [com.lemonodor.commoncrawl :as cc]
            [com.lemonodor.xio :as xio]
            [midje.cascalog :refer :all]
            [midje.sweet :refer :all]))


(defn print-methods [item]
  (pprint/print-table
   (sort-by
    :name
    (filter :exception-types (:members (reflect/reflect item)))))
  true)


(deftest path-tests
  (fact "default text-paths"
    (cc/text-path
     "s3://aws-publicdatasets/common-crawl/parse-output/segment")
    =>
    (str
     "s3://aws-publicdatasets/common-crawl/parse-output/segment/"
     "{1346823845675,1346823846036,1346823846039,1346823846110,1346823846125,"
     "1346823846150,1346823846176,1346876860445,1346876860454,1346876860467,"
     "1346876860493,1346876860565,1346876860567,1346876860596,1346876860609,"
     "1346876860611,1346876860614,1346876860648,1346876860765,1346876860767,"
     "1346876860774,1346876860777,1346876860779,1346876860782,1346876860786,"
     "1346876860789,1346876860791,1346876860795,1346876860798,1346876860804,"
     "1346876860807,1346876860817,1346876860819,1346876860828,1346876860835,"
     "1346876860838,1346876860840,1346876860843,1346876860877,1346981172137,"
     "1346981172142,1346981172155,1346981172184,1346981172186,1346981172229,"
     "1346981172231,1346981172234,1346981172239,1346981172250,1346981172253,"
     "1346981172255,1346981172258,1346981172261,1346981172264,1346981172266,"
     "1346981172268}/textData*"))
  (fact "specified text-paths"
    (cc/text-path "some-prefix" [1 2 3])
    =>
    "some-prefix/{1,2,3}/textData*")
  (fact "default arc-paths"
    (cc/arc-path
     "s3://aws-publicdatasets/common-crawl/parse-output/segment")
    =>
    (str
     "s3://aws-publicdatasets/common-crawl/parse-output/segment/"
     "{1346823845675,1346823846036,1346823846039,1346823846110,1346823846125,"
     "1346823846150,1346823846176,1346876860445,1346876860454,1346876860467,"
     "1346876860493,1346876860565,1346876860567,1346876860596,1346876860609,"
     "1346876860611,1346876860614,1346876860648,1346876860765,1346876860767,"
     "1346876860774,1346876860777,1346876860779,1346876860782,1346876860786,"
     "1346876860789,1346876860791,1346876860795,1346876860798,1346876860804,"
     "1346876860807,1346876860817,1346876860819,1346876860828,1346876860835,"
     "1346876860838,1346876860840,1346876860843,1346876860877,1346981172137,"
     "1346981172142,1346981172155,1346981172184,1346981172186,1346981172229,"
     "1346981172231,1346981172234,1346981172239,1346981172250,1346981172253,"
     "1346981172255,1346981172258,1346981172261,1346981172264,1346981172266,"
     "1346981172268}/*.arc.gz"))
  (fact "specified arc-paths"
    (cc/arc-path "some-prefix" [1 2 3])
    =>
    "some-prefix/{1,2,3}/*.arc.gz"))


(deftest arc-file-tests
  (testing "create arc item"
    (let [item (cc/arc-file-item-from-bytes
                "WAT"
                (xio/binary-slurp (io/resource "metafilter.arc")))]
      (is (= (cc/item-mime-type item) "text/html"))
      (is (= (subs (cc/item-text item) 0 30) "<!DOCTYPE HTML PUBLIC \"-//W3C/")))))


(deftest arc-tests
  (fact "ARC tap"
    (<- [?url]
        ((c/first-n
          (cc/hfs-arc-item-tap
           (io/file (io/resource "1262847572760_10.arc.gz")))
          5)
         ?url ?arcitem))
    =>
    (produces
     [["http://1015jamz.com/"]
      ["http://1015jamz.com/CNet-com-Extras-/3602304"]
      ["http://1015jamz.com/High-School-Invasion/2179263"]
      ["http://1015jamz.com/Jasmine/4118295"]
      ["http://1015jamz.com/Suga-Bear/2129607"]])
    (<- [?numurls]
        ((cc/hfs-arc-item-tap
          (io/file (io/resource "1262847572760_10.arc.gz")))
         ?url ?arcitem)
        (c/count :> ?numurls))
    =>
    (produces
     [[11023]]))
  (fact "mime-type"
    (<- [?mimetype ?n]
        ((cc/hfs-arc-item-tap
          (io/file (io/resource "1262847572760_10.arc.gz")))
         ?url ?arcitem)
        (cc/item-mime-type ?arcitem :> ?mimetype)
        (c/count :> ?n)
        )
    =>
    (produces
     [["text/html" 10134]
      ["application/x-javascript" 304]
      ["text/plain" 240]
      ["application/pdf" 172]
      ["text/xml" 48]
      ["application/javascript" 44]
      ["text/javascript" 42]
      ["text/css" 12]
      ["application/vnd.ms-excel" 9]
      ["application/xml" 5]
      ["application/atom+xml" 4]
      ["application/rss+xml" 3]
      ["text/js" 3]
      ["application/rdf+xml" 2]
      ["application/opensearchdescription+xml" 1]]))
  (fact "item-text"
    (<- [?prefix]
        ((c/first-n
          (cc/hfs-arc-item-tap
           (io/file (io/resource "1262847572760_10.arc.gz")))
          5)
         ?url ?arcitem)
        (cc/item-text :< ?arcitem :> ?body)
        (subs ?body 0 25 :> ?prefix)
        )
    =>
    (produces
     [["<html>\r\n\r\n<head>\r\n<title>"]
      ["<html>\r\n\r\n<head>\r\n<title>"]
      ["<html>\r\n\r\n<head>\r\n<title>"]
      ["<html>\r\n\r\n<head>\r\n<title>"]
      ["<html>\r\n\r\n<head>\r\n<title>"]])))


(deftest another-test
  (fact "aggregator?"
    (let [tap [["1" "a"]
               ["1" "b"]
               ["2" "c"]]]
      (<- [?url ?word]
          (tap :> ?url ?word))
      =>
      (produces [[]]))))
