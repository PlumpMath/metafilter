(ns com.lemonodor.metafilter.taglines-test
  (:require [cascalog.logic.testing :refer :all]
            [clojure.java.io :as io]
            [clojure.test :refer :all]
            [com.lemonodor.metafilter.taglines :as taglines]
            [midje.sweet :refer :all])
  (:import (org.apache.hadoop.io Text)))


(deftest hostname-tests
  (fact "Hostname parsing"
    (-> "http://zorg.me/give/me/the/stones?q=foo bar" Text. taglines/parse-hostname)
    => "zorg.me"
    (-> "" Text. taglines/parse-hostname) => ""
    (-> "()" Text. taglines/parse-hostname) => ""
    (-> "abc" Text. taglines/parse-hostname) => ""
    (-> "what://wsx/qaz?q=a%20b" Text. taglines/parse-hostname) => "wsx"))

(deftest comment-parsing-tests
  (fact "comments parsing"
    (-> "Can-we-do-that-there-Be-that-here-Check-Equaldex.html"
        io/resource
        slurp
        taglines/get-comments)
    =>
    (list
     (str "They still list gay marriage as being illegal in Arkansas. "
          "Metafilter: Gay marriage legal. I wonder if it's more of a lag "
          "in updating or a conscious decision to wait until all the "
          "appeals, etc., run their course. ")
     "You are not currently logged in. Log in or create a new account")
    (-> "We-dare-to-be-ourselves.html"
        io/resource
        slurp
        taglines/get-comments
        count)
    => 72))

(deftest taglines-tests
  (fact "Proper sentence delimiting"
    (->> "Desperate-Lives-Im-caught-in-the-middle-Desperate-Lives-Uh-huhh.html"
         io/resource
         slurp
         taglines/get-comments
         (map taglines/metafilter-taglines)
         (filter seq))
    =>
    [["MetaFilter: time out from reading Wittgenstein, drinking port and playing backgammon"]
     ["MetaFilter: time out from reading port, drinking backgammon, and playing Wittgenstein."]
     ["MetaFilter: time out from reading port, drinking backgammon, and playing, Wittgenstein."]
     ["Metafilter: Can't we have just one nice thing on the internet instead of this shit?"]]))

(deftest query-tests
  (with-expected-sink-sets [nothing-trapped []]
    (let [metadata-tap
          [[(Text. "http://not-metafilter.com/woo")
            (Text.
             (slurp (io/resource "Can-we-do-that-there-Be-that-here-Check-Equaldex.html")))]
           [(Text. "http://www.metafilter.com/123/woo")
            (Text.
             (slurp (io/resource "Can-we-do-that-there-Be-that-here-Check-Equaldex.html")))]
           [(Text. "http://www.metafilter.com/123/woo")
            (Text. (slurp (io/resource "We-dare-to-be-ourselves.html")))]]]
      ;; (fact "the TLD query works all right"
      ;;   (taglines/query-taglines metadata-tap nothing-trapped) =>
      ;;   (produces
      ;;    [["MetaFilter: ruining it with unseemly jackin-it motions." 1]
      ;;     ["MetaFilter: unnecessary noodle fuck" 1]
      ;;     ["Metafilter: Gay marriage legal." 1]
      ;;     ["Metafilter: fucking terrible (and probably terrible for fucking, too.)" 2]]))
      )))
