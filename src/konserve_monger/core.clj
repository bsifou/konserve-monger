(ns konserve-monger.core
  (:require [konserve.serializers :as ser]
            [konserve.protocols :refer [-serialize -deserialize]]
            [hasch.core :refer [uuid]]
            [clojure.core.async :as async
             :refer [<!! <! >! put! close! timeout chan alt! go go-loop]]
            [clojure.edn :as edn]

            [monger.core :as mg]
            [konserve.filestore :as fs :refer [new-fs-store]]
            [konserve.core :as k]
                                        ;[com.ashafa.clutch :refer [couch create!] :as cl]
            [konserve.protocols :refer [PEDNAsyncKeyValueStore
                                        -exists? -get-in -update-in
                                        PBinaryAsyncKeyValueStore
                                        -bassoc -bget]]
            [clojure.string :as str]
            [monger.collection :as mc]
            [clojure.test :refer :all])

  (:import org.bson.types.ObjectId
           [com.mongodb DB WriteConcern]
           [java.io ByteArrayOutputStream]
           [java.util UUID]))

(defn key->collection [k]
  (if (keyword? k)
    (apply
     str
     (rest (first (str/split (str k) #"/"))))
    k))

(defn str->keyword [x]
  (apply keyword (rest x)))

;; TODO preserve the namespace of deep nested keyword within document


;; key storeage in document

;; namespaced keywrods get stored as string keywords
;; to put it back we know it by the "/"

;; non namespaced keywords get stored normally

;; UUID as UUID other strings

;; TODO
;; postwalk to stringify
;; postwalk to keywordize


(defn nsed-keyword? [s]
  (= (count (str/split (str s) #"/")) 2))

(defn adapt-keyword-in [k]
  (cond (nsed-keyword? k)
        (apply str (rest (str k)))
        (uuid? k) (str k)
        :else
        k))

(defn adapt-keyword-out [k]
  (try (UUID/fromString (str k))
       (catch Exception e
         #_(println "ERROR KEY IS " k)
         (cond (nsed-keyword? k)
               (keyword k)
               :else
               k))))

(defn adapt-v-in [v]
  (if (and (vector? v)
           (= (count v) 2)
           (nsed-keyword? (first v)))
    (assoc v 0 (adapt-keyword-in (first v)))
    (adapt-keyword-in v)))

(defn
  adapt-value-in [x]
  (clojure.walk/walk (fn [x] (if (vector? x) (adapt-v-in x) (str x))) identity x))

(defn adapt-map-in
  "Recursively transforms all map keys from keywords to strings."
  {:added "1.1"}
  [m]
  (let [f (fn [[k v]] [(adapt-keyword-in k) (adapt-v-in v)])]
    ;; only apply to maps
    (clojure.walk/postwalk (fn [x] (cond (map? x)
                                         (into {} (map f x))
                                         :else x)) m)))

(defn adapt-v-out [v]
  (if (and
       (vector? v)
       (= (count v) 2)
       (nsed-keyword? (first v)))
    (assoc v 0 (adapt-keyword-out (first v)))
    (adapt-keyword-out v)))

(defn
  adapt-value-out [x]
  (clojure.walk/walk (fn [x] (if (vector? x) (adapt-v-out x)  x)) identity x))

(defn clean-top-underscored-keys [m]
  (dissoc m :_key :_is-keyword :_id))

(defn adapt-map-out [m]
  "Recursively transforms all map keys from keywords to strings."
  {:added "1.1"}
  [m]
  (let [f (fn [[k v]] [(adapt-keyword-out k) (adapt-v-out v)])]
    ;; only apply to maps
    (clojure.walk/postwalk (fn [x] (if (map? x) (into {} (map f x)) x)) m)))

(defrecord MongerStore [conn-url conn coll-opts db read-handlers write-handlers serializer locks]
  PEDNAsyncKeyValueStore
  (-exists? [this key]
    (go (let [collection (key->collection key)]
          (mc/any? db collection))))

  ;; the first key acts as collection name
  ;; the second is the id of the document inside with key _id (passed as second parameter)
  ;; the third & so on are the attributes inside the said document with each key
  ;;    (attribute) can lead to other map with keys ... (recursive)
  ;; for values that goes right to the collection on put it in document with _:id and
  ;; key "_val"
  ;; TODO use rkeys for nested keys
  (-get-in [this key-vec]
    #_(go (print keys))
    (let [fkey       (first key-vec)
          collection (key->collection fkey)]

      (go (let [res (mc/find-maps db collection)]
                                        ;res (get-in document rkey)]
                                        ;res
            (apply merge
                   (map
                    (fn [m]
                      (let [m1 (adapt-map-out m)
                            k  (adapt-keyword-out (get m1 :_key))
                            k-2 (adapt-keyword-out (get m1 :_key2))
                            k  (if (:_is-keyword m1) (keyword k) k)
                            result (clean-top-underscored-keys m1)]
                        (if k-2
                          result
                          {k result})))
                    res))))))


  ;; we put document in collection (first arg) with _id (second arg)
  ;; to update it, we pull up that said document and apply update-in
  ;; then rewrite (upsert?)


  (-update-in [this key-vec up-fn up-fn-args]
    ;; handle when we cant find document
    (let [[fkey skey & rkey :as all] key-vec
          ;; skey need to be modifed ...


          id         (str (uuid skey))
                                        ;rkey       (rest all)

          is-kw      (keyword? skey)

          collection (key->collection fkey)
          old        (adapt-map-out (mc/find-one-as-map db collection {:_id id}))
          value?     (:_val old)
                                        ;old    (if value? (:val old) old)
                                        ;new        {:fuckyou "yes"}
          r          (up-fn nil)
                                        ; new {}

          new        (cond
                       (and old value?) (apply update old :_val up-fn up-fn-args)
                       (not (map r)) r
                       (empty? rkey) (apply up-fn old up-fn-args)
                       :else (apply update-in old rkey up-fn up-fn-args))
          ns-k-str   (or (adapt-keyword-in skey) (:_key old) (adapt-keyword-in (ffirst new)))]
      (go
        (mc/update-by-id db collection id (adapt-map-in (assoc new (if skey :_key :_key2) ns-k-str :_is-keyword is-kw))
                         {:upsert true})

        [(get-in (clean-top-underscored-keys old) rkey) (get-in (clean-top-underscored-keys new) rkey)]
        #_(mc/insert db collection (assoc new :_id id)))))
  (-update-in [this key-vec up-fn]
    (-update-in this key-vec up-fn []))

  (-assoc-in [this key-vec val]
    (let [#_#_val (if (map? val) val {:_val val})]
      (-update-in this key-vec (fn [_] val))))

  (-dissoc [this key]
    (go
      (let [collection (key->collection key)]
        (mc/remove db collection)))))

(defn new-monger-store
  [& {:keys [conn-url coll-opts serializer read-handlers write-handlers]
      :or   {serializer     (ser/fressian-serializer)
             read-handlers  (atom {})
             write-handlers (atom {})
             conn-url       "mongodb://127.0.0.1:27017/db0"
             coll-opts      {:name "konserve"
                             :opts {}}}}]

  (go

    (try
      (let [{:keys [conn db]} (mg/connect-via-uri conn-url)]
        (map->MongerStore {:conn-url       conn-url
                           :conn           conn
                           :db             db
                           :coll-opts      coll-opts
                           :read-handlers  read-handlers
                           :write-handlers write-handlers
                           :serializer     serializer
                           :locks          (atom {})}))
      (catch Exception e
        e))))


;; REPL


(def conn-url "mongodb://127.0.0.1:27017/db0")
(def connection (mg/connect-via-uri conn-url))
(def db (:db connection))
(def conn (:conn connection))
(def uuid2 (uuid))
(def store (<!! (new-monger-store)))
(def fs-store (<!! (new-fs-store "/tmp/store")))

(deftest monger-store-test
  (testing "Test the couchdb store functionality."
    (let []
      ;; writes
      
      (<!! (k/dissoc store :account/id))
      (<!! (k/dissoc fs-store :account/id))

      (is (= (<!! (k/assoc-in store [:account/id uuid2] {:account/name "test"}))
             (<!! (k/assoc-in fs-store [:account/id uuid2] {:account/name "test"}))))

      (is (= (<!! (k/assoc-in store [:account/id uuid2 :account/name] "test2"))
             (<!! (k/assoc-in fs-store [:account/id uuid2 :account/name] "test2"))))

      (is (= (<!! (k/get-in store [:account/id uuid2 :account/name]))
             (<!! (k/get-in fs-store [:account/id uuid2 :account/name]))))

      (is (= (<!! (k/get-in store [:account/id uuid2 :account/name]))
             (<!! (k/get-in fs-store [:account/id uuid2 :account/name]))))

      (is (= (<!! (k/update-in store [:account/id uuid2] merge {:account/name "test3"}))
             (<!! (k/update-in fs-store [:account/id uuid2] merge {:account/name "test3"}))))

      (is (= (<!! (k/assoc-in store [:account/id uuid2] {:account/addresses [[:address/id #uuid "ffffffff-ffff-ffff-ffff-000000000001"]]}))
             (<!! (k/assoc-in fs-store [:account/id uuid2] {:account/addresses [[:address/id #uuid "ffffffff-ffff-ffff-ffff-000000000001"]]}))))

      ;; reads

      (is (= (<!! (k/get-in store [:account/id]))
             (<!! (k/get-in fs-store [:account/id]))))

      (is (= (<!! (k/get-in store [:account/id uuid2]))
             (<!! (k/get-in fs-store [:account/id uuid2]))))

      (is (= (<!! (k/get-in store [:account/id uuid2 :account/addresses]))
             (<!! (k/get-in fs-store [:account/id uuid2 :account/addresses])))))))

(comment
  ;; tests for writes

  ;; assoc in

        ;; test for get ins)))


  (<!! (k/get-in store ["item0"]))

  (<!! (k/assoc-in store ["item" :c] {:a 1}))

  (<!! (k/assoc-in store ["item0" (uuid)] {(uuid) 1 :b (uuid)}))
  (<!! (k/get-in store ["item"]))
  (<!! (k/dissoc store "item0"))

  (<!! (k/get-in store ["item"]))

  (<!! (k/assoc-in store ["item" :test] {:a 1 :c 2}))

  ;(<!! (k/get-in store1 ["item"]))


  (<!! (k/update-in store ["item" :my-name/key] 7))

  (<!! (k/get-in store ["item" :my-name/key] 7))

  (<!! (k/assoc-in store ["item0" "item3"] {:b 4}))
  (<!! (k/get-in store1 ["item0"]))
  (<!! (k/exists? store "item0"))
  (<!! (k/dissoc store "item"))

  (<!! (k/assoc-in store ["foo" (uuid)] {:a 3}))
  (<!! (k/assoc-in store ["foo" (uuid)] 33))
  (<!! (k/get-in store ["foo"]))
  (<!! (k/exists? store "foo"))

  (<!! (k/dissoc store1 "foo"))

  (<!! (k/assoc-in store [:bar2] 42))
  (<!! (k/update-in store [:bar2] inc))
  (<!! (k/get-in store [:bar2 :bar3]))

  (<!! (k/append store :error-log {:type :horrible}))
  (<!! (k/log store :error-log))

  (let [ba (byte-array (* 10 1024 1024) (byte 42))]
    (time (<!! (k/bassoc store "banana" ba)))))
