(ns jepsen.etcd.db
  "Database setup and automation"
  (:require [clojure.tools.logging :refer [info warn]]
            [clojure.java.io :as io]
            [clojure.string :as str]
            [dom-top.core :refer [real-pmap]]
            [jepsen [control :as c]
                    [core :as jepsen]
                    [db :as db]
                    [util :as util :refer [meh
                                           random-nonempty-subset]]]
            [jepsen.control.util :as cu]
            [jepsen.os.debian :as debian]
            [jepsen.etcd [client :as client]
                         [support :as s]]
            [slingshot.slingshot :refer [throw+ try+]]))

(def dir "/opt/keta")
;(def logfile (str dir "/logs/keta.log"))
(def logfile "/tmp/keta.log")
(def pidfile (str dir "/keta.pid"))
(def kafka-version "kafka_2.13-2.6.0")
(def kafka-dir (str "/opt/" kafka-version))
;(def kafka-logfile (str kafka-dir "/logs/server.log"))
(def kafka-logfile "/tmp/server.log")
(def kafka-pidfile "/tmp/kafka.pid")
(def maven-version "apache-maven-3.6.3")
(def zk-logfile "/tmp/zk.log")
(def zk-pidfile "/tmp/zk.pid")

(defn wipe!
  "Wipes data files on the current node."
  [node]
  (c/su
    (c/exec :rm :-rf (str dir "/" node ".etcd"))))

(defn from-highest-term
  "Takes a test and a function (f node client). Evaluates that function with a
  client bound to each node in parallel, and returns the response with the
  highest term."
  [test f]
  (let [rs (->> (:nodes test)
                (real-pmap (fn [node]
                             (try+
                               (client/remap-errors
                                 (client/with-client [c node] (f node c)))
                               (catch client/client-error? e nil))))
                (remove nil?))]
    (if (seq rs)
      (last (sort-by (comp :raft-term :header) rs))
      (throw+ {:type :no-node-responded}))))

(defn primary
  "Picks the highest primary by term"
  [test]
  (from-highest-term test
                     (fn [node c]
                       (->> (client/member-status c node)
                            :leader
                            (client/member-id->node c)))))

(defn initial-cluster
  "Takes a set of nodes and constructs an initial cluster string, like
  \"foo=foo:2380,bar=bar:2380,...\""
  [nodes]
  (->> nodes
       (map (fn [node]
              (str node "=" (s/peer-url node))))
       (str/join ",")))

(defn start!
  "Starts keta on the given node. Options:

    :initial-cluster-state    Either :new or :existing
    :nodes                    A set of nodes that will comprise the cluster."
  [node opts]
  (c/su
    (cu/start-daemon!
      {:logfile zk-logfile
       :pidfile zk-pidfile
       :chdir   kafka-dir}

      (str kafka-dir "/bin/zookeeper-server-start.sh")
      (str kafka-dir "/config/zookeeper.properties"))
    (cu/start-daemon!
      {:logfile kafka-logfile
       :pidfile kafka-pidfile
       :chdir   kafka-dir}

      (str kafka-dir "/bin/kafka-server-start.sh")
      (str kafka-dir "/config/server.properties"))
    (cu/start-daemon!
      {:logfile logfile
       :pidfile pidfile
       :chdir   dir}

      (str dir "/bin/keta-start")
      (str dir "/config/keta.properties"))))

(defn members
  "Takes a test, asks all nodes for their membership, and returns the highest
  membership based on term."
  [test]
  (->> (from-highest-term test (fn [node client] (client/list-members client)))
       :members))

(defn refresh-members!
  "Takes a test and updates the current cluster membership, based on querying
  nodes presently in the test's cluster."
  [test]
  (let [raw-members (members test)
        members (->> raw-members
                     (map :name)
                     set)]
    (if (some str/blank? members)
      (throw+ {:type    ::blank-member-name
               :members raw-members})
      (do (info "Current membership is" (pr-str members))
          (reset! (:members test) members)))))

(defn addable-nodes
  "What nodes could we add to this cluster?"
  [test]
  (remove @(:members test) (:nodes test)))

(defn grow!
  "Adds a random node from the test to the cluster, if possible. Refreshes
  membership."
  [test]
  ; First, get a picture of who the nodes THINK is in the cluster
  (refresh-members! test)

  ; Can we add a node?
  (if-let [addable-nodes (seq (addable-nodes test))]
    (let [new-node (rand-nth addable-nodes)]
      (info :adding new-node)

      ; Tell the cluster the new node is a part of it
      (client/remap-errors
        (client/with-client [c (rand-nth (vec @(:members test)))]
          (client/add-member! c new-node)))

      ; Update the test map to include the new node
      (swap! (:members test) conj new-node)

      ; And start the new node--it'll pick up the current members from the test
      (c/on-nodes test [new-node] (partial db/start! (:db test)))

      new-node)

    :no-nodes-available-to-add))

(defn shrink!
  "Removes a random node from the cluster, if possible. Refreshes membership."
  [test]
  ; First, get a picture of who the nodes THINK is in the cluster
  (refresh-members! test)
  ; Next, remove a node.
  (if (< (count @(:members test)) 2)
    :too-few-members-to-shrink

    (let [node (rand-nth (vec @(:members test)))]
      ; Ask cluster to remove it
      (let [contact (-> test :members deref (disj node) vec rand-nth)]
        (client/remap-errors
          (client/with-client [c contact]
            (info :removing node :via contact)
            (client/remove-member! c node))))

      ; Kill the node and wipe its data dir; otherwise we'll break the cluster
      ; when it restarts
      (c/on-nodes test [node]
                  (fn [test node]
                    (db/kill! (:db test) test node)
                    (info "Wiping" node)
                    (wipe! node)))

      ; Record that the node's gone
      (swap! (:members test) disj node)
      node)))

(defn install-open-jdk8!
  "Installs open jdk8"
  []
  (c/su
    (c/exec :apt-get :update)
    (c/exec :apt-get :install :-y "apt-transport-https" "ca-certificates" "wget" "dirmngr" "gnupg" "software-properties-common")
    (c/exec :wget "https://adoptopenjdk.jfrog.io/adoptopenjdk/api/gpg/key/public" :-P "/tmp")
    (c/exec :apt-key :add "/tmp/public")
    (c/exec :add-apt-repository :-y "https://adoptopenjdk.jfrog.io/adoptopenjdk/deb/")
    (c/exec :apt-get :update)
    (c/exec :apt-get :install :-y "adoptopenjdk-8-hotspot")
    (c/exec :update-alternatives :--set :java "/usr/lib/jvm/adoptopenjdk-8-hotspot-amd64/bin/java")
    ))

(defn zk-node-ids
  "Returns a map of node names to node ids."
  [test]
  (->> test
       :nodes
       (map-indexed (fn [i node] [node i]))
       (into {})))

(defn zk-node-id
  "Given a test and a node name from that test, returns the ID for that node."
  [test node]
  ((zk-node-ids test) node))

(defn db
  "Etcd DB. Pulls version from test map's :version"
  []
  (reify
    db/Process
    (start! [_ test node]
      (start! node {}))

    (kill! [_ test node]
      (c/su
        (cu/stop-daemon! "KetaMain" pidfile)
        (cu/stop-daemon! kafka-pidfile)
        (cu/stop-daemon! zk-pidfile)))

    db/Pause
    (pause!  [_ test node])
    (resume! [_ test node])

    db/Primary
    (setup-primary! [_ test node])

    (primaries [_ test])

    db/DB
    (setup! [db test node]
      (let [version (:version test)]
        (install-open-jdk8!)
        (info node "installing keta")
        (c/su
          (let [url (str "https://downloads.apache.org/kafka/2.6.0/" kafka-version ".tgz")]
            ;(cu/install-archive! url "/opt")
            (c/exec :rm :-rf (c/lit "/tmp/*"))
            (c/cd "/opt"
                  (when-not (cu/exists? kafka-version)
                    (c/exec :wget url :-P "/tmp")
                    (c/exec :tar :-xf (str "/tmp/" kafka-version ".tgz") :-C "/opt")
                    (c/exec :echo
                            (-> "zookeeper.properties"
                                io/resource
                                slurp)
                            :> (str kafka-dir "/config/zookeeper.properties"))
                    (c/exec :echo
                            (-> "server.properties"
                                io/resource
                                slurp)
                            :> (str kafka-dir "/config/server.properties"))
                    (c/exec :echo (zk-node-id test node) :> "/tmp/zookeeper/myid")))

            (c/cd "/opt"
                  (when-not (cu/exists? maven-version)
                    (c/exec :wget (str "https://mirrors.sonic.net/apache/maven/maven-3/3.6.3/binaries/" maven-version "-bin.tar.gz") :-P "/tmp")
                    (c/exec :tar :-xf (str "/tmp/" maven-version "-bin.tar.gz") :-C "/opt")))
            (c/cd "/opt"
                  (when-not (cu/exists? "keta")
                    (c/exec :apt-get :install :-y "git")
                    (c/exec :git :clone "https://github.com/rayokota/keta.git")
                    (c/exec :echo
                            (-> "keta.properties"
                                io/resource
                                slurp
                                (str/replace "$NODE_NAME" (name node))
                                :> (str dir "/config/keta.properties")))))
            (c/cd dir
                  (c/exec "/opt/apache-maven-3.6.3/bin/mvn" :package :-DskipTests))
            )))
      (db/start! db test node)

      ; Wait for node to come up
      (let [c (client/client node)]
        (try
          (client/await-node-ready c)
          (finally (client/close! c))))

      ; Once everyone's done their initial startup, we set initialized? to
      ; true, so future runs use --initial-cluster-state existing.
      (jepsen/synchronize test)
      (reset! (:initialized? test) true))

    (teardown! [db test node]
      (info node "tearing down keta")
      (db/kill! db test node)
      (c/su (c/exec :rm :-rf (c/lit "/tmp/*"))))

    db/LogFiles
    (log-files [_ test node]
      [logfile])))
