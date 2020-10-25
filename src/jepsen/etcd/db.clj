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
(def logfile "/tmp/keta.log")
(def pidfile "/tmp/keta.pid")
(def kafka-version "kafka_2.13-2.6.0")
(def kafka-dir (str "/opt/" kafka-version))
(def kafka-logfile "/tmp/kafka.log")
(def kafka-pidfile "/tmp/kafka.pid")
(def maven-version "apache-maven-3.6.3")
(def zk-logfile "/tmp/zk.log")
(def zk-pidfile "/tmp/zk.pid")

(defn start!
  "Starts Keta on the given node. Options:

    :initial-cluster-state    Either :new or :existing
    :nodes                    A set of nodes that will comprise the cluster."
  [node opts]
  (c/su
    (cu/start-daemon!
      {:logfile logfile
       :pidfile pidfile
       :chdir   dir}
      (str dir "/bin/keta-start")
      (str dir "/config/keta.properties"))))

(defn start-kafka!
  "Starts Kafka on the given node"
  [test node]
  (c/su
    (cu/start-daemon!
      {:logfile zk-logfile
       :pidfile zk-pidfile
       :chdir   kafka-dir}
      (str kafka-dir "/bin/zookeeper-server-start.sh")
      (str kafka-dir "/config/zookeeper.properties"))
    (Thread/sleep 5000)
    (jepsen/synchronize test)
    (cu/start-daemon!
      {:logfile kafka-logfile
       :pidfile kafka-pidfile
       :chdir   kafka-dir}
      (str kafka-dir "/bin/kafka-server-start.sh")
      (str kafka-dir "/config/server.properties"))
    (Thread/sleep 5000)
    (jepsen/synchronize test)
    (try (c/exec (str kafka-dir "/bin/kafka-topics.sh") :--create :--topic "_keta_commits"
                 :--replication-factor 5 :--partitions 1 :--config :cleanup.policy=compact
                 :--if-not-exists :--zookeeper "localhost:2181")
         (c/exec (str kafka-dir "/bin/kafka-topics.sh") :--create :--topic "_keta_timestamps"
                 :--replication-factor 5 :--partitions 1 :--config :cleanup.policy=compact
                 :--if-not-exists :--zookeeper "localhost:2181")
         (c/exec (str kafka-dir "/bin/kafka-topics.sh") :--create :--topic "_keta_leases"
                 :--replication-factor 5 :--partitions 1 :--config :cleanup.policy=compact
                 :--if-not-exists :--zookeeper "localhost:2181")
         (c/exec (str kafka-dir "/bin/kafka-topics.sh") :--create :--topic "_keta_kv"
                 :--replication-factor 5 :--partitions 1 :--config :cleanup.policy=compact
                 :--if-not-exists :--zookeeper "localhost:2181")
         (info node "successfully created topics")
         (catch RuntimeException e
           (info node "could not create topics")))
    ))

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

(defn zoo-cfg-servers
  "Constructs a zoo.cfg fragment for servers."
  [test node]
  (->> (zk-node-ids test)
       (map (fn [[n id]]
              (str "server." id "=" (if (= n node) "0.0.0.0" (name n)) ":2888:3888")))
       (str/join "\n")))

(defn db
  "Keta DB. Pulls version from test map's :version"
  []
  (reify
    db/Process
    (start! [_ test node]
      (start! node {}))

    (kill! [_ test node]
      (c/su
        (cu/stop-daemon! "KetaMain" pidfile)
        (Thread/sleep 2000)))

    db/Pause
    (pause!  [_ test node])
    (resume! [_ test node])

    db/Primary
    (setup-primary! [_ test node])

    (primaries [_ test]
      ["n1"])

    db/DB
    (setup! [db test node]
      (let [version (:version test)]
        (install-open-jdk8!)
        (info node "installing Keta")
        (c/su
          (let [url (str "https://downloads.apache.org/kafka/2.6.0/" kafka-version ".tgz")]
            ;(cu/install-archive! url "/opt")
            (c/exec :rm :-rf (c/lit "/tmp/*"))
            (c/cd "/opt"
                  (when-not (cu/exists? kafka-version)
                    (c/exec :wget url :-P "/tmp")
                    (c/exec :tar :-xf (str "/tmp/" kafka-version ".tgz") :-C "/opt")))

            (c/cd "/opt"
                  (when-not (cu/exists? maven-version)
                    (c/exec :wget (str "https://mirrors.sonic.net/apache/maven/maven-3/3.6.3/binaries/" maven-version "-bin.tar.gz") :-P "/tmp")
                    (c/exec :tar :-xf (str "/tmp/" maven-version "-bin.tar.gz") :-C "/opt")))
            (c/exec :mkdir "/tmp/zookeeper")
            (c/exec :echo (zk-node-id test node) :> "/tmp/zookeeper/myid")
            (c/exec :echo (str (slurp (io/resource "zookeeper.properties"))
                               "\n"
                               (zoo-cfg-servers test node))
                    :> (str kafka-dir "/config/zookeeper.properties"))
            (c/exec :echo
                    (-> "server.properties"
                        io/resource
                        slurp
                        (str/replace "$NODE_NAME" (name node)))
                    :> (str kafka-dir "/config/server.properties"))
            (c/cd "/opt"
                  (when-not (cu/exists? "keta")
                    (c/exec :apt-get :install :-y "git")
                    (c/exec :git :clone "https://github.com/rayokota/keta.git")))
            (c/exec :echo
                    (-> "keta.properties"
                        io/resource
                        slurp
                        (str/replace "$NODE_NAME" (name node)))
                    :> (str dir "/config/keta.properties"))
            (info node "building Keta")
            (c/cd dir
                  (c/exec :git :pull)
                  (c/exec "/opt/apache-maven-3.6.3/bin/mvn" :package :-DskipTests))
            )))
      (start-kafka! test node)
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
      (info node "tearing down Keta")
      (db/kill! db test node)
      (c/su
        (cu/stop-daemon! kafka-pidfile)
        (cu/stop-daemon! zk-pidfile)
        (c/exec :rm :-rf (c/lit "/tmp/*"))))

    db/LogFiles
    (log-files [_ test node]
      [logfile kafka-logfile zk-logfile])))