(ns jepsen.etcd.nemesis
  "Nemeses for etcd"
  (:require [clojure.pprint :refer [pprint]]
            [clojure.tools.logging :refer [info warn]]
            [jepsen [nemesis :as n]
                    [generator :as gen]
                    [net :as net]
                    [util :as util]]
            [jepsen.nemesis.time :as nt]
            [jepsen.nemesis.combined :as nc]
            [jepsen.etcd.db :as db]))

(defn nemesis-package
  "Constructs a nemesis and generators for etcd."
  [opts]
  (let [opts (update opts :faults set)]
    (-> (nc/nemesis-packages opts)
        ;(concat [(member-package opts)])
        (->> (remove nil?))
        nc/compose-packages)))
