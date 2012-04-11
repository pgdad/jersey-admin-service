(ns jerseyAdminService.AdminService
  (:require [zookeeper :as zk] [clojure.tools.logging :as log] [clojure.string])
  (:use [clojure.data.json :only [json-str]])
  (:use [jerseyzoo.JerseyZooServletContainer :only [getZooConnection]])
  (:import (javax.ws.rs GET POST PathParam Path Produces)
           (jerseyzoo JerseyZooServletContainer))
  (:gen-class))

(definterface EnvGetter
  (^String getEnvs []))

(definterface AppGetter
  (^String getApps [^String env]))

(definterface RegionGetter
  (^String getRegions [^String env ^String app]))

(definterface ServiceGetter
  (^String getServices [^String env ^String app ^String region]))

(definterface ServerGetter
  (^String getServers [^String env ^String app ^String region]))

(definterface ClientRegistrationGetter
  (^String getRegistrations [^String env ^String app ^String service]))

(definterface ClientRegistrator
  (^void addRegistration [^String env ^String app ^String service ^String id]))

(defn- child-nodes
  [connection node]
  (let [children (zk/children connection node)
        c-nodes (map (fn [child]
                       (str node "/" child))
                     children)]
    c-nodes))

(defn- g-child-nodes
  [connection node]
  (let [c-nodes (child-nodes connection node)
        gc-nodes (map (fn [child]
                        (child-nodes connection child))
                      c-nodes)]
    (flatten gc-nodes)))

(defn- gg-child-nodes
  [connection node]
  (let [gc-nodes (g-child-nodes connection node)
        ggc-nodes (map (fn [child]
                         (child-nodes connection child))
                       gc-nodes)]
    (flatten ggc-nodes)))

(defn- ggg-child-nodes
  [connection node]
  (let [ggc-nodes (gg-child-nodes connection node)
        gggc-nodes (map (fn [child]
                          (child-nodes connection child))
                        ggc-nodes)]
    (flatten gggc-nodes)))

(deftype ^{Path "/environments"} EnvGetterImpl []
         EnvGetter
         (^{GET true
            Produces ["application/json"]}
          ^String getEnvs [this]
          (use 'clojure.data.json)
          (use 'jerseyzoo.JerseyZooServletContainer)
          (let [z @(getZooConnection "localhost")
		envs (zk/children z "/services")]
            (println (str "ZOO: " z))
            (json-str envs))))

(deftype ^{Path "/applications/{env}"} AppGetterImpl []
         AppGetter
         (^{GET true
            Produces ["application/json"]}
          ^String getApps [this ^{PathParam "env"} ^String env]
          (use 'clojure.data.json)
          (use 'jerseyzoo.JerseyZooServletContainer)
          (let [z @(getZooConnection "localhost")
		apps (zk/children z (str "/services/" env))]
            (json-str apps))))

(deftype ^{Path "/regions/{env}/{app}"} RegionGetterImpl []
         RegionGetter
         (^{GET true
            Produces ["application/json"]}
          ^String getRegions [this
                              ^{PathParam "env"} ^String env
                              ^{PathParam "app"} ^String app]
          (use 'clojure.data.json)
          (use 'jerseyzoo.JerseyZooServletContainer)
          (let [z @(getZooConnection "localhost")
		regions (zk/children z (str "/services/" env "/" app "/services"))]
            (json-str regions))))

(def nl-pattern (re-pattern "\n"))

(def slash-pattern (re-pattern "/"))

(defn- service-info
  [connection node prefix]
  (let [data (zk/data connection node)
        service-data (:data data)
        data-str (String. service-data "UTF-8")
        data-parts (clojure.string/split data-str nl-pattern)
        s-info (.replaceFirst node prefix "")
        s-info-parts (clojure.string/split s-info slash-pattern)
        name (nth s-info-parts 1)
        major (nth s-info-parts 2)
        minor (nth s-info-parts 3)
        micro (nth s-info-parts 4)]
    
    {name {:instance (nth data-parts 1) :url (nth data-parts 2)
           :major major :minor minor :micro micro}}
    ))

(deftype ^{Path "/services/{env}/{app}/{region}"} ServiceGetterImpl []
         ServiceGetter
         (^{GET true
            Produces ["application/json"]}
          ^String getServices [this
                               ^{PathParam "env"} ^String env
                               ^{PathParam "app"} ^String app
                               ^{PathParam "region"} ^String region]
          (use 'clojure.data.json)
          (use 'jerseyAdminService.AdminService)
          (let [z @(getZooConnection "localhost")
                prefix (str "/services/" env "/" app "/services/" region)
                service-nodes (child-nodes z prefix)
                instances (flatten (map (fn [child]
                                          (ggg-child-nodes z child))
                                        service-nodes))
                services-data (map (fn [serv-node]
                                     (service-info z serv-node prefix))
                                   instances)
                ]
            (json-str services-data))))

(defn- server-info
  [connection node]
  (let [data (zk/data connection node)
        server-data (:data data)
        data-str (String. server-data "UTF-8")
        data-parts (clojure.string/split data-str nl-pattern)
        load (read-string (nth data-parts 1))
        host (nth data-parts 2)]
    {:node node :load load :host host}))

(deftype ^{Path "/servers/{env}/{app}/{region}"} ServerGetterImpl []
         ServerGetter
         (^{GET true
            Produces ["application/json"]}
          ^String getServers [this
                              ^{PathParam "env"} ^String env
                              ^{PathParam "app"} ^String app
                              ^{PathParam "region"} ^String region]
          (use 'clojure.data.json)
          (use 'jerseyAdminService.AdminService)
          (let [z @(getZooConnection "localhost")
                prefix (str "/services/" env "/" app "/servers/" region)
                server-nodes (child-nodes z prefix)
                servers-data (map (fn [serv-node]
                                    (server-info z serv-node))
                                  server-nodes)]
            (json-str servers-data))))

(deftype ^{Path "/clientregistrations/{env}/{app}/{service}"}
    ClientRegistrationGetterImpl []
    ClientRegistrationGetter
    (^{GET true
       Produces ["application/json"]}
     ^String getRegistrations [this
                               ^{PathParam "env"} ^String env
                               ^{PathParam "app"} ^String app
                               ^{PathParam "service"} ^String service]
     (use 'clojure.data.json)
     (use 'jerseyAdminService.AdminService)
     (let [z @(getZooConnection "localhost")
           prefix (str "/clientregistrations/" env "/" app "/" service)
           clients (zk/children z prefix)]
       (json-str clients))))

(deftype ^{Path "/clientregistrations/{env}/{app}/{service}/{id}"}
    ClientRegistratorImpl []
    ClientRegistrator
    (^{POST true
       Consumes ["plain/text"]}
     ^void addRegistration [this
                            ^{PathParam "env"} ^String env
                            ^{PathParam "app"} ^String app
                            ^{PathParam "service"} ^String service
                            ^{PathParam "id"} ^String id]
     (use 'jerseyAdminService.AdminService)
     (let [z @(getZooConnection "localhost")
           node (str "/clientregistrations/" env "/" app "/" service "/" id)]
       (zk/create-all z node :persistent? true))))