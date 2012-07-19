(ns jerseyAdminService.AdminService
  (:require [zookeeper :as zk] [clojure.tools.logging :as log]
            [clojure.string] [clojure.java.io])
  (:use [clj-zoo.serverSession])
  (:use [clojure.data.json :only [json-str]])
  (:use [jerseyzoo.JerseyZooServletContainer :only [getZooConnection]])
  (:import (javax.ws.rs GET POST PUT DELETE FormParam PathParam Path Produces)
           (jerseyzoo JerseyZooServletContainer) (java.util Properties))
  (:gen-class))

(definterface RegionGetter
  (^String getRegions [^String app]))

(definterface ServiceGetter
  (^String getServices [^String region]))

(definterface PassiveServiceGetter
  (^String getPassiveServices [^String region]))

(definterface PassivationRequestor
  (^void requestPassivation [^String region ^String service ^String id]))
;; 
(definterface ActivationRequestor
  (^void requestActivation [^String region ^String service ^String id]))

(definterface ServerGetter
  (^String getServers [^String region]))

(definterface ClientRegistrationGetter
  (^String getRegistrations [^String service]))

(definterface ClientsRegistrationGetter
  (^String getAllRegistrations []))

(definterface ClientRegistrator
  (^void addRegistration [^String service ^String id]))

(definterface CreatePassiveGetter
  (^String getAllCreatePassives []))

(definterface CreatePassive
  (^void addCreatePassive [^String service])
  (^void rmCreatePassive [^String service]))

(definterface CleanUpCliRegs
  (^String cleanUpCliRegs []))

(definterface CleanUpServices
  (^String cleanUpServices []))

(defn- getKeepersFromSysPropsOrPropsFile
  []
  (let [keeper-prop (System/getProperty "keepers")]
    (if keeper-prop
      keeper-prop
      (let [props-url (.getFile
                       (clojure.java.io/resource "jerseyAdminService.properties"))
            props (java.util.Properties.)
            r (clojure.java.io/reader props-url)]
        (. props load r)
        (. props getProperty "keepers")))
    ))

(def getKeepers (memoize getKeepersFromSysPropsOrPropsFile))

(defn- child-nodes
  [connection node]
  (let [exists (zk/exists connection node)
        children (if exists (zk/children connection node) '())
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

(deftype ^{Path "/regions"} RegionGetterImpl []
         RegionGetter
         (^{GET true
            Produces ["application/json"]}
          ^String getRegions [this]
          (use 'clojure.data.json)
          (use 'jerseyzoo.JerseyZooServletContainer)
          (let [f @(getCuratorFramework (getKeepers))
		regions (-> f .getChildren (.forPath "/services"))]
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
    
    {name {:node node :instance (nth data-parts 1) :url (nth data-parts 2)
           :major major :minor minor :micro micro}}
    ))

(deftype ^{Path "/services/{region}"} ServiceGetterImpl []
         ServiceGetter
         (^{GET true
            Produces ["application/json"]}
          ^String getServices [this
                               ^{PathParam "region"} ^String region]
          (use 'clojure.data.json)
          (use 'jerseyAdminService.AdminService)
          (let [f @(getCuratorFramework (getKeepers))
                prefix (str "/services/" region)
                services (-> f .getChildren (.forPath prefix))
                ]
            (json-str services))))

(deftype ^{Path "/passiveservices/{region}"}
    PassiveServiceGetterImpl []
    PassiveServiceGetter
    (^{GET true
       Produces ["application/json"]}
     ^String getPassiveServices [this
                                 ^{PathParam "region"} ^String region]
     (use 'clojure.data.json)
     (use 'jerseyAdminService.AdminService)
     (let [f @(getCuratorFramework (getKeepers))
           prefix ("/passiveservices/" region)
           services (-> f .getChildren (.forPath prefix))
           ]
       (json-str services))))

(defn- server-info
  [connection node]
  (let [data (zk/data connection node)
        server-data (:data data)
        data-str (String. server-data "UTF-8")
        data-parts (clojure.string/split data-str nl-pattern)
        load (read-string (nth data-parts 1))
        host (nth data-parts 2)]
    {:node node :load load :host host}))

(deftype ^{Path "/servers/{region}"} ServerGetterImpl []
         ServerGetter
         (^{GET true
            Produces ["application/json"]}
          ^String getServers [this
                              ^{PathParam "region"} ^String region]
          (use 'clojure.data.json)
          (use 'jerseyAdminService.AdminService)
          (let [f @(getCuratorFramework (getKeepers))
                prefix ("/servers/" region)
                servers (-> f .getChildren (.forPath prefix))]
            (json-str servers))))

(deftype ^{Path "/clientregistrations/{env}/{app}/{service}"}
    ClientRegistrationGetterImpl []
    ClientRegistrationGetter
    (^{GET true
       Produces ["application/json"]}
     ^String getRegistrations [this
                               ^{PathParam "service"} ^String service]
     (use 'clojure.data.json)
     (use 'jerseyAdminService.AdminService)
     (let [f @(getCuratorFramework (getKeepers))
           prefix ("/clientregistrations/" service)
           reg-exists (-> f .checkExists (.forPath prefix))
           clients (if reg-exists (-> f .getChildren (.forPath prefix)) '())]
       (json-str clients))))

(deftype ^{Path "/all-clientregistrations"}
    ClientsRegistrationGetterImpl []
    ClientsRegistrationGetter
    (^{GET true
       Produces ["application/json"]}
     ^String getAllRegistrations [this]
     (use 'clojure.data.json)
     (use 'jerseyAdminService.AdminService)
     (let [f @(getCuratorFramework (getKeepers))
           services-reg-ref (ref {})
           srv-root "/clientregistrations"
           cliregs-exist (-> f .checkExists (.forPath srv-root))
           services (if cliregs-exist
                      (-> f .getChildren (.forPath srv-root))
                      '())]
       (doseq [service services]
         (let [cli-root (str srv-root "/" service)
               clients (-> f .getChildren (.forPath cli-root))]
           (dosync
            (alter services-reg-ref assoc service clients))))
       (dosync
        (alter app-reg-ref assoc app @services-reg-ref))
                (json-str @app-reg-ref))))

(deftype ^{Path "/clientregistrations/{service}/{id}"}
    ClientRegistratorImpl []
    ClientRegistrator
    (^{PUT true
       Consumes ["plain/text"]}
     ^void addRegistration [this
                            ^{PathParam "service"} ^String service
                            ^{PathParam "id"} ^String id]
     (use 'jerseyAdminService.AdminService)
     (let [f @(getCuratorFramework (getKeepers))
           path (str "/clientregistrations/" service "/" id)]
       (-> f .create .creatingParentsIfNeeded (.forPath path)))))

(deftype ^{Path "/createpassive"}
    CreatePassiveGetterImpl []
    CreatePassiveGetter
    (^{GET true
       Produces ["application/json"]}
     ^String getAllCreatePassives [this]
     (use 'clojure.data.json)
     (use 'jerseyAdminService.AdminService)
     (let [f @(getCuratorFramework (getKeepers))
           srv-root "/createpassive"
           srv-root-exists (-> f .checkExists (.forPath srv-root))
           services (if srv-root-exists
                      (-> f .getChildren (.forPath srv-root))
                      '())]
       (json-str services))))

(deftype ^{Path "/createpassive/{service}"}
    CreatePassiveImpl []
    CreatePassive
    (^{PUT true
       Consumes ["plain/text"]}
     ^void addCreatePassive [this
                            ^{PathParam "service"} ^String service]
     (use 'jerseyAdminService.AdminService)
     (let [f @(getCuratorFramework (getKeepers))
           path ("/createpassive/" service)]
       (-> f .create .creatingParentsIfNeeded (.forPath path))))

    (^{DELETE true
       Consumes ["plain/text"]}
     ^void rmCreatePassive [this
                           ^{PathParam "service"} ^String service]
     (use 'jerseyAdminService.AdminService)
     (let [f @(getCuratorFramework (getKeepers))
           path (str "/createpassive/" service)
           cr-passive-exists (-> f .checkExists (.forPath path))]
       (if cr-passive-exists
           (-> f .delete (.forPath path))))))

(deftype ^{Path "/requestpassivation"}
    PassivationRequestorImpl []
    PassivationRequestor
    (^{POST true
       Consumes ["application/x-www-form-urlencoded"]}
     ^void requestPassivation [this
                               ^{FormParm "region"} ^String region
                               ^{FormParm "service"} ^String service
                               ^{FormParam "id"} ^String id]
     (use 'jerseyAdminService.AdminService)
     (let [f @(getCuratorFramework (getKeepers))]
       (ssesion/requestPassivation f region service id)
       nil)
     ))

(deftype ^{Path "/requestactivation"}
    ActivationRequestorImpl []
    ActivationRequestor
    (^{POST true
       Consumes ["application/x-www-form-urlencoded"]}
     ^void requestActivation [this
                               ^{FormParm "region"} ^String region
                               ^{FormParm "service"} ^String service
                               ^{FormParam "id"} ^String id]
     (use 'jerseyAdminService.AdminService)
     (let [f @(getCuratorFramework (getKeepers))]
       (ssesion/requestPassivation f region service id)
       nil)
  ))

(deftype ^{Path "/cleanupclientregistrations/{env}"}
    CleanUpCliRegsImpl []
    CleanUpCliRegs
    (^{GET true
       Consumes ["plain/text"]}
     ^String cleanUpCliRegs [this]
     (use 'jerseyAdminService.AdminService)
     (let [f @(getCuratorFramework (getKeepers))
           service-paths (-> f .getChildren (.forPath "/clientregistrations"))]
       ;; remove all <SERVICE> nodes that have no <CLIENT-ID> children
       (doseq [serv service-paths]
         (let [serv-path (str "/clientregistrations/" serv)])
         (if-not (-> f .getChildren (.forPath serv-path))
           (do
             (-> f .delete (.forPath serv-path))
             (dosync (alter removed-nodes-ref
                            (fn [nodes] (cons serv-path nodes)))))))
       (json-str @removed-nodes-ref))))

(defn- delete-childless-node
  [connection accum-ref node]
  (if-not (zk/children connection node)
    (do
      (zk/delete connection node)
      (dosync
       (alter accum-ref (fn [accum] (cons node accum)))))))

(defn- cleanupservices
  [f active?]
  (let [removed-nodes-ref (ref '())
        s-node (if active? "/services" "/passiveservices")
        s-exists (-> f .checkExists (.forPath s-node))
        regions (if app-exists (-> f .getChildren (.forPath s-node)) '())]
    (if-not (= regions '())
      (doseq [region regions]
        (let [service-parent (str s-node "/" region)
              services (-> f .getChildren (.forPath (str s-node "/" region)))]
          (doseq [service services]
            (let [service-path (str service-parent "/" service)
                  ids (-> f .getChildren (.forPath service-path))]
              (when (= ids '())
                (do
                  (-> f .delete (.forPath service-path))
                  (dosync
                   (alter removed-nodes-ref conj service-path)))))))))
    (json-str @removed-nodes-ref)))

(deftype ^{Path "/cleanup/services"}
    CleanUpServicesImpl []
    CleanUpServices
    (^{GET true
       Consumes ["plain/text"]}
     ^String cleanUpServices [this]
     (use 'jerseyAdminService.AdminService)
     (cleanupservices @(getCuratorFramework (getKeepers)) true)))

(deftype ^{Path "/cleanup/passiveservices"}
    CleanUpPassiveServicesImpl []
    CleanUpServices
    (^{GET true
       Consumes ["plan/text"]}
     ^String cleanUpServices [this]
     (use 'jerseyAdminService.AdminService)
     (cleanupservices @(getCuratorFramework (getKeepers)) false)))
