(ns jerseyAdminService.AdminService
  (:require [zookeeper :as zk])
  (:use [clojure.data.json :only [json-str]])
  (:import (javax.ws.rs GET POST PathParam Path Produces))
)

(definterface EnvGetter
  (^String getEnvs []))

(definterface AppGetter
  (^String getApps [^String env]))

(definterface RegionGetter
  (^String getRegions [^String env ^String app]))

(deftype ^{Path "/environments"} EnvGetterImpl []
  EnvGetter
  (^{GET true
	Produces ["application/json"]}
	^String getEnvs [this]
	(use 'clojure.data.json)
	(let [z (zk/connect "localhost")
		envs (zk/children z "/services")]
		(zk/close z)
		(json-str envs))))

(deftype ^{Path "/applications/{env}"} AppGetterImpl []
  AppGetter
  (^{GET true
	Produces ["application/json"]}
	^String getApps [this ^{PathParam "env"} ^String env]
	(use 'clojure.data.json)
	(let [z (zk/connect "localhost")
		apps (zk/children z (str "/services/" env))]
		(zk/close z)
		(json-str apps))))

(deftype ^{Path "/regions/{env}/{app}"} RegionGetterImpl []
  RegionGetter
  (^{GET true
	Produces ["application/json"]}
	^String getRegions [this ^{PathParam "env"} ^String env
				^{PathParam "app"} ^String app]
	(use 'clojure.data.json)
	(println (str "APP: " app " ENV: " env))
	(let [z (zk/connect "localhost")
		regions (zk/children z (str "/services/" env "/" app "/services"))]
		(zk/close z)
		(json-str regions))))


