(defproject org.clojars.pgdad/jerseyAdminService "1.0.3"
  :description "FIXME: write description"
  :aot [jerseyAdminService.AdminService jerseyAdminService.adm]
  :repl-init jerseyAdminService.AdminService
  :main jerseyAdminService.adm
  :dependencies [[org.clojure/clojure "1.3.0"]
                 [org.clojure/tools.logging "0.2.3"]
                 [org.clojure/data.json "0.1.3"]
                 [log4j/log4j "1.2.16"]
                 [jersey-service/jersey-service "1.0.2"]
                 [jersey-zoo-clj/jersey-zoo-clj "1.0.1"]]
  :dev-dependencies [[org.eclipse.jetty/jetty-servlet "8.1.2.v20120308"]])
