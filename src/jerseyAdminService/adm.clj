(ns jerseyAdminService.adm
  (:require [clojure.tools.logging :as log])
  (:import [java.net.InetAddress]
           (jerseyservice JerseyServiceServlet)
           (com.sun.jersey.spi.container.servlet ServletContainer)
           (com.sun.jersey.api.core PackagesResourceConfig)
           (com.sun.jersey.spi.container.servlet WebServletConfig)
           (org.eclipse.jetty.servlet ServletContextHandler ServletHolder)
           (org.eclipse.jetty.server Server)
           (org.eclipse.jetty.server.handler HandlerCollection ConnectHandler)
           (org.eclipse.jetty.server.nio SelectChannelConnector)
           (java.util HashMap)
           )
  (:gen-class))

(defn -main
  [keepers env app region name major minor micro port]
  (let [server (Server.)
        myHost (.. java.net.InetAddress getLocalHost getHostName)
        connector (SelectChannelConnector.)
        handlers (HandlerCollection.)
        ]
    (. connector setPort (read-string port))
    (. server addConnector connector)
    (. server setHandler handlers)
    ;; setup jersey servlet
    (log/spy :debug "IN MAIN")
    (let [context (ServletContextHandler.
                   handlers "/" ServletContextHandler/SESSIONS)
          jerseyServlet (ServletHolder.
                         (JerseyServiceServlet. "jerseyAdminService.AdminService" keepers env app region name major minor micro (str "http://" myHost ":" port)))
          handler (ConnectHandler.)]
      (. context addServlet jerseyServlet "/*")
      (. handlers addHandler handler)
      (. server start))))

