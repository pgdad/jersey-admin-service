(ns jerseyAdminService.adm
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
  [keepers env app region name major minor micro url]
  (let [server (Server.)
        connector (SelectChannelConnector.)
        handlers (HandlerCollection.)
        ]
        (. connector setPort 8787)
        (. server addConnector connector)
        (. server setHandler handlers)
        ;; setup jersey servlet
        (println "IN MAIN")
        (let [context (ServletContextHandler.
                handlers "/" ServletContextHandler/SESSIONS)
                jerseyServlet (ServletHolder.
                                (JerseyServiceServlet. "jerseyAdminService.AdminService" keepers env app region name major minor micro url))
                handler (ConnectHandler.)]
                (. context addServlet jerseyServlet "/*")
                (. handlers addHandler handler)
                (. server start))))

