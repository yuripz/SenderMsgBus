package net.plumbing.msgbus.config;

import org.eclipse.jetty.server.*;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.env.Environment;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;

//import org.springframework.boot.web.embedded.jetty.JettyServletWebServerFactory;
//import org.springframework.boot.jetty.servlet.JettyServletWebServerFactory;
//import org.springframework.boot.web.servlet.server.ServletWebServerFactory;
// ПРАВИЛЬНО для Spring Boot 4.1.1
/*
import org.springframework.boot.web.server.servlet.ServletWebServerFactory;
import org.springframework.context.annotation.Bean;
import org.springframework.web.context.WebApplicationContext;
import org.springframework.web.context.support.AnnotationConfigWebApplicationContext;
import org.springframework.web.servlet.DispatcherServlet;
import org.springframework.boot.web.servlet.ServletRegistrationBean;
import org.springframework.web.context.support.GenericWebApplicationContext;*/

@Configuration
//@ComponentScan(basePackages = "net.plumbing.msgbus.*")
public class Sender_AppConfig {

    private static final Logger AppConfig_log = LoggerFactory.getLogger(Sender_AppConfig.class);

/*
    @Bean
    public DispatcherServlet dispatcherServlet(ApplicationContext applicationContext ) {
        DispatcherServlet servlet = new DispatcherServlet();
        // Напрямую связываем сервлет с главным контекстом, где сканируются @RestController
        servlet.setApplicationContext(applicationContext);
        // Явно указываем Спрингу брать конфигурацию из Classpath корня
        servlet.setContextConfigLocation("classpath:dispatcherServlet-servlet.xml");
        servlet.setPublishContext(false);
        return servlet;

    }

    @Bean
    public ServletRegistrationBean<DispatcherServlet> dispatcherServletRegistration(DispatcherServlet dispatcherServlet) {
        // Используем "/*" чтобы дикие маски вида /** корректно отлавливались
        org.springframework.boot.web.servlet.ServletRegistrationBean<DispatcherServlet> servletRegistrationBean =
                new org.springframework.boot.web.servlet.ServletRegistrationBean<>(dispatcherServlet, "/*");
        servletRegistrationBean.setName("dispatcherServlet");
        servletRegistrationBean.setLoadOnStartup(1);
        return servletRegistrationBean;

    }
    @Bean
    public JettyServletWebServerFactory  servletWebServerFactory( Environment applicationEnv ) {
        JettyServletWebServerFactory jettyServletFactory = new JettyServletWebServerFactory();
        // Здесь можно явно задать порт из пропертей, если Спринг его игнорирует:
        // Читаем server.port или ставим дефолт 8118
        int port = applicationEnv.getProperty("server.port", Integer.class, 8118);
        jettyServletFactory.setPort(port);
        AppConfig_log.info("servletWebServerFactory for ServletWebServerFactory setPort {}", port);
        //jettyServletFactory.setPort(8118);
        // Читаем настройки потоков Jetty
        int maxThreads = applicationEnv.getProperty("server.jetty.threads.max", Integer.class, 30);


        jettyServletFactory.addServerCustomizers(server -> {
            // 1. Пул потоков
            org.eclipse.jetty.util.thread.QueuedThreadPool threadPool = server.getBean(org.eclipse.jetty.util.thread.QueuedThreadPool.class);
            if (threadPool != null) {
                // Применяем настройки пула потоков к серверу Jetty
                threadPool.setMaxThreads(maxThreads);
                AppConfig_log.info("servletWebServerFactory for ServletWebServerFactory threadPool.setMaxThreads {}", maxThreads);
            }
            // 2. Размер заголовков
            int requestHeaderSize = applicationEnv.getProperty("server.max-http-request-header-size", Integer.class, 81920);
            int responseHeaderSize = applicationEnv.getProperty("server.jetty.max-http-response-header-size", Integer.class, 81920);

            for (Connector connector : server.getConnectors()) {
                if (connector instanceof ServerConnector) {
                    HttpConnectionFactory connectionFactory = ((ServerConnector) connector)
                            .getConnectionFactory(HttpConnectionFactory.class);
                    if (connectionFactory != null) {
                        HttpConfiguration config = connectionFactory.getHttpConfiguration();
                        config.setRequestHeaderSize(requestHeaderSize);
                        config.setResponseHeaderSize(responseHeaderSize);
                        AppConfig_log.info("servletWebServerFactory for ServletWebServerFactory setResponseHeaderSize {}, setResponseHeaderSize{}", requestHeaderSize, responseHeaderSize);
                    }
                }
            }

            // 3. Access Log для Jetty 12
            if (applicationEnv.getProperty("server.jetty.accesslog.enabled", Boolean.class, true)) {
                String filename = applicationEnv.getProperty("server.jetty.accesslog.filename", "./_Logz/access.log");

                // В Jetty 12 используется CustomRequestLog
                // Формат Extended NCSA
                String format = CustomRequestLog.EXTENDED_NCSA_FORMAT;
                boolean append = applicationEnv.getProperty("server.jetty.accesslog.append", Boolean.class, false);

                // Используем AsyncRequestLogWriter, который умеет работать с файлами
                AsyncRequestLogWriter writer = new AsyncRequestLogWriter();
                writer.setFilename(filename);
                writer.setAppend(append);

                CustomRequestLog requestLog = new CustomRequestLog(writer, format);
                server.setRequestLog(requestLog);
                AppConfig_log.info("servletWebServerFactory for ServletWebServerFactory setRequestLog to `{}`", filename );
            }
        });
        // 4. Forward Headers Strategy
        String strategy = applicationEnv.getProperty("server.forward-headers-strategy", "native");
        if ("native".equals(strategy)) {
            jettyServletFactory.setUseForwardHeaders(true);
            AppConfig_log.info("servletWebServerFactory for ServletWebServerFactory setUseForwardHeaders {}", true );
        }
        AppConfig_log.info("servletWebServerFactory for ServletWebServerFactory done");
        return jettyServletFactory;
    }
*/

//    @Autowired
//    private  TaskPollProperties taskPollProperties ;
    @Bean(name = "taskExecutor")
    public ThreadPoolTaskExecutor taskExecutor() {
        ThreadPoolTaskExecutor pool = new ThreadPoolTaskExecutor();
//        pool.setCorePoolSize(taskPollProperties.getcorePoolSize());
//        pool.setMaxPoolSize(taskPollProperties.getmaxPoolSize());
        pool.setCorePoolSize(500);
        pool.setMaxPoolSize(503);
        pool.setWaitForTasksToCompleteOnShutdown(true);
        pool.setThreadNamePrefix("Sender-");
        AppConfig_log.info( "taskExecutor: getThreadNamePrefix:" + pool.getThreadNamePrefix() );

        AppConfig_log.info("ThreadPoolTaskExecutor for taskExecutor prepared: CorePoolSize(500), MaxPoolSize(503); ");
        return pool;
    }

}
