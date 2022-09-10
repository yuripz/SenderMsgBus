package ru.hermes.msgbus.telegramm;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.ProxySelector;
import java.net.URI;
import java.net.URLEncoder;
import java.net.http.HttpClient;
import java.net.http.HttpHeaders;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.concurrent.TimeUnit;

import com.mashape.unirest.http.Unirest;
import com.mashape.unirest.http.exceptions.UnirestException;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.conn.ClientConnectionManager;
import org.apache.http.conn.ssl.NoopHostnameVerifier;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.impl.conn.PoolingHttpClientConnectionManager;
import org.slf4j.Logger;

import javax.net.ssl.HttpsURLConnection;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManager;
import javax.net.ssl.X509TrustManager;
import javax.net.ssl.SSLParameters;

public class NotifyByChannel {
    // https://api.telegram.org/bot1450268713:AAGMgWJ1ET91dvY5KofxNfXJBRJ_iFpTqZo/sendMessage
    // ?chat_id=-1001328897633
    // &text=*bold text* normal-text _italic text_ ```pre-formatted Ext-fixed-width code block```
    // &parse_mode=Markdown
    private static final HttpClient httpClient = HttpClient.newBuilder()
            .version(HttpClient.Version.HTTP_1_1)
            .connectTimeout(Duration.ofSeconds(10))
            .build();

    private static HttpClientBuilder httpClientBuilder  = null;
    private static CloseableHttpClient ApiRestHttpClient = null;
    private static PoolingHttpClientConnectionManager syncConnectionManager = null;

    public static boolean Telegram_sendMessage( String message4telegramm, Logger sendMessage_log) {
       if ( false) {
//           httpClient = HttpClient.newBuilder()
//                   .sslContext(ru.hermes.msgbus.threads.utils.MessageHttpSend.getSSLContext())
//                   .followRedirects(HttpClient.Redirect.ALWAYS)
//                   .connectTimeout(Duration.ofSeconds(10))
//                   .version(HttpClient.Version.HTTP_1_1)
//                   .build();

           // Create a trust manager that does not validate certificate chains
           TrustManager[] trustAllCerts = new TrustManager[]{
                   new X509TrustManager() {
                       public java.security.cert.X509Certificate[] getAcceptedIssuers() {
                           return null;
                       }
                       public void checkClientTrusted(
                               java.security.cert.X509Certificate[] certs, String authType) {
                       }
                       public void checkServerTrusted(
                               java.security.cert.X509Certificate[] certs, String authType) {
                       }
                   }
           };

// Install the all-trusting trust manager
           SSLContext sc = null;

           try {
               sc = SSLContext.getInstance("SSL");
               sc.init(null, trustAllCerts, new java.security.SecureRandom());
               HttpsURLConnection.setDefaultSSLSocketFactory(sc.getSocketFactory());
               SSLContext.setDefault( sc );
           } catch (Exception e) {
           }
           //SSLParameters sslParams = SSLParameters();
           // This should prevent host validation
           //sslParams.endpointIdentificationAlgorithm = "";

//           httpClient = HttpClient.newBuilder()
//                   .connectTimeout(Duration.ofMillis( 12 * 1000))
//                    .sslContext(sc) // SSL context 'sc' initialised as earlier
//                   //.sslParameters(parameters) // ssl parameters if overriden
//                   .build();
           sendMessage_log.warn( "Telegram_SendMessage httpClient version=> " + httpClient.version() + " " + httpClient.sslContext());
       }

        String URI_Sring=null;
        //
        try {
        // URI_Sring = "https://jsonplaceholder.typicode.com/users/2";
            URI_Sring =
                    "https://api.telegram.org/bot1450268713:AAGMgWJ1ET91dvY5KofxNfXJBRJ_iFpTqZo/sendMessage" +
                            "?chat_id=-1001328897633&text=" +
                            URLEncoder.encode( message4telegramm, StandardCharsets.UTF_8.toString()) + "&parse_mode=Markdown";
         //   URI_Sring="http://httpbin.org/get";
         //  URI_Sring = "https://172.64.200.15:443/users/2";
       //  URI_Sring =  "https://api.megaindex.ru/scan_mail_position?user=megaindex-api-test@megaindex.ru&password=123456&lr=213&request=%D0%9F%D0%BB%D0%B8%D1%82%D0%BA%D0%B0&show_title=1";

            HttpRequest request = HttpRequest.newBuilder()
                    .GET()
                    //.uri(URI.create("https://httpbin.org/get"))
                    .uri(URI.create(URI_Sring))
                    .setHeader("User-Agent", "Java-11 HttpClient") // add request header
                    .build();

            HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());
            sendMessage_log.warn( "URI_Sring:[" + URI_Sring + "] sent"  );
            // print response headers
            HttpHeaders headers = response.headers();
            // headers.map().forEach((k, v) -> sendMessage_log.warn(k + ":" + v));
            sendMessage_log.warn( "Telegram_SendMessage response => " + response.body() );
        }
        catch ( InterruptedException | IOException | IllegalArgumentException  e) {
                e.printStackTrace();
                sendMessage_log.error( "URI_Sring:[" + URI_Sring + "] fault" + e.toString() );
                return false;
        }
        return true;
    }

    public static boolean Telegram_SendMessage( String message4telegramm, Logger sendMessage_log) {
  /*
        if ( ApiRestHttpClient == null) {
            int ConnectTimeoutInMillis = 5 * 1000;
            int ReadTimeoutInMillis = 40 * 1000;
            int ApiRestWaitTime = 50;
            syncConnectionManager = new PoolingHttpClientConnectionManager();
            syncConnectionManager.setMaxTotal( 4);
            syncConnectionManager.setDefaultMaxPerRoute(2);

            RequestConfig rc;
            rc = RequestConfig.custom()
                    .setConnectionRequestTimeout(ConnectTimeoutInMillis)
                    .setConnectTimeout(ConnectTimeoutInMillis)
                    .setSocketTimeout( ReadTimeoutInMillis)
                    .build();
            httpClientBuilder = HttpClientBuilder.create()
                    .disableDefaultUserAgent()
                    .disableRedirectHandling()
                    .disableAutomaticRetries()
                    .setUserAgent("Mozilla/5.0")
                    .setSSLContext( ru.hermes.msgbus.threads.utils.MessageHttpSend.getSSLContext() )
                    .disableAuthCaching()
                    .disableConnectionState()
                    .disableCookieManagement()
                    // .useSystemProperties() // HE-5663  https://stackoverflow.com/questions/5165126/without-changing-code-how-to-force-httpclient-to-use-proxy-by-environment-varia
                    .setConnectionManager(syncConnectionManager)
                    .setSSLHostnameVerifier(new NoopHostnameVerifier())
                    .setConnectionTimeToLive( ApiRestWaitTime + 5, TimeUnit.SECONDS)
                    .evictIdleConnections((long) (ApiRestWaitTime + 5)*2, TimeUnit.SECONDS);
            httpClientBuilder.setDefaultRequestConfig(rc);
            ApiRestHttpClient = httpClientBuilder.build();

        }
        String xURI_Sring = "https://api.telegram.org/bot1450268713:AAGMgWJ1ET91dvY5KofxNfXJBRJ_iFpTqZo/sendMessage?chat_id=-1001328897633&text=" +
                "*bold text* normal-text  _italic text_ ```pre-formatted Ext-fixed-width code block```" + "&parse_mode=Markdown";
        String URI_Sring=null;
        try {
            URI_Sring =
                    "https://api.telegram.org/bot1450268713:AAGMgWJ1ET91dvY5KofxNfXJBRJ_iFpTqZo/sendMessage" +
                            "?chat_id=-1001328897633&text=" +
                            URLEncoder.encode( message4telegramm, StandardCharsets.UTF_8.toString()) + "&parse_mode=Markdown"
            ;
            Unirest.setHttpClient( ApiRestHttpClient);

            String RestResponse = Unirest.get(URI_Sring)
                            //.queryString("chat_id", "-1001328897633")
                    .asString().getBody();
            sendMessage_log.warn( "Telegram_SendMessage Response=> " + RestResponse);

        }
        catch (  IOException | IllegalArgumentException | UnirestException e) {
            e.printStackTrace();
            sendMessage_log.error( "URI_Sring:[" + URI_Sring + "] fault" + e.toString() );
            return false;
        }

   */
        return true;
    }
//    public static void testSyncGet() throws IOException, InterruptedException {
//        HttpClient client = HttpClient.newHttpClient();
//        HttpRequest request = HttpRequest.newBuilder()
//                .uri(URI.create("https://www.baidu.com"))
//                .build();
//
//        HttpResponse<String> response =
//                client.send(request, HttpResponse.BodyHandlers.ofString());
//
//        System.out.println(response.body());
//    }
}
