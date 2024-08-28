package net.plumbing.msgbus.telegramm;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.ProxySelector;
import java.net.URI;
import java.net.URLEncoder;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import java.net.http.HttpClient;
import java.net.http.HttpHeaders;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;

import static net.plumbing.msgbus.threads.utils.MessageUtils.stripNonValidXMLCharacters;

public class NotifyByChannel {
    // https://api.telegram.org/bot1450268713:AAGMgWJ1ET91dvY5KofxNfXJBRJ_iFpTqZo/sendMessage
    // ?chat_id=-1001328897633
    // &text=*bold text* normal-text _italic text_ ```pre-formatted Ext-fixed-width code block```
    // &parse_mode=Markdown


    private static String ChatBotUrl= null;
    public static void Telegram_setChatBotUrl( String ChatBotUrlProperties, Logger sendMessage_log) {
        ChatBotUrl= ChatBotUrlProperties;
        sendMessage_log.warn("Telegram_setChatBotUrl to `"+ ChatBotUrlProperties + "`");
    }

    private static final HttpClient httpClient = HttpClient.newBuilder()
            .version(HttpClient.Version.HTTP_1_1)
            .connectTimeout(Duration.ofSeconds(10))
            .build();

    public static void Telegram_sendMessage( String message4telegramm, Logger sendMessage_log) {
        if ( ChatBotUrl == null) {
            sendMessage_log.warn( "Telegram_SendMessage ChatBotUrl => NULL !" );
            return ;
        }

        if ( ! ChatBotUrl.startsWith("http") ) {
            sendMessage_log.warn( "Telegram_SendMessage ChatBotUrl != http !" );
            return ;
        }
        String URI_Sring=null;
        //
        try {
        // URI_Sring = "https://jsonplaceholder.typicode.com/users/2";
            URI_Sring = ChatBotUrl +
                    //"https://api.telegram.org/bot1450268713:AAGMgWJ1ET91dvY5KofxNfXJBRJ_iFpTqZo/sendMessage" +
                    //        "?chat_id=-1001328897633&text=" +
                            URLEncoder.encode( message4telegramm, StandardCharsets.UTF_8) + "&parse_mode=Markdown";
         //   URI_Sring="http://httpbin.org/get";
         //  URI_Sring = "https://172.64.200.15:443/users/2";
       //  URI_Sring =  "https://api.megaindex.ru/scan_mail_position?user=megaindex-api-test@megaindex.ru&password=123456&lr=213&request=%D0%9F%D0%BB%D0%B8%D1%82%D0%BA%D0%B0&show_title=1";

            HttpRequest request = HttpRequest.newBuilder()
                    .GET()
                    //.uri(URI.create("https://httpbin.org/get"))
                    .uri(URI.create(URI_Sring))
                    .setHeader("User-Agent", "Java-21 HttpClient") // add request header
                    .build();

            HttpResponse<String> response = httpClient
                    .send(request, HttpResponse.BodyHandlers.ofString());
            sendMessage_log.warn( "URI_Sring:[" + URI_Sring + "] sent"  );
            // print response headers
            HttpHeaders headers = response.headers();
            // headers.map().forEach((k, v) -> sendMessage_log.warn(k + ":" + v));
            sendMessage_log.warn( "Telegram_SendMessage response => " + response.body() );
        }
        catch ( InterruptedException | IOException | IllegalArgumentException  e) {
                e.printStackTrace();
                sendMessage_log.error( "URI_Sring:[" + URI_Sring + "] fault" + e.toString() );
                return ;
        }
        return ;
    }
    /*
    public static CloseableHttpClient getCloseableHttpClient( // MessageQueueVO messageQueueVO, MessageDetails Message , TheadDataAccess theadDataAccess,
                                                      PoolingHttpClientConnectionManager syncConnectionManager,
                                                      Logger MessegeReceive_Log) {
        int ReadTimeoutInMillis = 120 * 1000;
        int ConnectTimeoutInMillis = 5 * 1000;
        SSLContext sslContext = MessageHttpSend.getSSLContext( );
        if ( sslContext == null ) {
            MessegeReceive_Log.error("[" +"] " + "SSLContextBuilder fault: ("  + ")");
            return null;
        }

        /// это в вызывающем методе !- syncConnectionManager = new PoolingHttpClientConnectionManager();
        syncConnectionManager.setMaxTotal((Integer) 4);
        syncConnectionManager.setDefaultMaxPerRoute((Integer) 2);
        RequestConfig rc;

        rc = RequestConfig.custom()
                .setConnectionRequestTimeout(ConnectTimeoutInMillis)
                .setConnectTimeout(ConnectTimeoutInMillis)
                .setSocketTimeout( ReadTimeoutInMillis)
                .build();

        HttpClientBuilder httpClientBuilder = HttpClientBuilder.create()
                .disableDefaultUserAgent()
                .disableRedirectHandling()
                .disableAutomaticRetries()
                .setUserAgent("Mozilla/5.0")
                .setSSLContext(sslContext)
                .disableAuthCaching()
                .disableConnectionState()
                .disableCookieManagement()
                // .useSystemProperties() // HE-5663  https://stackoverflow.com/questions/5165126/without-changing-code-how-to-force-httpclient-to-use-proxy-by-environment-varia
                .setConnectionManager(syncConnectionManager)
                .setSSLHostnameVerifier(new NoopHostnameVerifier())
                .setConnectionTimeToLive( 120 + 5, TimeUnit.SECONDS)
                .evictIdleConnections((long) (120 + 5)*2, TimeUnit.SECONDS);
        httpClientBuilder.setDefaultRequestConfig(rc);

        CloseableHttpClient
                ApiRestHttpClient = httpClientBuilder.build();
        if ( ApiRestHttpClient == null) {
            try {
                syncConnectionManager.shutdown();
                syncConnectionManager.close();
            } catch ( Exception e) {
                MessegeReceive_Log.error("["  +"] " + "Внутренняя ошибка - httpClientBuilder.build() не создал клиента. И ещё проблема с syncConnectionManager.shutdown()...");
                System.err.println("["  +"] " + "Внутренняя ошибка - httpClientBuilder.build() не создал клиента. И ещё проблема с syncConnectionManager.shutdown()..." + e.getMessage()); //e.printStackTrace();
            }
            MessegeReceive_Log.error("["  +"] Внутренняя Ошибка  " + "httpClientBuilder.build() fault");

            return null;
        }
        return ApiRestHttpClient  ;

    }
    */

    /******
    public static int  test_Post (Logger MessageSend_Log ) {

        String EndPointUrl = "http://10.42.111.242:80/integration/api/v2/germes/UpdateO2OCRMsub";

        int ConnectTimeoutInMillis = 11 * 1000;
        int ReadTimeoutInMillis = 30 * 1000;
        String RestResponse;
        String AckXSLT_4_make_JSON = null;

        kong.unirest.HttpResponse<byte[]> Response;

        Integer restResponseStatus;

        byte[] RequestBody;

        PoolingHttpClientConnectionManager syncConnectionManager = new PoolingHttpClientConnectionManager();
        CloseableHttpClient
                ApiRestHttpClient = getCloseableHttpClient(  // messageQueueVO,  Message ,  theadDataAccess,
                syncConnectionManager,
                MessageSend_Log);
        if (ApiRestHttpClient == null)
        {
            syncConnectionManager.shutdown();
            syncConnectionManager.close();
            return -36;
        }

        try {

            String XML_MsgSEND = "<UpdateO2OCRMsub><from>HRMS</from><to>CRM B2O</to><id>24427010</id><number>1085</number><subProcessId>119</subProcessId><subId>1586669</subId><stageSubName>Выполнение СМР</stageSubName><workResultList><workResult><workId>30195.1</workId><workName>Комплекс работ Подряд</workName><workType>СМР</workType><costItem>OPEX</costItem><workMethod>Подрядный способ</workMethod><unit>рубль</unit><amount>25000</amount><costUnits>25000</costUnits><priceUnit>1</priceUnit></workResult></workResultList></UpdateO2OCRMsub>";
        Map<String, String> httpHeaders = new HashMap<>();
        String headerParams[];
        httpHeaders.put("User-Agent", "msgBus/Java-17");

        httpHeaders.put("Connection", "close");
        if (AckXSLT_4_make_JSON != null) {
            httpHeaders.put("Content-Type", "text/json;charset=UTF-8");
        } else {
            httpHeaders.put("Content-Type", "text/xml;charset=UTF-8");
        }


        MessageSend_Log.info("[" + "] sendPostMessage.Unirest.post `" + "` httpHeaders.size=" + httpHeaders.size());
        //+                 "; headerParams= " + headerParams.toString() );


        RequestBody = XML_MsgSEND.getBytes(StandardCharsets.UTF_8);


        try {
            Unirest.config().httpClient(ApiRestHttpClient);
            /*Unirest.config().socketTimeout(ReadTimeoutInMillis)
                    .connectTimeout(ConnectTimeoutInMillis)
                    .setDefaultHeader("Accept", "application/xml")
                    .followRedirects(true)
                    .enableCookieManagement(true)
                    .automaticRetries(false)
                    .verifySsl(false)
            ;

             *//*

            MessageSend_Log.info("[" + "]" + "sendPostMessage.Unirest.post(" + EndPointUrl + ").connectTimeoutInMillis=" + ConnectTimeoutInMillis +
                    ";.readTimeoutInMillis=ReadTimeoutInMillis= " + ReadTimeoutInMillis + " PropUser:");


            Response =
                    Unirest.post(EndPointUrl)
                            .headers(httpHeaders) // возможно несколько заголовков!
                            .body(RequestBody)
                            .asBytes()
            ; //.getRawBody();//.asString() //.getBody();
            // Response = httpResponse.getBody();
            restResponseStatus = Response.getStatus();
            byte[] Test;
            Test = Response.getBody();
            Headers headers = Response.getHeaders();
            MessageSend_Log.warn("[" + "]" + "sendPostMessage.Response getHeaders()=" + headers.all().toString() + " getHeaders().size=" + headers.size());

            MessageSend_Log.warn("[" + "]" + "sendPostMessage.Response httpCode=" + restResponseStatus + " getBody().length=" + Response.getBody().length);
            MessageSend_Log.warn("[" + "]" + "sendPostMessage.Response getBody()=" + Arrays.toString(Test) + " getBody().length=" + Test.length);


            // перекодируем ответ из кодировки, которая была указана в шаблоне для внешней системы в UTF_8 RestResponse = Response.getBody();
            try {
                RestResponse = stripNonValidXMLCharacters(IOUtils.toString(Response.getBody(), "UTF-8")); // StandardCharsets.UTF_8);
                MessageSend_Log.warn("[" + "]" + "sendPostMessage.Response RestResponse=`" + RestResponse + "` getBody().length=" + RestResponse.length());

            } catch (Exception ioExc) {
                String PropEncoding_Out;
                PropEncoding_Out = "UTF_8";

                System.err.println("[" + "] IOUtils.toString.UnsupportedEncodingException: Encoding `" + PropEncoding_Out + "`");
                ioExc.printStackTrace();
                MessageSend_Log.error("[" + "] IOUtils.toString from `" + PropEncoding_Out + "` to_UTF_8 fault:" + ioExc);

                return -1;
            }
            /**//*


        } catch (UnirestException e) {
            System.err.println("[" + "]  Exception");
            e.printStackTrace();
            MessageSend_Log.error("[" + "]" + "sendPostMessage.Unirest.post (" + EndPointUrl + ") fault, UnirestException:" + e);
        }
     }    catch ( Exception allE) {
            try {
                ApiRestHttpClient.close();

            } catch ( java.io.IOException IOE ) {
                MessageSend_Log.error("[" + "]" + "sendPostMessage.Unirest.ApiRestHttpClient.close fault, UnirestException:" + IOE);
            }
             ApiRestHttpClient = null;
            try {
            syncConnectionManager.shutdown();
            syncConnectionManager.close();
            } catch ( Exception anyE ) {
                MessageSend_Log.error("[" + "]" + "sendPostMessage.Unirest.syncConnectionManager.close fault, UnirestException:" + anyE);
            }
            syncConnectionManager = null;

      } finally {
            MessageSend_Log.warn("[" + "]" + "sendPostMessage.Unirest.ApiRestHttpClient.close finally" );
            if (ApiRestHttpClient != null)
                try {
                    ApiRestHttpClient.close();

                } catch ( java.io.IOException IOE ) {
                    MessageSend_Log.error("[" + "]" + "sendPostMessage.Unirest.ApiRestHttpClient.close finally fault, UnirestException:" + IOE);
                }
            ApiRestHttpClient = null;
            if (syncConnectionManager != null)
            try {
                syncConnectionManager.shutdown();
                syncConnectionManager.close();
            } catch ( Exception anyE ) {
                MessageSend_Log.error("[" + "]" + "sendPostMessage.Unirest.syncConnectionManager.close finally fault, UnirestException:" + anyE);
            }
            syncConnectionManager = null;
      }

        return 0;

    }
    */
}
