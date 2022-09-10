package ru.hermes.msgbus.telegramm;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.ProxySelector;
import java.net.URI;
import java.net.URLEncoder;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.concurrent.TimeUnit;


import org.slf4j.Logger;

import java.net.http.HttpClient;
import java.net.http.HttpHeaders;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;

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
                return ;
        }
        return ;
    }

}
