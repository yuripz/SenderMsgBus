package net.plumbing.msgbus.controller;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.*;

import jakarta.servlet.ServletRequest;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;

import net.plumbing.msgbus.common.json.JSONObject;
import net.plumbing.msgbus.common.json.XML;
import net.plumbing.msgbus.common.json.JSONException;

import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.sql.SQLException;

import static net.plumbing.msgbus.common.ApplicationProperties.DataSourcePoolMetadata;
import static net.plumbing.msgbus.common.XMLchars.*;

@RestController
public class GetController {
    private static final Logger Controller_log = LoggerFactory.getLogger(GetController.class);

    @GetMapping(path ={ "/**" }, produces = MediaType.ALL_VALUE,  consumes = MediaType.ALL_VALUE)
    @CrossOrigin(origins = "*")
//    @ResponseStatus(HttpStatus.OK)
    @ResponseBody

    public String GetHttpRequest( ServletRequest getServletRequest, HttpServletResponse getResponse) {
        //@PathVariable
        HttpServletRequest httpRequest = (HttpServletRequest) getServletRequest;
        // Controller_log.warn("GetHttpRequest->RemoteAddr: \"" + getServletRequest.getRemoteAddr() + "\" ,RemoteHost: \"" + getServletRequest.getRemoteHost() + "\"" );
        String url = httpRequest.getRequestURL().toString();
        String queryString;
        try {
            queryString = URLDecoder.decode(httpRequest.getQueryString(), StandardCharsets.UTF_8);
        } catch (NullPointerException | IllegalArgumentException e) {
            Controller_log.error("httpRequest.getRequestURL `" + url + "` URLDecoder.decode `" + httpRequest.getQueryString() + " `fault " + e.getMessage());
            //System.err.println("httpRequest.getRequestURL `" + url + "` URLDecoder.decode `" + httpRequest.getQueryString() + " `fault " + e.getMessage());
            //e.printStackTrace();
            queryString = httpRequest.getQueryString();
        }
        getResponse.addHeader("Access-Control-Allow-Origin", "*");
        //Controller_log.warn("url= (" + url + ") queryString(" + queryString + ")");
        Controller_log.warn("httpRequest.getMethod()" + httpRequest.getMethod() + ": url= (" + url + ") queryString(" + queryString + ")");
        String HttpResponse;
        /*
        if ( queryString == null )
        {   getResponse.setStatus(500);
            HttpResponse= Fault_Client_noNS_Begin +
                    "Клиент не передал " +
                    XML.escape(httpRequest.getMethod() + ": url= (" + url + ") queryString(" + queryString + ")" ) +
                    " параметры в запросе" +
                    Fault_noNS_End ;
            Controller_log.warn("HttpResponse:\n" + HttpResponse);

            try {
                JSONObject xmlJSONObj = XML.toJSONObject(HttpResponse);

                String jsonPrettyPrintString = xmlJSONObj.toString(4);
                Controller_log.warn("jsonPrettyPrintString:[" + jsonPrettyPrintString + "]");
                getResponse.setContentType("text/json;Charset=UTF-8");
                return(jsonPrettyPrintString);

            } catch (JSONException e) {
                Controller_log.error( "XML.toJSONObject() fault: {}",e.getMessage() );
                e.printStackTrace();
            }

            return HttpResponse;
        }

         */
        getResponse.setContentType("text/json;Charset=UTF-8");
        HttpResponse = Body_noNS_Begin +
                "<MsgData>Ok</MsgData>" + "<URL>" + url + "</URL>" +
                Body_noNS_End;
        try {
            JSONObject xmlJSONObj = XML.toJSONObject(HttpResponse);

            String jsonPrettyPrintString = xmlJSONObj.toString(4); //StringEscapeUtils.unescapeXml (xmlJSONObj.toString(4) );
            getResponse.setContentType("text/json;Charset=UTF-8");
            return (jsonPrettyPrintString);

        } catch (JSONException e) {
            Controller_log.error( "XML.toJSONObject() fault: {}",e.getMessage() );
            e.printStackTrace();
        }

        return HttpResponse;
    }

}
