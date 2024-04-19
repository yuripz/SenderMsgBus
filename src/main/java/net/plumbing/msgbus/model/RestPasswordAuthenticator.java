package net.plumbing.msgbus.model;

import java.net.Authenticator;
import java.net.PasswordAuthentication;

public class RestPasswordAuthenticator {
    protected Authenticator getPasswordAuthenticator( String PropUserLogin, String PropUserPswd )
    {
        Authenticator authenticator;
        if ( PropUserLogin != null && PropUserPswd !=null ) {
            char[] xPassCharArray = new char[PropUserPswd.length()];
            for (int i = 0; i < PropUserPswd.length(); i++) {
                xPassCharArray[i] = PropUserPswd.charAt(i);
            }
            authenticator = new Authenticator() {
                @Override
                protected PasswordAuthentication getPasswordAuthentication() {
                    return new PasswordAuthentication(
                            PropUserLogin,
                            xPassCharArray // messageDetails.MessageTemplate4Perform.getPropUserPswd().toCharArray()
                    );
                }
            };
        }
        else authenticator= null;

        return authenticator;
    }
/*
    protected PasswordAuthentication getPasswordAuthentication(String PropUserLogin, String PropUserPswd) {
        char[] xPassCharArray = new char[ PropUserPswd.length() ];
        for (int i=0 ;i < PropUserPswd.length(); i++)
        {
            xPassCharArray[i]= PropUserPswd.charAt(i);
        }
        return new PasswordAuthentication(
                PropUserLogin,
                xPassCharArray // messageDetails.MessageTemplate4Perform.getPropUserPswd().toCharArray()
        );
    }
 */
}
