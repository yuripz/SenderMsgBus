package net.plumbing.msgbus.common;
import org.slf4j.Logger;


import javax.xml.transform.ErrorListener;
import javax.xml.transform.TransformerException;

public class xlstErrorListener implements ErrorListener
{
    private  Logger xlstError_Log;
    private String XSLTerr;

    @Override
    public void warning(TransformerException exception) throws TransformerException {
        XSLTerr = sStackTracе.strInterruptedException(exception);
        xlstError_Log.error( "ErrorListener.warning: " + XSLTerr);
        exception.printStackTrace();
        throw (exception);
    }

    @Override
    public void error(TransformerException exception) throws TransformerException {
        XSLTerr = sStackTracе.strInterruptedException(exception);
        xlstError_Log.error( "ErrorListener.error: " + XSLTerr);
        exception.printStackTrace();
        throw (exception);
    }

    @Override
    public void fatalError(TransformerException exception) throws TransformerException {
        XSLTerr = sStackTracе.strInterruptedException(exception);
        xlstError_Log.error( "ErrorListener.fatalError: " + XSLTerr);
        exception.printStackTrace();
        throw (exception);
    }
    public void setXlstError_Log( Logger p_Error_Log ){
        this.xlstError_Log = p_Error_Log;
        this.XSLTerr= null;
    }
    public String getXlstError(  ){
        return this.XSLTerr;
    }


}
