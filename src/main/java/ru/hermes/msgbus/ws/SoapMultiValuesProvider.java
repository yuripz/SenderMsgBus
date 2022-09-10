package ru.hermes.msgbus.ws;

import javax.xml.namespace.QName;
import java.util.Set;

/**
 * @author Tom Bujok
 * @since 1.0.0
 */
public interface SoapMultiValuesProvider {

    Set<String> getMultiValues(QName name);

}
