package ru.hermes.msgbus.ws.annotation;

import java.lang.annotation.*;

/**
 * @author: Tom Bujok (tom.bujok@gmail.com)
 */
@Documented
@Target(value = ElementType.TYPE)
@Retention(value = RetentionPolicy.CLASS)
public @interface ThreadSafe {
}
