package commonj.connector.runtime;

import java.util.Map;

/**
 * Stub interface for IBM Integration Designer DataHandler.
 * This interface is provided by the IBM runtime; this stub exists only for compilation.
 */
public interface DataHandler {
    Object transform(Object source, Class<?> targetClass, Object options) throws DataHandlerException;
    void transformInto(Object source, Object target, Object options) throws DataHandlerException;
    void setBindingContext(Map<String, Object> context);
}
