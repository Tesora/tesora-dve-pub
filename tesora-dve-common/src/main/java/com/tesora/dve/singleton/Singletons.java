// OS_STATUS: public
package com.tesora.dve.singleton;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Modifier;
import java.util.concurrent.ConcurrentHashMap;

/**
 *
 */
public class Singletons {
    static final Logger logger = LoggerFactory.getLogger(Singletons.class);

    static ConcurrentHashMap<Class,Object> singletons = new ConcurrentHashMap<>();


    public static <S,C extends S> void register(Class<S> interfaceClazz, C concreteImpl){
        innerPut(interfaceClazz, concreteImpl, false);
    }

    /**
     *  This method is like register, but is willing to swap out an existing instance.  We are not protected from clients
     *  that have looked up the previous service and are making calls on it, so this is a bit of a hack.  Currently it is
     *  required for the unit tests to work properly, but should be phased out when possible.
     */
    public static <S,C extends S> void replace(Class<S> interfaceClazz, C concreteImpl){
        innerPut(interfaceClazz, concreteImpl, true);
    }

    private static <S, C extends S> void innerPut(Class<S> interfaceClazz, C concreteImpl, boolean replace) {
        if (!isLegalKey(interfaceClazz))
            throw new IllegalArgumentException("Registry key must be an interface");

        if (!interfaceClazz.isInstance(concreteImpl))
            throw new IllegalArgumentException("Entry must implement the provided interface");

        if (replace)
            singletons.put(interfaceClazz,concreteImpl);
        else {
            Object existing = singletons.putIfAbsent(interfaceClazz,concreteImpl);
            if (existing != null) {
                String message = String.format("Entry for %s already present, concrete class is %s", interfaceClazz.getSimpleName(), existing.getClass().getSimpleName());
                throw new IllegalStateException(message);
            }
        }
    }

    private static <S> boolean isLegalKey(Class<S> interfaceClazz) {
        return interfaceClazz.isInterface() || Modifier.isAbstract(interfaceClazz.getModifiers());
    }

    public static <S> S unregister(Class<S> interfaceClazz){
        if (! interfaceClazz.isInterface() )
            throw new IllegalArgumentException("Registry key must be an interface");

        return interfaceClazz.cast(singletons.remove(interfaceClazz));
    }



    public static <S> S lookup(Class<S> interfaceClazz){
        return innerLookup(interfaceClazz,null, true);
    }

    public static <S> S lookup(Class<S> interfaceClazz, S defaultInstance){
        return innerLookup(interfaceClazz,defaultInstance, true);
    }

    public static <S> S require(Class<S> interfaceClazz){
        return innerLookup(interfaceClazz, null, false);
    }

    public static <S> S require(Class<S> interfaceClazz, S defaultInstance){
        return innerLookup(interfaceClazz,defaultInstance, false);
    }

    private static <S> S innerLookup(Class<S> interfaceClazz, S defaultValue, boolean returnNullIfMissing) {
        Object entry = singletons.get(interfaceClazz);
        if (entry != null) {
            return interfaceClazz.cast(entry);
        } else if (defaultValue != null) {
            return defaultValue;
        } else {
            if (returnNullIfMissing)
                return null;
            else
                throw new IllegalStateException(String.format("Singleton for %s interface was not available",interfaceClazz.getSimpleName()));
        }
    }

}
