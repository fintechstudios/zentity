package io.zentity.common;

import io.zentity.common.FunctionalUtil.UnCheckedSupplier;
import org.elasticsearch.SpecialPermission;
import org.elasticsearch.common.CheckedSupplier;

import java.security.AccessController;
import java.security.PrivilegedAction;
import java.util.function.Supplier;

public class SecurityUtil {
    /**
     * Run a function with escalated privileges.
     *
     * @param func The function to run.
     * @param <T> The function's return type.
     * @return The result.
     */
    public static <T> T doPrivileged(Supplier<T> func) {
        SecurityManager manager = System.getSecurityManager();
        // if security is disabled, just run the function
        if (manager == null) {
            return func.get();
        }

        SpecialPermission.check();
        return  AccessController.doPrivileged((PrivilegedAction<T>) func::get);
    }

    /**
     * Run a function, that might throw a checked exception, with escalated privileges.
     *
     * @param func The function to run.
     * @param <T> The function's return type.
     * @return The result.
     */
    public static <T> T doPrivileged(CheckedSupplier<T, ?> func) {
        return doPrivileged(UnCheckedSupplier.from(func));
    }
}
