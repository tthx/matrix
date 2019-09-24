package com.orange.tgi.ols.arsec.paas.aacm.matrix.lambda.util;

/**
 * Une interface permettant d'etendre l'interface
 * {@link java.util.function.Function} aux exceptions
 */
@FunctionalInterface
public interface FunctionWithException<T, R, E extends Exception> {
  R apply(T x) throws E;
}
