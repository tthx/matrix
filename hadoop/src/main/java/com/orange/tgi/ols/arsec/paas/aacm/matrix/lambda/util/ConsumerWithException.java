package com.orange.tgi.ols.arsec.paas.aacm.matrix.lambda.util;

/**
 * Une interface permettant d'etendre l'interface
 * {@link java.util.function.Consumer} aux exceptions
 */
@FunctionalInterface
public interface ConsumerWithException<T, E extends Exception> {
  void apply(T x) throws E;
}
