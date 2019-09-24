package com.orange.tgi.ols.arsec.paas.aacm.matrix.lambda.util;

/**
 * Une interface permettant d'etendre l'interface
 * {@link java.util.function.BiConsumer} aux exceptions
 */
@FunctionalInterface
public interface BiConsumerWithException<T, U, E extends Exception> {
  void apply(T x, U y) throws E;
}
