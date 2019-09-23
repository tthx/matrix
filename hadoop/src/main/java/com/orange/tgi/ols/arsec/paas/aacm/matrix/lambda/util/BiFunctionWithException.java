package com.orange.tgi.ols.arsec.paas.aacm.matrix.lambda.util;

/**
 * Une interface permettant d'etendre l'interface
 * {@link java.util.function.BiFunction} aux exceptions
 */
@FunctionalInterface
public interface BiFunctionWithException<T, R, U, E extends Exception> {
	U apply(T x, R y) throws E;
}
