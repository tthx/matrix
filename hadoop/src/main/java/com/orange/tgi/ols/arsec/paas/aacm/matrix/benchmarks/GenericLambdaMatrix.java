package com.orange.tgi.ols.arsec.paas.aacm.matrix.benchmarks;

import java.util.Random;
import java.util.function.BiFunction;
import java.util.function.Function;

public class GenericLambdaMatrix<T extends Number> {
	private int n, m;
	private T data[][];

	public GenericLambdaMatrix(final int n, final int m, final boolean init,
			final BiFunction<Integer, Integer, T[][]> newDataFunction,
			final Function<Random, T> randomDataFunction) {
		Random r = new Random();
		this.n = n;
		this.m = m;
		data = newDataFunction.apply(n, m);
		if (init)
			for (int i = 0; i < n; i++)
				for (int j = 0; j < m; j++)
					data[i][j] = randomDataFunction.apply(r);
	}

	public static <T extends Number> GenericLambdaMatrix<T> multiply(
			final GenericLambdaMatrix<T> x, final GenericLambdaMatrix<T> y,
			final BiFunction<Integer, Integer, T[][]> newDataFunction,
			final BiFunction<T, T, T> sum, final BiFunction<T, T, T> mult) {
		GenericLambdaMatrix<T> r = new GenericLambdaMatrix<T>(x.n, y.m, false,
				newDataFunction, null);
		for (int i = 0; i < r.n; i++)
			for (int j = 0; j < r.m; j++) {
				T a = mult.apply(x.data[i][0], y.data[0][j]);
				for (int k = 1; k < x.m; k++)
					a = sum.apply(a, mult.apply(x.data[i][k], y.data[k][j]));
				r.data[i][j] = a;
			}
		return r;
	}
}
