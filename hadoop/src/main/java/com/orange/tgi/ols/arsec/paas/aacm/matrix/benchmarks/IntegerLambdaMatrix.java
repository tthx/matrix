package com.orange.tgi.ols.arsec.paas.aacm.matrix.benchmarks;

import java.util.Random;
import java.util.function.BiFunction;

public class IntegerLambdaMatrix {
	private int n, m;
	private Integer data[][];

	public IntegerLambdaMatrix(final int n, final int m, boolean init) {
		Random r = new Random();
		this.n = n;
		this.m = m;
		data = new Integer[n][m];
		if (init)
			for (int i = 0; i < n; i++)
				for (int j = 0; j < m; j++)
					data[i][j] = r.nextInt();
	}

	public static IntegerLambdaMatrix multiply(final IntegerLambdaMatrix x,
			final IntegerLambdaMatrix y,
			final BiFunction<Integer, Integer, Integer> sum,
			final BiFunction<Integer, Integer, Integer> mult) {
		IntegerLambdaMatrix r = new IntegerLambdaMatrix(x.n, y.m, false);
		for (int i = 0; i < r.n; i++)
			for (int j = 0; j < r.m; j++) {
				Integer a = mult.apply(x.data[i][0], y.data[0][j]);
				for (int k = 1; k < x.m; k++)
					a = sum.apply(a, mult.apply(x.data[i][k], y.data[k][j]));
				r.data[i][j] = a;
			}
		return r;
	}
}
