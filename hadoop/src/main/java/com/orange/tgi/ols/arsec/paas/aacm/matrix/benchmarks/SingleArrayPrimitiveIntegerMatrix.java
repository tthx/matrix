package com.orange.tgi.ols.arsec.paas.aacm.matrix.benchmarks;

import java.util.Random;

public class SingleArrayPrimitiveIntegerMatrix {
	private int n, m;
	private int data[];

	public SingleArrayPrimitiveIntegerMatrix(final int n, final int m,
			boolean init) {
		Random r = new Random();
		this.n = n;
		this.m = m;
		data = new int[n * m];
		if (init)
			for (int i = 0; i < n; i++)
				for (int j = 0; j < m; j++)
					data[(i * m) + j] = r.nextInt();
	}

	public static SingleArrayPrimitiveIntegerMatrix multiply(
			final SingleArrayPrimitiveIntegerMatrix x,
			final SingleArrayPrimitiveIntegerMatrix y) {
		SingleArrayPrimitiveIntegerMatrix r = new SingleArrayPrimitiveIntegerMatrix(
				x.n, y.m, false);
		int ri, xi;
		for (int i = 0; i < r.n; i++) {
			ri = i * r.m;
			xi = i * x.m;
			for (int j = 0; j < r.m; j++) {
				int a = x.data[xi] * y.data[j];
				for (int k = 1; k < x.m; k++)
					a += x.data[xi + k] * y.data[(k * y.m) + j];
				r.data[ri + j] = a;
			}
		}
		return r;
	}
}
