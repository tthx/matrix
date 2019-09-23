package com.orange.tgi.ols.arsec.paas.aacm.matrix.benchmarks;

import java.util.Random;

public class PrimitiveIntegerMatrix {
	private int n, m;
	private int data[][];

	public PrimitiveIntegerMatrix(final int n, final int m, boolean init) {
		Random r = new Random();
		this.n = n;
		this.m = m;
		data = new int[n][m];
		if (init)
			for (int i = 0; i < n; i++)
				for (int j = 0; j < m; j++)
					data[i][j] = r.nextInt();
	}

	public static PrimitiveIntegerMatrix multiply(
			final PrimitiveIntegerMatrix x, final PrimitiveIntegerMatrix y) {
		PrimitiveIntegerMatrix r = new PrimitiveIntegerMatrix(x.n, y.m, false);
		for (int i = 0; i < r.n; i++)
			for (int j = 0; j < r.m; j++) {
				int a = x.data[i][0] * y.data[0][j];
				for (int k = 1; k < x.m; k++)
					a += x.data[i][k] * y.data[k][j];
				r.data[i][j] = a;
			}
		return r;
	}
}
