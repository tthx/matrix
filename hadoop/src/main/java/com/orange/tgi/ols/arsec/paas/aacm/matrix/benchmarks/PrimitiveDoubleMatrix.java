package com.orange.tgi.ols.arsec.paas.aacm.matrix.benchmarks;

import java.util.Random;

public class PrimitiveDoubleMatrix {
  private int n, m;
  private double data[][];

  public PrimitiveDoubleMatrix(final int n, final int m, boolean init) {
    Random r = new Random();
    this.n = n;
    this.m = m;
    data = new double[n][m];
    if (init)
      for (int i = 0; i < n; i++)
        for (int j = 0; j < m; j++)
          data[i][j] = r.nextDouble();
  }

  public static PrimitiveDoubleMatrix multiply(final PrimitiveDoubleMatrix x,
      final PrimitiveDoubleMatrix y) {
    PrimitiveDoubleMatrix r = new PrimitiveDoubleMatrix(x.n, y.m, false);
    for (int i = 0; i < r.n; i++)
      for (int j = 0; j < r.m; j++) {
        double a = x.data[i][0] * y.data[0][j];
        for (int k = 1; k < x.m; k++)
          a += x.data[i][k] * y.data[k][j];
        r.data[i][j] = a;
      }
    return r;
  }
}
