package com.orange.tgi.ols.arsec.paas.aacm.matrix.benchmarks;

import java.util.Random;

public class DoubleMatrix {
  private int n, m;
  private Double data[][];

  public DoubleMatrix(final int n, final int m, boolean init) {
    Random r = new Random();
    this.n = n;
    this.m = m;
    data = new Double[n][m];
    if (init)
      for (int i = 0; i < n; i++)
        for (int j = 0; j < m; j++)
          data[i][j] = r.nextDouble();
  }

  public static DoubleMatrix multiply(final DoubleMatrix x,
      final DoubleMatrix y) {
    DoubleMatrix r = new DoubleMatrix(x.n, y.m, false);
    for (int i = 0; i < r.n; i++)
      for (int j = 0; j < r.m; j++) {
        Double a = x.data[i][0] * y.data[0][j];
        for (int k = 1; k < x.m; k++)
          a += x.data[i][k] * y.data[k][j];
        r.data[i][j] = a;
      }
    return r;
  }
}
