package com.orange.tgi.ols.arsec.paas.aacm.matrix.benchmarks;

import java.util.Random;

import org.junit.Test;

import com.carrotsearch.junitbenchmarks.AbstractBenchmark;
import com.carrotsearch.junitbenchmarks.BenchmarkOptions;

@BenchmarkOptions(benchmarkRounds = 50, warmupRounds = 20)
public class DataTypeBenchmarks extends AbstractBenchmark {
	private final int N = 100, M = 200;

	@Test
	public void doubleBenchmarks() {
		DoubleMatrix x = new DoubleMatrix(N, M, true), y = new DoubleMatrix(M,
				N, true);
		DoubleMatrix.multiply(x, y);
	}

	@Test
	public void primitiveDoubleBenchmarks() {
		PrimitiveDoubleMatrix x = new PrimitiveDoubleMatrix(N, M, true), y = new PrimitiveDoubleMatrix(
				M, N, true);
		PrimitiveDoubleMatrix.multiply(x, y);
	}

	@Test
	public void singleArrayPrimitiveDoubleBenchmarks() {
		SingleArrayPrimitiveDoubleMatrix x = new SingleArrayPrimitiveDoubleMatrix(
				N, M, true), y = new SingleArrayPrimitiveDoubleMatrix(M, N,
				true);
		SingleArrayPrimitiveDoubleMatrix.multiply(x, y);
	}

	@Test
	public void doubleLambdaBenchmarks() {
		DoubleLambdaMatrix x = new DoubleLambdaMatrix(N, M, true), y = new DoubleLambdaMatrix(
				M, N, true);
		DoubleLambdaMatrix.multiply(x, y, (final Double a, final Double b) -> a
				+ b, (final Double a, final Double b) -> a * b);
	}

	@Test
	public void doubleGenericLambdaBenchmarks() {
		GenericLambdaMatrix<Double> x = new GenericLambdaMatrix<Double>(N, M,
				true, (final Integer n, final Integer m) -> new Double[n][m], (
						Random r) -> r.nextDouble()), y = new GenericLambdaMatrix<Double>(
				M, N, true,
				(final Integer n, final Integer m) -> new Double[n][m], (
						Random r) -> r.nextDouble());
		GenericLambdaMatrix.multiply(x, y,
				(final Integer n, final Integer m) -> new Double[n][m], (
						final Double a, final Double b) -> a + b, (
						final Double a, final Double b) -> a * b);
	}

	@Test
	public void integerBenchmarks() {
		IntegerMatrix x = new IntegerMatrix(N, M, true), y = new IntegerMatrix(
				M, N, true);
		IntegerMatrix.multiply(x, y);
	}

	@Test
	public void primitiveIntegerBenchmarks() {
		PrimitiveIntegerMatrix x = new PrimitiveIntegerMatrix(N, M, true), y = new PrimitiveIntegerMatrix(
				M, N, true);
		PrimitiveIntegerMatrix.multiply(x, y);
	}

	@Test
	public void singleArrayPrimitiveIntegerBenchmarks() {
		SingleArrayPrimitiveIntegerMatrix x = new SingleArrayPrimitiveIntegerMatrix(
				N, M, true), y = new SingleArrayPrimitiveIntegerMatrix(M, N,
				true);
		SingleArrayPrimitiveIntegerMatrix.multiply(x, y);
	}

	@Test
	public void integerLambdaBenchmarks() {
		IntegerLambdaMatrix x = new IntegerLambdaMatrix(N, M, true), y = new IntegerLambdaMatrix(
				M, N, true);
		IntegerLambdaMatrix.multiply(x, y,
				(final Integer a, final Integer b) -> a + b, (final Integer a,
						final Integer b) -> a * b);
	}

	@Test
	public void integerGenericLambdaBenchmarks() {
		GenericLambdaMatrix<Integer> x = new GenericLambdaMatrix<Integer>(N, M,
				true, (final Integer n, final Integer m) -> new Integer[n][m],
				(Random r) -> r.nextInt()), y = new GenericLambdaMatrix<Integer>(
				M, N, true,
				(final Integer n, final Integer m) -> new Integer[n][m], (
						Random r) -> r.nextInt());
		GenericLambdaMatrix.multiply(x, y,
				(final Integer n, final Integer m) -> new Integer[n][m], (
						final Integer a, final Integer b) -> a + b, (
						final Integer a, final Integer b) -> a * b);
	}
}
