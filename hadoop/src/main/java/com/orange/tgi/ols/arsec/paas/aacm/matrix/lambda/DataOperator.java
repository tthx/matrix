package com.orange.tgi.ols.arsec.paas.aacm.matrix.lambda;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.function.BiFunction;

/**
 * Classe permmetant de definir des operations sur des nombres.
 *
 * @param <T> Le type du premier terme d'une operation
 * @param <U> Le type du second terme d'une operation
 * @param <R> Le type du resultat d'une operation
 */
public class DataOperator<T extends Number, U extends Number,
    R extends Number> {
  /** Operation d'addition au format {@code f(T x, U y) -> R r} */
  private BiFunction<T, U, R> sum = null;
  /** Operation d'addition au format {@code f(R x, R y) -> R r} */
  private BiFunction<R, R, R> sumR = null;
  /** Operation de multiplication au format {@code f(T x, U y) -> R r} */
  private BiFunction<T, U, R> multiply = null;

  /**
   * Recupere une operation d'addition au format {@code f(T x, U y) -> R r}
   * 
   * @return une operation d'addition au format {@code f(T x, U y) -> R r}
   */
  public BiFunction<T, U, R> getSum() {
    return sum;
  }

  /**
   * Positionne une operation d'addition au format {@code f(T x, U y) -> R r}
   * 
   * @param sum une operation d'addition au format {@code f(T x, U y) -> R r}
   */
  public void setSum(BiFunction<T, U, R> sum) {
    this.sum = sum;
  }

  /**
   * Recupere une operation d'addition au format {@code f(R x, R y) -> R r}
   * 
   * @return une operation d'addition au format {@code f(R x, R y) -> R r}
   */
  public BiFunction<R, R, R> getSumR() {
    return sumR;
  }

  /**
   * Positionne une operation d'addition au format {@code f(R x, R y) -> R r}
   * 
   * @param sumR une operation d'addition au format {@code f(R x, R y) -> R r}
   */
  public void setSumR(BiFunction<R, R, R> sumR) {
    this.sumR = sumR;
  }

  /**
   * Recupere une operation de multiplication au format
   * {@code f(T x, U y) -> R r}
   * 
   * @return une operation de multiplication au format
   *         {@code f(T x, U y) -> R r}
   */
  public BiFunction<T, U, R> getMultiply() {
    return multiply;
  }

  /**
   * Positionne une operation de multiplication au format
   * {@code f(T x, U y) -> R r}
   * 
   * @param multiply une operation de multiplication au format
   *                 {@code f(T x, U y) -> R r}
   */
  public void setMultiply(BiFunction<T, U, R> multiply) {
    this.multiply = multiply;
  }

  /** Instancie un operateur vide */
  public DataOperator() {
  }

  /**
   * Instancie un operateur a partir de fonctions
   * 
   * @param sum      une operation d'addition au format
   *                 {@code f(T x, U y) -> R r}
   * @param sumR     une operation d'addition au format
   *                 {@code f(T x, U y) -> R r}
   * @param multiply une operation de multiplication au format
   *                 {@code f(T x, U y) -> R r}
   */
  public DataOperator(final BiFunction<T, U, R> sum,
      final BiFunction<R, R, R> sumR, final BiFunction<T, U, R> multiply) {
    this.sum = sum;
    this.sumR = sumR;
    this.multiply = multiply;
  }

  /**
   * Cree une instance de {@link DataOperator} a partir des
   * {@link com.orange.imt.ist.isad.stag.matrix.lambda.Matrix.DataType} de deux
   * matrices.
   * 
   * @param  x une matrice
   * @param  y une matrice
   * @return   une instance de {@link DataOperator}
   */
  public static
      DataOperator<? extends Number, ? extends Number, ? extends Number>
      builder(final Matrix.DataType xDataType,
          final Matrix.DataType yDataType) {
    DataOperator<? extends Number, ? extends Number, ? extends Number> r = null;
    switch (xDataType) {
      case Byte:
        switch (yDataType) {
          case Byte:
            r = new DataOperator<Byte, Byte, Integer>(
                (final Byte a, final Byte b) -> a.intValue() + b.intValue(),
                (final Integer a, final Integer b) -> a + b,
                (final Byte a, final Byte b) -> a.intValue() * b.intValue());
            break;
          case Short:
            r = new DataOperator<Byte, Short, Integer>(
                (final Byte a, final Short b) -> a.intValue() + b.intValue(),
                (final Integer a, final Integer b) -> a + b,
                (final Byte a, final Short b) -> a.intValue() * b.intValue());
            break;
          case Integer:
            r = new DataOperator<Byte, Integer, Integer>(
                (final Byte a, final Integer b) -> a.intValue() + b,
                (final Integer a, final Integer b) -> a + b,
                (final Byte a, final Integer b) -> a.intValue() * b);
            break;
          case Long:
            r = new DataOperator<Byte, Long, Long>(
                (final Byte a, final Long b) -> a.longValue() + b,
                (final Long a, final Long b) -> a + b,
                (final Byte a, final Long b) -> a.longValue() * b);
            break;
          case Float:
            r = new DataOperator<Byte, Float, Float>(
                (final Byte a, final Float b) -> a.floatValue() + b,
                (final Float a, final Float b) -> a + b,
                (final Byte a, final Float b) -> a.floatValue() * b);
            break;
          case Double:
            r = new DataOperator<Byte, Double, Double>(
                (final Byte a, final Double b) -> a.doubleValue() + b,
                (final Double a, final Double b) -> a + b,
                (final Byte a, final Double b) -> a.doubleValue() * b);
            break;
          case BigInteger:
            r = new DataOperator<Byte, BigInteger, BigInteger>(
                (final Byte a, final BigInteger b) -> b
                    .add(new BigInteger(a.toString())),
                (final BigInteger a, final BigInteger b) -> a.add(b),
                (final Byte a, final BigInteger b) -> b
                    .multiply(new BigInteger(a.toString())));
            break;
          case BigDecimal:
            r = new DataOperator<Byte, BigDecimal, BigDecimal>(
                (final Byte a, final BigDecimal b) -> b
                    .add(new BigDecimal(a.intValue())),
                (final BigDecimal a, final BigDecimal b) -> a.add(b),
                (final Byte a, final BigDecimal b) -> b
                    .multiply(new BigDecimal(a.intValue())));
            break;
        }
        break;
      case Short:
        switch (yDataType) {
          case Byte:
            r = new DataOperator<Short, Byte, Integer>(
                (final Short a, final Byte b) -> a.intValue() + b.intValue(),
                (final Integer a, final Integer b) -> a + b,
                (final Short a, final Byte b) -> a.intValue() * b.intValue());
            break;
          case Short:
            r = new DataOperator<Short, Short, Integer>(
                (final Short a, final Short b) -> a.intValue() + b.intValue(),
                (final Integer a, final Integer b) -> a + b,
                (final Short a, final Short b) -> a.intValue() * b.intValue());
            break;
          case Integer:
            r = new DataOperator<Short, Integer, Integer>(
                (final Short a, final Integer b) -> a.intValue() + b,
                (final Integer a, final Integer b) -> a + b,
                (final Short a, final Integer b) -> a.intValue() * b);
            break;
          case Long:
            r = new DataOperator<Short, Long, Long>(
                (final Short a, final Long b) -> a.longValue() + b,
                (final Long a, final Long b) -> a + b,
                (final Short a, final Long b) -> a.longValue() * b);
            break;
          case Float:
            r = new DataOperator<Short, Float, Float>(
                (final Short a, final Float b) -> a.floatValue() + b,
                (final Float a, final Float b) -> a + b,
                (final Short a, final Float b) -> a.floatValue() * b);
            break;
          case Double:
            r = new DataOperator<Short, Double, Double>(
                (final Short a, final Double b) -> a.doubleValue() + b,
                (final Double a, final Double b) -> a + b,
                (final Short a, final Double b) -> a.doubleValue() * b);
            break;
          case BigInteger:
            r = new DataOperator<Short, BigInteger, BigInteger>(
                (final Short a, final BigInteger b) -> b
                    .add(new BigInteger(a.toString())),
                (final BigInteger a, final BigInteger b) -> a.add(b),
                (final Short a, final BigInteger b) -> b
                    .multiply(new BigInteger(a.toString())));
            break;
          case BigDecimal:
            r = new DataOperator<Short, BigDecimal, BigDecimal>(
                (final Short a, final BigDecimal b) -> b
                    .add(new BigDecimal(a.intValue())),
                (final BigDecimal a, final BigDecimal b) -> a.add(b),
                (final Short a, final BigDecimal b) -> b
                    .multiply(new BigDecimal(a.intValue())));
            break;
        }
        break;
      case Integer:
        switch (yDataType) {
          case Byte:
            r = new DataOperator<Integer, Byte, Integer>(
                (final Integer a, final Byte b) -> a + b.intValue(),
                (final Integer a, final Integer b) -> a + b,
                (final Integer a, final Byte b) -> a * b.intValue());
            break;
          case Short:
            r = new DataOperator<Integer, Short, Integer>(
                (final Integer a, final Short b) -> a + b.intValue(),
                (final Integer a, final Integer b) -> a + b,
                (final Integer a, final Short b) -> a * b.intValue());
            break;
          case Integer:
            r = new DataOperator<Integer, Integer, Integer>(
                (final Integer a, final Integer b) -> a + b,
                (final Integer a, final Integer b) -> a + b,
                (final Integer a, final Integer b) -> a * b);
            break;
          case Long:
            r = new DataOperator<Integer, Long, Long>(
                (final Integer a, final Long b) -> a.longValue() + b,
                (final Long a, final Long b) -> a + b,
                (final Integer a, final Long b) -> a.longValue() * b);
            break;
          case Float:
            r = new DataOperator<Integer, Float, Float>(
                (final Integer a, final Float b) -> a.floatValue() + b,
                (final Float a, final Float b) -> a + b,
                (final Integer a, final Float b) -> a.floatValue() * b);
            break;
          case Double:
            r = new DataOperator<Integer, Double, Double>(
                (final Integer a, final Double b) -> a.doubleValue() + b,
                (final Double a, final Double b) -> a + b,
                (final Integer a, final Double b) -> a.doubleValue() * b);
            break;
          case BigInteger:
            r = new DataOperator<Integer, BigInteger, BigInteger>(
                (final Integer a, final BigInteger b) -> b
                    .add(new BigInteger(a.toString())),
                (final BigInteger a, final BigInteger b) -> a.add(b),
                (final Integer a, final BigInteger b) -> b
                    .multiply(new BigInteger(a.toString())));
            break;
          case BigDecimal:
            r = new DataOperator<Integer, BigDecimal, BigDecimal>(
                (final Integer a, final BigDecimal b) -> b
                    .add(new BigDecimal(a)),
                (final BigDecimal a, final BigDecimal b) -> a.add(b),
                (final Integer a, final BigDecimal b) -> b
                    .multiply(new BigDecimal(a)));
            break;
        }
        break;
      case Long:
        switch (yDataType) {
          case Byte:
            r = new DataOperator<Long, Byte, Long>(
                (final Long a, final Byte b) -> a + b.longValue(),
                (final Long a, final Long b) -> a + b,
                (final Long a, final Byte b) -> a * b.longValue());
            break;
          case Short:
            r = new DataOperator<Long, Short, Long>(
                (final Long a, final Short b) -> a + b.longValue(),
                (final Long a, final Long b) -> a + b,
                (final Long a, final Short b) -> a * b.longValue());
            break;
          case Integer:
            r = new DataOperator<Long, Integer, Long>(
                (final Long a, final Integer b) -> a + b.longValue(),
                (final Long a, final Long b) -> a + b,
                (final Long a, final Integer b) -> a * b.longValue());
            break;
          case Long:
            r = new DataOperator<Long, Long, Long>(
                (final Long a, final Long b) -> a + b,
                (final Long a, final Long b) -> a + b,
                (final Long a, final Long b) -> a * b);
            break;
          case Float:
            r = new DataOperator<Long, Float, Float>(
                (final Long a, final Float b) -> a.floatValue() + b,
                (final Float a, final Float b) -> a + b,
                (final Long a, final Float b) -> a.floatValue() * b);
            break;
          case Double:
            r = new DataOperator<Long, Double, Double>(
                (final Long a, final Double b) -> a.doubleValue() + b,
                (final Double a, final Double b) -> a + b,
                (final Long a, final Double b) -> a.doubleValue() * b);
            break;
          case BigInteger:
            r = new DataOperator<Long, BigInteger, BigInteger>(
                (final Long a, final BigInteger b) -> b
                    .add(new BigInteger(a.toString())),
                (final BigInteger a, final BigInteger b) -> a.add(b),
                (final Long a, final BigInteger b) -> b
                    .multiply(new BigInteger(a.toString())));
            break;
          case BigDecimal:
            r = new DataOperator<Long, BigDecimal, BigDecimal>(
                (final Long a, final BigDecimal b) -> b.add(new BigDecimal(a)),
                (final BigDecimal a, final BigDecimal b) -> a.add(b),
                (final Long a, final BigDecimal b) -> b
                    .multiply(new BigDecimal(a)));
            break;
        }
        break;
      case Float:
        switch (yDataType) {
          case Byte:
            r = new DataOperator<Float, Byte, Float>(
                (final Float a, final Byte b) -> a + b.floatValue(),
                (final Float a, final Float b) -> a + b,
                (final Float a, final Byte b) -> a * b.floatValue());
            break;
          case Short:
            r = new DataOperator<Float, Short, Float>(
                (final Float a, final Short b) -> a + b.floatValue(),
                (final Float a, final Float b) -> a + b,
                (final Float a, final Short b) -> a * b.floatValue());
            break;
          case Integer:
            r = new DataOperator<Float, Integer, Float>(
                (final Float a, final Integer b) -> a + b.floatValue(),
                (final Float a, final Float b) -> a + b,
                (final Float a, final Integer b) -> a * b.floatValue());
            break;
          case Long:
            r = new DataOperator<Float, Long, Float>(
                (final Float a, final Long b) -> a + b.floatValue(),
                (final Float a, final Float b) -> a + b,
                (final Float a, final Long b) -> a * b.floatValue());
            break;
          case Float:
            r = new DataOperator<Float, Float, Float>(
                (final Float a, final Float b) -> a + b,
                (final Float a, final Float b) -> a + b,
                (final Float a, final Float b) -> a * b);
            break;
          case Double:
            r = new DataOperator<Float, Double, Double>(
                (final Float a, final Double b) -> a.doubleValue() + b,
                (final Double a, final Double b) -> a + b,
                (final Float a, final Double b) -> a.doubleValue() * b);
            break;
          case BigInteger:
            r = new DataOperator<Float, BigInteger, BigDecimal>(
                (final Float a, final BigInteger b) -> new BigDecimal(a)
                    .add(new BigDecimal(b)),
                (final BigDecimal a, final BigDecimal b) -> a.add(b),
                (final Float a, final BigInteger b) -> new BigDecimal(a)
                    .multiply(new BigDecimal(b)));
            break;
          case BigDecimal:
            r = new DataOperator<Float, BigDecimal, BigDecimal>(
                (final Float a, final BigDecimal b) -> b.add(new BigDecimal(a)),
                (final BigDecimal a, final BigDecimal b) -> a.add(b),
                (final Float a, final BigDecimal b) -> b
                    .multiply(new BigDecimal(a)));
            break;
        }
        break;
      case Double:
        switch (yDataType) {
          case Byte:
            r = new DataOperator<Double, Byte, Double>(
                (final Double a, final Byte b) -> a + b.doubleValue(),
                (final Double a, final Double b) -> a + b,
                (final Double a, final Byte b) -> a * b.doubleValue());
            break;
          case Short:
            r = new DataOperator<Double, Short, Double>(
                (final Double a, final Short b) -> a + b.doubleValue(),
                (final Double a, final Double b) -> a + b,
                (final Double a, final Short b) -> a * b.doubleValue());
            break;
          case Integer:
            r = new DataOperator<Double, Integer, Double>(
                (final Double a, final Integer b) -> a + b.doubleValue(),
                (final Double a, final Double b) -> a + b,
                (final Double a, final Integer b) -> a * b.doubleValue());
            break;
          case Long:
            r = new DataOperator<Double, Long, Double>(
                (final Double a, final Long b) -> a + b.doubleValue(),
                (final Double a, final Double b) -> a + b,
                (final Double a, final Long b) -> a * b.doubleValue());
            break;
          case Float:
            r = new DataOperator<Double, Float, Double>(
                (final Double a, final Float b) -> a + b.doubleValue(),
                (final Double a, final Double b) -> a + b,
                (final Double a, final Float b) -> a * b.doubleValue());
            break;
          case Double:
            r = new DataOperator<Double, Double, Double>(
                (final Double a, final Double b) -> a + b,
                (final Double a, final Double b) -> a + b,
                (final Double a, final Double b) -> a * b);
            break;
          case BigInteger:
            r = new DataOperator<Double, BigInteger, BigDecimal>(
                (final Double a, final BigInteger b) -> new BigDecimal(a)
                    .add(new BigDecimal(b)),
                (final BigDecimal a, final BigDecimal b) -> a.add(b),
                (final Double a, final BigInteger b) -> new BigDecimal(a)
                    .multiply(new BigDecimal(b)));
            break;
          case BigDecimal:
            r = new DataOperator<Double, BigDecimal, BigDecimal>(
                (final Double a, final BigDecimal b) -> b
                    .add(new BigDecimal(a)),
                (final BigDecimal a, final BigDecimal b) -> a.add(b),
                (final Double a, final BigDecimal b) -> b
                    .multiply(new BigDecimal(a)));
            break;
        }
        break;
      case BigInteger:
        switch (yDataType) {
          case Byte:
            r = new DataOperator<BigInteger, Byte, BigInteger>(
                (final BigInteger a, final Byte b) -> a
                    .add(new BigInteger(b.toString())),
                (final BigInteger a, final BigInteger b) -> a.add(b),
                (final BigInteger a, final Byte b) -> a
                    .add(new BigInteger(b.toString())));
            break;
          case Short:
            r = new DataOperator<BigInteger, Short, BigInteger>(
                (final BigInteger a, final Short b) -> a
                    .add(new BigInteger(b.toString())),
                (final BigInteger a, final BigInteger b) -> a.add(b),
                (final BigInteger a, final Short b) -> a
                    .add(new BigInteger(b.toString())));
            break;
          case Integer:
            r = new DataOperator<BigInteger, Integer, BigInteger>(
                (final BigInteger a, final Integer b) -> a
                    .add(new BigInteger(b.toString())),
                (final BigInteger a, final BigInteger b) -> a.add(b),
                (final BigInteger a, final Integer b) -> a
                    .multiply(new BigInteger(b.toString())));
            break;
          case Long:
            r = new DataOperator<BigInteger, Long, BigInteger>(
                (final BigInteger a, final Long b) -> a
                    .add(new BigInteger(b.toString())),
                (final BigInteger a, final BigInteger b) -> a.add(b),
                (final BigInteger a, final Long b) -> a
                    .multiply(new BigInteger(b.toString())));
            break;
          case Float:
            r = new DataOperator<BigInteger, Float, BigDecimal>(
                (final BigInteger a, final Float b) -> new BigDecimal(a)
                    .add(new BigDecimal(b)),
                (final BigDecimal a, final BigDecimal b) -> a.add(b),
                (final BigInteger a, final Float b) -> new BigDecimal(a)
                    .multiply(new BigDecimal(b)));
            break;
          case Double:
            r = new DataOperator<BigInteger, Double, BigDecimal>(
                (final BigInteger a, final Double b) -> new BigDecimal(a)
                    .add(new BigDecimal(b)),
                (final BigDecimal a, final BigDecimal b) -> a.add(b),
                (final BigInteger a, final Double b) -> new BigDecimal(a)
                    .multiply(new BigDecimal(b)));
            break;
          case BigInteger:
            r = new DataOperator<BigInteger, BigInteger, BigInteger>(
                (final BigInteger a, final BigInteger b) -> a.add(b),
                (final BigInteger a, final BigInteger b) -> a.add(b),
                (final BigInteger a, final BigInteger b) -> a.multiply(b));
            break;
          case BigDecimal:
            r = new DataOperator<BigInteger, BigDecimal, BigDecimal>(
                (final BigInteger a, final BigDecimal b) -> b
                    .add(new BigDecimal(a)),
                (final BigDecimal a, final BigDecimal b) -> a.add(b),
                (final BigInteger a, final BigDecimal b) -> b
                    .multiply(new BigDecimal(a)));
            break;
        }
        break;
      case BigDecimal:
        switch (yDataType) {
          case Byte:
            r = new DataOperator<BigDecimal, Byte, BigDecimal>(
                (final BigDecimal a, final Byte b) -> a.add(new BigDecimal(b)),
                (final BigDecimal a, final BigDecimal b) -> a.add(b),
                (final BigDecimal a, final Byte b) -> a
                    .multiply(new BigDecimal(b)));
            break;
          case Short:
            r = new DataOperator<BigDecimal, Short, BigDecimal>(
                (final BigDecimal a, final Short b) -> a.add(new BigDecimal(b)),
                (final BigDecimal a, final BigDecimal b) -> a.add(b),
                (final BigDecimal a, final Short b) -> a
                    .multiply(new BigDecimal(b)));
            break;
          case Integer:
            r = new DataOperator<BigDecimal, Integer, BigDecimal>(
                (final BigDecimal a, final Integer b) -> a
                    .add(new BigDecimal(b)),
                (final BigDecimal a, final BigDecimal b) -> a.add(b),
                (final BigDecimal a, final Integer b) -> a
                    .multiply(new BigDecimal(b)));
            break;
          case Long:
            r = new DataOperator<BigDecimal, Long, BigDecimal>(
                (final BigDecimal a, final Long b) -> a.add(new BigDecimal(b)),
                (final BigDecimal a, final BigDecimal b) -> a.add(b),
                (final BigDecimal a, final Long b) -> a
                    .multiply(new BigDecimal(b)));
            break;
          case Float:
            r = new DataOperator<BigDecimal, Float, BigDecimal>(
                (final BigDecimal a, final Float b) -> a.add(new BigDecimal(b)),
                (final BigDecimal a, final BigDecimal b) -> a.add(b),
                (final BigDecimal a, final Float b) -> a
                    .multiply(new BigDecimal(b)));
            break;
          case Double:
            r = new DataOperator<BigDecimal, Double, BigDecimal>(
                (final BigDecimal a, final Double b) -> a
                    .add(new BigDecimal(b)),
                (final BigDecimal a, final BigDecimal b) -> a.add(b),
                (final BigDecimal a, final Double b) -> a
                    .multiply(new BigDecimal(b)));
            break;
          case BigInteger:
            r = new DataOperator<BigDecimal, BigInteger, BigDecimal>(
                (final BigDecimal a, final BigInteger b) -> a
                    .add(new BigDecimal(b)),
                (final BigDecimal a, final BigDecimal b) -> a.add(b),
                (final BigDecimal a, final BigInteger b) -> a
                    .multiply(new BigDecimal(b)));
            break;
          case BigDecimal:
            r = new DataOperator<BigDecimal, BigDecimal, BigDecimal>(
                (final BigDecimal a, final BigDecimal b) -> a.add(b),
                (final BigDecimal a, final BigDecimal b) -> a.add(b),
                (final BigDecimal a, final BigDecimal b) -> a.multiply(b));
            break;
        }
        break;
    }
    return r;
  }

}
