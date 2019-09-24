package com.orange.tgi.ols.arsec.paas.aacm.matrix.lambda;

import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.math.BigDecimal;
import java.math.BigInteger;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.HadoopTestCase;
import org.junit.After;
import org.junit.Test;

import com.orange.tgi.ols.arsec.paas.aacm.matrix.exception.MatrixBoundAdditionException;
import com.orange.tgi.ols.arsec.paas.aacm.matrix.exception.MatrixBoundCopyException;
import com.orange.tgi.ols.arsec.paas.aacm.matrix.exception.MatrixBoundMultiplicationException;
import com.orange.tgi.ols.arsec.paas.aacm.matrix.exception.MatrixBoundReadException;
import com.orange.tgi.ols.arsec.paas.aacm.matrix.exception.MatrixBoundWriteException;
import com.orange.tgi.ols.arsec.paas.aacm.matrix.lambda.hadoop.MatrixParameter;

public class MatrixTest extends HadoopTestCase {

  private final Path testDir= new Path("/tmp/matrix");
  
  public MatrixTest() throws IOException {
    super(LOCAL_MR, LOCAL_FS, 1, 1);
  }

  @After
  public void tearDown() throws Exception {
    getFileSystem().delete(testDir, true);
    super.tearDown();
  }

  @Test
  public void multiplyByte() throws NumberFormatException, IOException,
      MatrixBoundMultiplicationException {
    Matrix<Byte> x = (Matrix<Byte>) Matrix.builder(5, 10, Matrix.DataType.Byte,
        true, null, true, null, null);
    Matrix<Byte> y = (Matrix<Byte>) Matrix.builder(10, 3, Matrix.DataType.Byte,
        true, null, true, null, null);
    Matrix<Integer> z;
    DataOperator<Byte, Byte, Integer> dataOperator = (DataOperator<Byte, Byte,
        Integer>) DataOperator.builder(x.getDataType(), y.getDataType());
    z = Matrix.multiply(x, y, dataOperator.getSumR(),
        dataOperator.getMultiply());
    System.out.printf("x:[\n%s\n]\n", x.toString());
    System.out.printf("y:[\n%s\n]\n", y.toString());
    System.out.printf("z:[\n%s\n]\n", z.toString());
  }

  @Test
  public void multiplyShort() throws NumberFormatException, IOException,
      MatrixBoundMultiplicationException {
    Matrix<Short> x = (Matrix<Short>) Matrix.builder(5, 10,
        Matrix.DataType.Short, true, null, true, null, null);
    Matrix<Short> y = (Matrix<Short>) Matrix.builder(10, 3,
        Matrix.DataType.Short, true, null, true, null, null);
    Matrix<Integer> z;
    DataOperator<Short, Short, Integer> dataOperator = (DataOperator<Short,
        Short, Integer>) DataOperator.builder(x.getDataType(), y.getDataType());
    z = Matrix.multiply(x, y, dataOperator.getSumR(),
        dataOperator.getMultiply());
    System.out.printf("x:[\n%s\n]\n", x.toString());
    System.out.printf("y:[\n%s\n]\n", y.toString());
    System.out.printf("z:[\n%s\n]\n", z.toString());
  }

  @Test
  public void multiplyByteShort() throws NumberFormatException, IOException,
      MatrixBoundMultiplicationException {
    Matrix<Byte> x = (Matrix<Byte>) Matrix.builder(5, 10, Matrix.DataType.Byte,
        true, null, true, null, null);
    Matrix<Short> y = (Matrix<Short>) Matrix.builder(10, 3,
        Matrix.DataType.Short, true, null, true, null, null);
    Matrix<Integer> z;
    DataOperator<Byte, Short, Integer> dataOperator = (DataOperator<Byte, Short,
        Integer>) DataOperator.builder(x.getDataType(), y.getDataType());
    z = Matrix.multiply(x, y, dataOperator.getSumR(),
        dataOperator.getMultiply());
    System.out.printf("x:[\n%s\n]\n", x.toString());
    System.out.printf("y:[\n%s\n]\n", y.toString());
    System.out.printf("z:[\n%s\n]\n", z.toString());
  }

  @Test
  public void multiplyInteger() throws NumberFormatException, IOException,
      MatrixBoundMultiplicationException {
    Matrix<Integer> x = (Matrix<Integer>) Matrix.builder(5, 10,
        Matrix.DataType.Integer, true, null, true, null, null);
    Matrix<Integer> y = (Matrix<Integer>) Matrix.builder(10, 3,
        Matrix.DataType.Integer, true, null, true, null, null);
    Matrix<Integer> z;
    DataOperator<Integer, Integer, Integer> dataOperator =
        (DataOperator<Integer, Integer, Integer>) DataOperator
            .builder(x.getDataType(), y.getDataType());
    z = Matrix.multiply(x, y, dataOperator.getSumR(),
        dataOperator.getMultiply());
    System.out.printf("x:[\n%s\n]\n", x.toString());
    System.out.printf("y:[\n%s\n]\n", y.toString());
    System.out.printf("z:[\n%s\n]\n", z.toString());
  }

  @Test
  public void multiplyIntegerDouble() throws NumberFormatException, IOException,
      MatrixBoundMultiplicationException {
    Matrix<Integer> x = (Matrix<Integer>) Matrix.builder(5, 10,
        Matrix.DataType.Integer, true, null, true, null, null);
    Matrix<Double> y = (Matrix<Double>) Matrix.builder(10, 3,
        Matrix.DataType.Double, true, null, true, null, null);
    Matrix<Double> z;
    DataOperator<Integer, Double, Double> dataOperator = (DataOperator<Integer,
        Double, Double>) DataOperator.builder(x.getDataType(), y.getDataType());
    z = Matrix.multiply(x, y, dataOperator.getSumR(),
        dataOperator.getMultiply());
    System.out.printf("x:[\n%s\n]\n", x.toString());
    System.out.printf("y:[\n%s\n]\n", y.toString());
    System.out.printf("z:[\n%s\n]\n", z.toString());
  }

  @Test
  public void multiplyFloatDouble() throws NumberFormatException, IOException,
      MatrixBoundMultiplicationException {
    Matrix<Float> x = (Matrix<Float>) Matrix.builder(5, 10,
        Matrix.DataType.Float, true, "1", false, null, null);
    Matrix<Double> y = (Matrix<Double>) Matrix.builder(10, 3,
        Matrix.DataType.Double, true, "1", false, null, null);
    Matrix<Double> z;
    DataOperator<Float, Double, Double> dataOperator = (DataOperator<Float,
        Double, Double>) DataOperator.builder(x.getDataType(), y.getDataType());
    z = Matrix.multiply(x, y, dataOperator.getSumR(),
        dataOperator.getMultiply());
    System.out.printf("x:[\n%s\n]\n", x.toString());
    System.out.printf("y:[\n%s\n]\n", y.toString());
    System.out.printf("z:[\n%s\n]\n", z.toString());
  }

  @Test
  public void multiplyBigInteger() throws NumberFormatException, IOException,
      MatrixBoundMultiplicationException {
    Matrix<BigInteger> x = (Matrix<BigInteger>) Matrix.builder(5, 10,
        Matrix.DataType.BigInteger, true, null, true, null, null);
    Matrix<BigInteger> y = (Matrix<BigInteger>) Matrix.builder(10, 3,
        Matrix.DataType.BigInteger, true, null, true, null, null);
    Matrix<BigInteger> z;
    DataOperator<BigInteger, BigInteger, BigInteger> dataOperator =
        (DataOperator<BigInteger, BigInteger, BigInteger>) DataOperator
            .builder(x.getDataType(), y.getDataType());
    z = Matrix.multiply(x, y, dataOperator.getSumR(),
        dataOperator.getMultiply());
    System.out.printf("x:[\n%s\n]\n", x.toString());
    System.out.printf("y:[\n%s\n]\n", y.toString());
    System.out.printf("z:[\n%s\n]\n", z.toString());
  }

  @Test
  public void multiplyBigDecimal() throws NumberFormatException, IOException,
      MatrixBoundMultiplicationException {
    Matrix<BigDecimal> x = (Matrix<BigDecimal>) Matrix.builder(5, 10,
        Matrix.DataType.BigDecimal, true, null, true, null, null);
    Matrix<BigDecimal> y = (Matrix<BigDecimal>) Matrix.builder(10, 3,
        Matrix.DataType.BigDecimal, true, null, true, null, null);
    Matrix<BigDecimal> z;
    DataOperator<BigDecimal, BigDecimal, BigDecimal> dataOperator =
        (DataOperator<BigDecimal, BigDecimal, BigDecimal>) DataOperator
            .builder(x.getDataType(), y.getDataType());
    z = Matrix.multiply(x, y, dataOperator.getSumR(),
        dataOperator.getMultiply());
    System.out.printf("x:[\n%s\n]\n", x.toString());
    System.out.printf("y:[\n%s\n]\n", y.toString());
    System.out.printf("z:[\n%s\n]\n", z.toString());
  }

  @Test
  public void multiplyBigDecimalBigInteger() throws NumberFormatException,
      IOException, MatrixBoundMultiplicationException {
    Matrix<BigDecimal> x = (Matrix<BigDecimal>) Matrix.builder(5, 10,
        Matrix.DataType.BigDecimal, true, "1", true, null, null);
    Matrix<BigInteger> y = (Matrix<BigInteger>) Matrix.builder(10, 3,
        Matrix.DataType.BigInteger, true, "1", true, null, null);
    Matrix<BigDecimal> z;
    DataOperator<BigDecimal, BigInteger, BigDecimal> dataOperator =
        (DataOperator<BigDecimal, BigInteger, BigDecimal>) DataOperator
            .builder(x.getDataType(), y.getDataType());
    z = Matrix.multiply(x, y, dataOperator.getSumR(),
        dataOperator.getMultiply());
    System.out.printf("x:[\n%s\n]\n", x.toString());
    System.out.printf("y:[\n%s\n]\n", y.toString());
    System.out.printf("z:[\n%s\n]\n", z.toString());
  }

  @Test
  public void multiplyBigIntegerBigDecimal() throws NumberFormatException,
      IOException, MatrixBoundMultiplicationException {
    Matrix<BigInteger> x = (Matrix<BigInteger>) Matrix.builder(5, 10,
        Matrix.DataType.BigInteger, true, "1", true, null, null);
    Matrix<BigDecimal> y = (Matrix<BigDecimal>) Matrix.builder(10, 3,
        Matrix.DataType.BigDecimal, true, "1", true, null, null);
    Matrix<BigDecimal> z;
    DataOperator<BigInteger, BigDecimal, BigDecimal> dataOperator =
        (DataOperator<BigInteger, BigDecimal, BigDecimal>) DataOperator
            .builder(x.getDataType(), y.getDataType());
    z = Matrix.multiply(x, y, dataOperator.getSumR(),
        dataOperator.getMultiply());
    System.out.printf("x:[\n%s\n]\n", x.toString());
    System.out.printf("y:[\n%s\n]\n", y.toString());
    System.out.printf("z:[\n%s\n]\n", z.toString());
  }

  @Test
  public void multiplyBigIntegerDouble() throws NumberFormatException,
      IOException, MatrixBoundMultiplicationException {
    Matrix<BigInteger> x = (Matrix<BigInteger>) Matrix.builder(5, 10,
        Matrix.DataType.BigInteger, true, "1", true, null, null);
    Matrix<Double> y = (Matrix<Double>) Matrix.builder(10, 3,
        Matrix.DataType.Double, true, "1", true, null, null);
    Matrix<BigDecimal> z;
    DataOperator<BigInteger, Double, BigDecimal> dataOperator =
        (DataOperator<BigInteger, Double, BigDecimal>) DataOperator
            .builder(x.getDataType(), y.getDataType());
    z = Matrix.multiply(x, y, dataOperator.getSumR(),
        dataOperator.getMultiply());
    System.out.printf("x:[\n%s\n]\n", x.toString());
    System.out.printf("y:[\n%s\n]\n", y.toString());
    System.out.printf("z:[\n%s\n]\n", z.toString());
  }

  @Test
  public void multiplyBigIntegerConcurrent() throws NumberFormatException,
      IOException, MatrixBoundMultiplicationException {
    Matrix<BigInteger> x = (Matrix<BigInteger>) Matrix.builder(5, 10,
        Matrix.DataType.BigInteger, true, null, true, null, null);
    Matrix<BigInteger> y = (Matrix<BigInteger>) Matrix.builder(10, 3,
        Matrix.DataType.BigInteger, true, null, true, null, null);
    Matrix<BigInteger> z1, z2;
    DataOperator<BigInteger, BigInteger, BigInteger> dataOperator =
        (DataOperator<BigInteger, BigInteger, BigInteger>) DataOperator
            .builder(x.getDataType(), y.getDataType());
    z1 = Matrix.multiply(x, y, 2, 2, dataOperator.getSumR(),
        dataOperator.getMultiply());
    z2 = Matrix.multiply(x, y, dataOperator.getSumR(),
        dataOperator.getMultiply());
    System.out.printf("x:[\n%s\n]\n", x.toString());
    System.out.printf("y:[\n%s\n]\n", y.toString());
    System.out.printf("z1:[\n%s\n]\n", z1.toString());
    System.out.printf("z2:[\n%s\n]\n", z2.toString());
    assertTrue(z1.equals(z2));
  }

  @Test
  public void multiplyBigDecimalConcurrent() throws NumberFormatException,
      IOException, MatrixBoundMultiplicationException {
    Matrix<BigDecimal> x = (Matrix<BigDecimal>) Matrix.builder(5, 10,
        Matrix.DataType.BigDecimal, true, null, true, null, null);
    Matrix<BigDecimal> y = (Matrix<BigDecimal>) Matrix.builder(10, 3,
        Matrix.DataType.BigDecimal, true, null, true, null, null);
    Matrix<BigDecimal> z1, z2;
    DataOperator<BigDecimal, BigDecimal, BigDecimal> dataOperator =
        (DataOperator<BigDecimal, BigDecimal, BigDecimal>) DataOperator
            .builder(x.getDataType(), y.getDataType());
    z1 = Matrix.multiply(x, y, 2, 2, dataOperator.getSumR(),
        dataOperator.getMultiply());
    z2 = Matrix.multiply(x, y, dataOperator.getSumR(),
        dataOperator.getMultiply());
    System.out.printf("x:[\n%s\n]\n", x.toString());
    System.out.printf("y:[\n%s\n]\n", y.toString());
    System.out.printf("z1:[\n%s\n]\n", z1.toString());
    System.out.printf("z2:[\n%s\n]\n", z2.toString());
    assertTrue(z1.equals(z2));
  }

  @Test
  public void sumBigInteger()
      throws NumberFormatException, IOException, MatrixBoundAdditionException {
    Matrix<BigInteger> x = (Matrix<BigInteger>) Matrix.builder(5, 10,
        Matrix.DataType.BigInteger, true, "1", true, null, null);
    Matrix<BigInteger> y = (Matrix<BigInteger>) Matrix.builder(5, 10,
        Matrix.DataType.BigInteger, true, "1", true, null, null);
    Matrix<BigInteger> w = (Matrix<BigInteger>) Matrix.builder(10, 20,
        Matrix.DataType.BigInteger, true, "1", true, null, null);
    Matrix<BigInteger> z;
    DataOperator<BigInteger, BigInteger, BigInteger> dataOperator =
        (DataOperator<BigInteger, BigInteger, BigInteger>) DataOperator
            .builder(x.getDataType(), y.getDataType());
    z = Matrix.sum(x, y, dataOperator.getSum(), false);
    System.out.printf("x:[\n%s\n]\n", x.toString());
    System.out.printf("y:[\n%s\n]\n", y.toString());
    System.out.printf("z:[\n%s\n]\n", z.toString());
    z = Matrix.sum(w, y, dataOperator.getSum(), true);
    System.out.printf("w:[\n%s\n]\n", w.toString());
    System.out.printf("z:[\n%s\n]\n", z.toString());
  }

  @Test
  public void addBigInteger()
      throws NumberFormatException, IOException, MatrixBoundAdditionException {
    Matrix<BigInteger> x = (Matrix<BigInteger>) Matrix.builder(5, 10,
        Matrix.DataType.BigInteger, true, "1", true, null, null);
    Matrix<BigInteger> y = (Matrix<BigInteger>) Matrix.builder(5, 10,
        Matrix.DataType.BigInteger, true, "1", true, null, null);
    Matrix<BigInteger> w = (Matrix<BigInteger>) Matrix.builder(10, 20,
        Matrix.DataType.BigInteger, true, "1", true, null, null);
    DataOperator<BigInteger, BigInteger, BigInteger> dataOperator =
        (DataOperator<BigInteger, BigInteger, BigInteger>) DataOperator
            .builder(x.getDataType(), y.getDataType());
    System.out.printf("x:[\n%s\n]\n", x.toString());
    System.out.printf("y:[\n%s\n]\n", y.toString());
    Matrix.add(x, y, dataOperator.getSum(), false);
    System.out.printf("(x+y):[\n%s\n]\n", x.toString());
    System.out.printf("w:[\n%s\n]\n", w.toString());
    Matrix.add(w, y, dataOperator.getSum(), true);
    System.out.printf("(w+y):[\n%s\n]\n", w.toString());
  }

  @Test
  public void IO() throws NumberFormatException, IOException {
    Configuration conf = createJobConf();
    FileSystem fs = FileSystem.get(conf);
    Path f = testDir.suffix("/bigdecimalmatrix.data");
    Matrix<BigDecimal> a = (Matrix<BigDecimal>) Matrix.builder(5, 10,
        Matrix.DataType.BigDecimal, true, null, true, conf, f);
    FSDataInputStream in = fs.open(f);
    Matrix<BigDecimal> b = new Matrix<BigDecimal>();
    b.readFields(in);
    in.close();
    assertTrue(a.equals(b));
  }

  @Test
  public void bigDecimalCopyIO()
      throws NumberFormatException, IOException, MatrixBoundCopyException,
      MatrixBoundReadException, MatrixBoundWriteException {
    Matrix<BigDecimal> b = new Matrix<BigDecimal>(), c1, c2, c3;
    int i0 = 5, j0 = 10, n = 5, m = 5;
    Configuration conf = createJobConf();
    FileSystem fs = FileSystem.get(conf);
    Path f = testDir.suffix("/bigdecimalmatrix.data");
    Matrix<BigDecimal> a = (Matrix<BigDecimal>) Matrix.builder(10, 20,
        Matrix.DataType.BigDecimal, true, null, true, conf, f);
    c1 = Matrix.copy(a, i0, j0, n, m);
    FSDataInputStream in = fs.open(f);
    b = (Matrix<BigDecimal>) Matrix.readHeader(in);
    c2 = Matrix.readData(in, b, i0, j0, n, m);
    in.seek(Matrix.HEADER_SIZE);
    c3 = Matrix.readData(in, b, i0, j0, n, m);
    in.close();
    assertTrue(c1.equals(c2));
    assertTrue(c1.equals(c3));
    FSDataOutputStream out = fs.create(f);
    Matrix.writeData(out, a, i0, j0, n, m, true);
    out.close();
    in = fs.open(f);
    c2.readFields(in);
    in.close();
    assertTrue(c1.equals(c2));
  }

  @Test
  public void integerCopyIO() throws IOException, MatrixBoundCopyException,
      MatrixBoundReadException, MatrixBoundWriteException {
    Configuration conf = createJobConf();
    Path f = testDir.suffix("/integermatrix.data");
    FileSystem fs = FileSystem.get(conf);
    Matrix<Integer> a = (Matrix<Integer>) Matrix.builder(10, 20,
        Matrix.DataType.Integer, true, null, true, conf, f);
    Matrix<Integer> b = new Matrix<Integer>(), c1, c2, c3;
    int i0 = 5, j0 = 10, n = 5, m = 5;
    c1 = Matrix.copy(a, i0, j0, n, m);
    FSDataInputStream in = fs.open(f);
    b = (Matrix<Integer>) Matrix.readHeader(in);
    c2 = Matrix.readData(in, b, i0, j0, n, m);
    in.seek(Matrix.HEADER_SIZE);
    c3 = Matrix.readData(in, b, i0, j0, n, m);
    in.close();
    assertTrue(c1.equals(c2));
    assertTrue(c1.equals(c3));
    FSDataOutputStream out = fs.create(f);
    Matrix.writeData(out, a, i0, j0, n, m, true);
    out.close();
    in = fs.open(f);
    c2.readFields(in);
    in.close();
    assertTrue(c1.equals(c2));
  }

  @Test
  public void doubleCopyIO() throws IOException, MatrixBoundCopyException,
      MatrixBoundReadException, MatrixBoundWriteException {
    Configuration conf = createJobConf();
    Path f = testDir.suffix("/doublematrix.data");
    FileSystem fs = FileSystem.get(conf);
    Matrix<Double> a = (Matrix<Double>) Matrix.builder(10, 20,
        Matrix.DataType.Double, true, null, true, conf, f);
    Matrix<Double> b = new Matrix<Double>(), c1, c2, c3;
    int i0 = 5, j0 = 10, n = 5, m = 5;
    c1 = Matrix.copy(a, i0, j0, n, m);
    FSDataInputStream in = fs.open(f);
    b = (Matrix<Double>) Matrix.readHeader(in);
    c2 = Matrix.readData(in, b, i0, j0, n, m);
    in.seek(Matrix.HEADER_SIZE);
    c3 = Matrix.readData(in, b, i0, j0, n, m);
    in.close();
    assertTrue(c1.equals(c2));
    assertTrue(c1.equals(c3));
    FSDataOutputStream out = fs.create(f);
    Matrix.writeData(out, a, i0, j0, n, m, true);
    out.close();
    in = fs.open(f);
    c2.readFields(in);
    in.close();
    assertTrue(c1.equals(c2));
  }

  @Test
  public void multiplyBigIntegerBlocksPartition() throws NumberFormatException,
      IOException, MatrixBoundMultiplicationException, MatrixBoundReadException,
      MatrixBoundWriteException {
    Configuration conf = createJobConf();
    FileSystem fs = FileSystem.get(conf);
    Path xPath =
        fs.makeQualified(new Path(conf.get(MatrixParameter.inputDir, null),
            conf.get(MatrixParameter.xFilename, null)));
    Path yPath =
        fs.makeQualified(new Path(conf.get(MatrixParameter.inputDir, null),
            conf.get(MatrixParameter.yFilename, null)));
    Path rPath =
        fs.makeQualified(new Path(conf.get(MatrixParameter.outputDir, null),
            conf.get(MatrixParameter.rPrefixFilename, null)));
    Matrix<BigInteger> x = (Matrix<BigInteger>) Matrix.builder(
        conf.getInt(MatrixParameter.xWidth, 0),
        conf.getInt(MatrixParameter.xHeight, 0), Matrix.DataType.BigInteger,
        true, null, true, conf, xPath);
    Matrix<BigInteger> y = (Matrix<BigInteger>) Matrix.builder(
        conf.getInt(MatrixParameter.yWidth, 0),
        conf.getInt(MatrixParameter.yHeight, 0), Matrix.DataType.BigInteger,
        true, null, true, conf, yPath);
    Matrix<BigInteger> z1, z2;
    DataOperator<BigInteger, BigInteger, BigInteger> dataOperator =
        (DataOperator<BigInteger, BigInteger, BigInteger>) DataOperator
            .builder(x.getDataType(), y.getDataType());
    Matrix.multiply(rPath, xPath, yPath,
        conf.getInt(MatrixParameter.nBlocks, 0),
        conf.getInt(MatrixParameter.mBlocks, 0),
        conf.getInt(MatrixParameter.nTasks, 0),
        conf.getInt(MatrixParameter.mTasks, 0), conf, dataOperator.getSumR(),
        dataOperator.getMultiply());
    FSDataInputStream in = fs.open(rPath.suffix(".data"));
    z1 = new Matrix<BigInteger>();
    z1.readFields(in);
    in.close();
    z2 = Matrix.multiply(x, y, dataOperator.getSumR(),
        dataOperator.getMultiply());
    assertTrue(z1.equals(z2));
  }

  @Test
  public void multiplyBigDecimalBlocksPartition() throws NumberFormatException,
      IOException, MatrixBoundMultiplicationException, MatrixBoundReadException,
      MatrixBoundWriteException {
    Configuration conf = createJobConf();
    FileSystem fs = FileSystem.get(conf);
    Path xPath =
        fs.makeQualified(new Path(conf.get(MatrixParameter.inputDir, null),
            conf.get(MatrixParameter.xFilename, null)));
    Path yPath =
        fs.makeQualified(new Path(conf.get(MatrixParameter.inputDir, null),
            conf.get(MatrixParameter.yFilename, null)));
    Path rPath =
        fs.makeQualified(new Path(conf.get(MatrixParameter.outputDir, null),
            conf.get(MatrixParameter.rPrefixFilename, null)));
    Matrix<BigDecimal> x = (Matrix<BigDecimal>) Matrix.builder(
        conf.getInt(MatrixParameter.xWidth, 0),
        conf.getInt(MatrixParameter.xHeight, 0), Matrix.DataType.BigDecimal,
        true, null, true, conf, xPath);
    Matrix<BigDecimal> y = (Matrix<BigDecimal>) Matrix.builder(
        conf.getInt(MatrixParameter.yWidth, 0),
        conf.getInt(MatrixParameter.yHeight, 0), Matrix.DataType.BigDecimal,
        true, null, true, conf, yPath);
    Matrix<BigDecimal> z1, z2;
    DataOperator<BigDecimal, BigDecimal, BigDecimal> dataOperator =
        (DataOperator<BigDecimal, BigDecimal, BigDecimal>) DataOperator
            .builder(x.getDataType(), y.getDataType());
    Matrix.multiply(rPath, xPath, yPath,
        conf.getInt(MatrixParameter.nBlocks, 0),
        conf.getInt(MatrixParameter.mBlocks, 0),
        conf.getInt(MatrixParameter.nTasks, 0),
        conf.getInt(MatrixParameter.mTasks, 0), conf, dataOperator.getSumR(),
        dataOperator.getMultiply());
    FSDataInputStream in = fs.open(rPath.suffix(".data"));
    z1 = new Matrix<BigDecimal>();
    z1.readFields(in);
    in.close();
    z2 = Matrix.multiply(x, y, dataOperator.getSumR(),
        dataOperator.getMultiply());
    assertTrue(z1.equals(z2));
  }

  @Test
  public void multiplyBigIntegerBigDecimalBlocksPartition()
      throws NumberFormatException, IOException,
      MatrixBoundMultiplicationException, MatrixBoundReadException,
      MatrixBoundWriteException {
    Configuration conf = createJobConf();
    FileSystem fs = FileSystem.get(conf);
    Path xPath =
        fs.makeQualified(new Path(conf.get(MatrixParameter.inputDir, null),
            conf.get(MatrixParameter.xFilename, null)));
    Path yPath =
        fs.makeQualified(new Path(conf.get(MatrixParameter.inputDir, null),
            conf.get(MatrixParameter.yFilename, null)));
    Path rPath =
        fs.makeQualified(new Path(conf.get(MatrixParameter.outputDir, null),
            conf.get(MatrixParameter.rPrefixFilename, null)));
    Matrix<BigInteger> x = (Matrix<BigInteger>) Matrix.builder(
        conf.getInt(MatrixParameter.xWidth, 0),
        conf.getInt(MatrixParameter.xHeight, 0), Matrix.DataType.BigInteger,
        true, null, true, conf, xPath);
    Matrix<BigDecimal> y = (Matrix<BigDecimal>) Matrix.builder(
        conf.getInt(MatrixParameter.yWidth, 0),
        conf.getInt(MatrixParameter.yHeight, 0), Matrix.DataType.BigDecimal,
        true, null, true, conf, yPath);
    Matrix<BigDecimal> z1, z2;
    DataOperator<BigInteger, BigDecimal, BigDecimal> dataOperator =
        (DataOperator<BigInteger, BigDecimal, BigDecimal>) DataOperator
            .builder(x.getDataType(), y.getDataType());
    Matrix.multiply(rPath, xPath, yPath,
        conf.getInt(MatrixParameter.nBlocks, 0),
        conf.getInt(MatrixParameter.mBlocks, 0),
        conf.getInt(MatrixParameter.nTasks, 0),
        conf.getInt(MatrixParameter.mTasks, 0), conf, dataOperator.getSumR(),
        dataOperator.getMultiply());
    FSDataInputStream in = fs.open(rPath.suffix(".data"));
    z1 = new Matrix<BigDecimal>();
    z1.readFields(in);
    in.close();
    z2 = Matrix.multiply(x, y, dataOperator.getSumR(),
        dataOperator.getMultiply());
    assertTrue(z1.equals(z2));
  }

  @Test
  public void multiplyBulkBigDecimalBlocksPartition()
      throws NumberFormatException, IOException,
      MatrixBoundMultiplicationException, MatrixBoundReadException,
      MatrixBoundWriteException {
    final int W = 200, H = 400, L = 500;
    Configuration conf = createJobConf();
    FileSystem fs = FileSystem.get(conf);
    Path xPath =
        fs.makeQualified(new Path(conf.get(MatrixParameter.inputDir, null),
            conf.get(MatrixParameter.xFilename, null)));
    Path yPath =
        fs.makeQualified(new Path(conf.get(MatrixParameter.inputDir, null),
            conf.get(MatrixParameter.yFilename, null)));
    Path rPath =
        fs.makeQualified(new Path(conf.get(MatrixParameter.outputDir, null),
            conf.get(MatrixParameter.rPrefixFilename, null)));
    Matrix<BigDecimal> x = (Matrix<BigDecimal>) Matrix.builder(W, H,
        Matrix.DataType.BigDecimal, true, null, true, conf, xPath);
    Matrix<BigDecimal> y = (Matrix<BigDecimal>) Matrix.builder(H, W,
        Matrix.DataType.BigDecimal, true, null, true, conf, yPath);
    Matrix<BigDecimal> z1, z2;
    DataOperator<BigDecimal, BigDecimal, BigDecimal> dataOperator =
        (DataOperator<BigDecimal, BigDecimal, BigDecimal>) DataOperator
            .builder(x.getDataType(), y.getDataType());
    Matrix.multiply(rPath, xPath, yPath,
        conf.getInt(MatrixParameter.nBlocks, 0),
        conf.getInt(MatrixParameter.mBlocks, 0),
        conf.getInt(MatrixParameter.nTasks, 0),
        conf.getInt(MatrixParameter.mTasks, 0), conf, dataOperator.getSumR(),
        dataOperator.getMultiply());
    FSDataInputStream in = fs.open(rPath.suffix(".data"));
    z1 = new Matrix<BigDecimal>();
    z1.readFields(in);
    in.close();
    z2 = Matrix.multiply(x, y, dataOperator.getSumR(),
        dataOperator.getMultiply());
    assertTrue(z1.equals(z2));
  }

}
