package com.orange.tgi.ols.arsec.paas.aacm.matrix;

import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.math.BigDecimal;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.HadoopTestCase;
import org.apache.hadoop.util.ToolRunner;

import org.junit.Before;
import org.junit.Test;

import com.orange.tgi.ols.arsec.paas.aacm.matrix.DataOperator;
import com.orange.tgi.ols.arsec.paas.aacm.matrix.Matrix;
import com.orange.tgi.ols.arsec.paas.aacm.matrix.hadoop.MatrixMultiply;
import com.orange.tgi.ols.arsec.paas.aacm.matrix.hadoop.MatrixParameter;

public class TestMultiplyMatrix extends HadoopTestCase {
  private static final Logger logger =
      LogManager.getLogger(TestMultiplyMatrix.class);

  public TestMultiplyMatrix() throws IOException {
    super(LOCAL_MR, LOCAL_FS, 1, 1);
  }

  @Before
  public void setUp() throws IOException {
    Configuration conf = createJobConf();
    FileSystem fs = FileSystem.get(conf);
    Path inputDir =
        fs.makeQualified(new Path(conf.get(MatrixParameter.inputDir, null)));
    fs.mkdirs(inputDir);
    Path xPath = fs.makeQualified(
        new Path(inputDir, conf.get(MatrixParameter.xFilename, null)));
    Path yPath = fs.makeQualified(
        new Path(inputDir, conf.get(MatrixParameter.yFilename, null)));
    Matrix.builder(conf.getInt(MatrixParameter.xWidth, -1),
        conf.getInt(MatrixParameter.xHeight, -1),
        Matrix.DataTypeDescriptionID.get(conf.get(MatrixParameter.xType, null)),
        true, null, true, conf, xPath);
    Matrix.builder(conf.getInt(MatrixParameter.yWidth, -1),
        conf.getInt(MatrixParameter.yHeight, -1),
        Matrix.DataTypeDescriptionID.get(conf.get(MatrixParameter.yType, null)),
        true, null, true, conf, yPath);
  }

  @Test
  public void matrixMultiply() throws Exception {
    Configuration conf = createJobConf();
    FileSystem fs = FileSystem.get(conf);
    ToolRunner.run(conf, new MatrixMultiply(), null);
    Path xPath =
        fs.makeQualified(new Path(conf.get(MatrixParameter.inputDir, null),
            conf.get(MatrixParameter.xFilename, null)));
    Path yPath =
        fs.makeQualified(new Path(conf.get(MatrixParameter.inputDir, null),
            conf.get(MatrixParameter.yFilename, null)));
    Path rPath =
        fs.makeQualified(new Path(conf.get(MatrixParameter.outputDir, null),
            conf.get(MatrixParameter.rPrefixFilename, null)));
    Matrix<BigDecimal> x = new Matrix<BigDecimal>(),
        y = new Matrix<BigDecimal>(), z, r = new Matrix<BigDecimal>();
    FSDataInputStream in;
    in = fs.open(xPath);
    x.readFields(in);
    in.close();
    in = fs.open(yPath);
    y.readFields(in);
    in.close();
    DataOperator<BigDecimal, BigDecimal, BigDecimal> dataOperator =
        (DataOperator<BigDecimal, BigDecimal, BigDecimal>) DataOperator
            .builder(x.getDataType(), y.getDataType());
    z = Matrix.multiply(x, y, dataOperator.getSumR(),
        dataOperator.getMultiply());
    in = fs.open(yPath);
    in = fs.open(rPath.suffix(".data"));
    r.readFields(in);
    in.close();
    assertTrue(z.equals(r));
  }
}
