package com.orange.tgi.ols.arsec.paas.aacm.matrix.hadoop;

public class MatrixParameter {
  public static final String parameterPrefix = "matrix.hadoop.parameter.";
  public static final String jobName = parameterPrefix + "job.name";
  public static final String inputDir = parameterPrefix + "dir.input";
  public static final String outputDir = parameterPrefix + "dir.output";
  public static final String xType = parameterPrefix + "x.type";
  public static final String xFilename = parameterPrefix + "x.file";
  public static final String xPath = parameterPrefix + "x.path";
  public static final String xWidth = parameterPrefix + "x.n";
  public static final String xHeight = parameterPrefix + "x.m";
  public static final String xValue = parameterPrefix + "x.value";
  public static final String yType = parameterPrefix + "y.type";
  public static final String yFilename = parameterPrefix + "y.file";
  public static final String yPath = parameterPrefix + "y.path";
  public static final String yWidth = parameterPrefix + "y.n";
  public static final String yHeight = parameterPrefix + "y.m";
  public static final String yValue = parameterPrefix + "y.value";
  public static final String rPrefixFilename =
      parameterPrefix + "r.prefix.file";
  public static final String rPrefixPath = parameterPrefix + "r.prefix.path";
  public static final String nBlocks = parameterPrefix + "block.n";
  public static final String mBlocks = parameterPrefix + "block.m";
  public static final String nTasks = parameterPrefix + "mapper.task.n";
  public static final String mTasks = parameterPrefix + "mapper.task.m";
  public static final String nThreads = parameterPrefix + "mapper.thread.n";
  public static final String mThreads = parameterPrefix + "mapper.thread.m";
  public static final String nReducers = parameterPrefix + "reducer.task.n";
}
