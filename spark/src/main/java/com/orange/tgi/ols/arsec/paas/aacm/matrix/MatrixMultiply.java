package com.orange.tgi.ols.arsec.paas.aacm.matrix;

import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
// RowMatrix
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.linalg.Vectors;
import org.apache.spark.mllib.linalg.distributed.RowMatrix;
// IndexedRowMatrix
import org.apache.spark.mllib.linalg.distributed.IndexedRow;
import org.apache.spark.mllib.linalg.distributed.IndexedRowMatrix;
// CoordinateMatrix
import org.apache.spark.mllib.linalg.distributed.MatrixEntry;
import org.apache.spark.mllib.linalg.distributed.CoordinateMatrix;
// BlockMatrix
import org.apache.spark.mllib.linalg.distributed.BlockMatrix;

public class MatrixMultiply {

  
  public static void main(String[] args) {
    SparkConf conf = new SparkConf().setAppName("Matrix Multiplication");
    JavaSparkContext sc = new JavaSparkContext(conf);
  }
  
  public void createRowMatrix(JavaSparkContext sc, int n, int m) {
    JavaRDD<Vector> rows = sc.parallelize(Vectors.zeros(n*m));
  }
  
  public void createIndexedRowMatrix(JavaSparkContext sc, int n, int m) {
    
  }
  
  public void createCoordinateMatrix(JavaSparkContext sc, int n, int m) {
    
  }
  
  public void createBlockMatrix(JavaSparkContext sc, int n, int m) {
    
  }

}
