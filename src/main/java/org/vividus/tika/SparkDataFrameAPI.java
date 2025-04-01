package org.vividus.tika;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.*;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.tika.metadata.Metadata;
import org.apache.tika.parser.AutoDetectParser;
import org.apache.tika.parser.ParseContext;
import org.apache.tika.sax.BodyContentHandler;
import org.xml.sax.ContentHandler;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

public class SparkDataFrameAPI {
    public static void main(String[] args) {

        SparkSession session=SparkSession.builder()
                .appName("Apache Tika Spark Metadata Extractor")
                .master("local[*]")
                .config("spark.sql.avro.compression.codec","snappy")
                .getOrCreate();

        JavaSparkContext sc=new JavaSparkContext(session.sparkContext());

        String inputDirectory="F:\\Important folders\\Six Sigma Green Belt";

        List<File> files= Arrays.stream(new File(inputDirectory).listFiles())
                .filter(File::isFile)
                .collect(Collectors.toList());

        JavaRDD<File> filesRdd = sc.parallelize(files);

        JavaRDD<Row> metadataRdd=filesRdd.map(file -> {
            AutoDetectParser parser = new AutoDetectParser();
            Metadata metadata = new Metadata();
            ContentHandler handler=new BodyContentHandler(-1);
            ParseContext parseContext=new ParseContext();

            try(InputStream is=new FileInputStream(file)){
                parser.parse(is,handler,metadata,parseContext);
            }catch(Exception e){
                e.printStackTrace();
            }

            return RowFactory.create(
                    file.getName(),
                    metadata.get("author"),
                    metadata.get("title"),
                    metadata.get("Content-Type")
            );
        });

        StructType schema= DataTypes.createStructType(new StructField[]{
DataTypes.createStructField("file_name",DataTypes.StringType,false),
                DataTypes.createStructField("author",DataTypes.StringType,true),
                DataTypes.createStructField("title",DataTypes.StringType,true),
                DataTypes.createStructField("content_type",DataTypes.StringType,true)
        });

        Dataset<Row> metaDataDF= session.createDataFrame(metadataRdd, schema);

        metaDataDF.coalesce(1)
                .write()
                .format("avro")
                .mode(SaveMode.Overwrite)
                .save("src/main/resources/temp_metadata.avro");
    try {
        FileSystem fs = FileSystem.get(session.sparkContext().hadoopConfiguration());
        Path tempDir=new Path("src/main/resources/temp_metadata.avro");

        FileStatus[] status=fs.listStatus(tempDir);
        Path srcPath=null;

        for(FileStatus fileStatus:status){
            if(fileStatus.getPath().getName().startsWith("part-00000")){
                srcPath=fileStatus.getPath();
                break;
            }
        }

        if (srcPath == null) {
            throw new FileNotFoundException("No matching part-00000 file found in temp_metadata.avro");
        }

        Path destPath=new Path("src/main/resources/data_frame_output_metadata.avro");
        fs.rename(srcPath,destPath);

        fs.delete(tempDir, true);
    }catch(Exception e){
        e.printStackTrace();
    }
        sc.close();
    }
}
