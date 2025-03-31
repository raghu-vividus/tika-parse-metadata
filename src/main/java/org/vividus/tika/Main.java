package org.vividus.tika;

import org.apache.avro.Schema;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;
import org.apache.tika.metadata.Metadata;
import org.apache.tika.parser.AutoDetectParser;
import org.apache.tika.parser.ParseContext;
import org.apache.tika.sax.BodyContentHandler;
import org.xml.sax.ContentHandler;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

public class Main {
    public static void main(String[] args) {

        SparkSession session=SparkSession.builder()
                .appName("Apache Tika Spark Metadata Extractor")
                .master("local[*]")
                .getOrCreate();

        JavaSparkContext sc=new JavaSparkContext(session.sparkContext());

        String inputDirectory="F:\\Important folders\\Six Sigma Green Belt";

        List<File> files=Arrays.stream(new File(inputDirectory).listFiles())
                .filter(File::isFile)
                .collect(Collectors.toList());

        JavaRDD<File> filesRdd = sc.parallelize(files);

        JavaRDD<MetaData> metadataRdd=filesRdd.map( file -> {
            AutoDetectParser parser = new AutoDetectParser();
            Metadata metadata = new Metadata();
            ContentHandler handler=new BodyContentHandler(-1);

            ParseContext parseContext=new ParseContext();

            try(InputStream is=new FileInputStream(file)){
                parser.parse(is,handler,metadata,parseContext);
            }catch(Exception e){
                e.printStackTrace();
            }
            /*
            String schemaJson = "{"
                    + "\"type\": \"record\","
                    + "\"name\": \"MetadataRecord\","
                    + "\"fields\": ["
                    + "{\"name\": \"file_name\", \"type\": \"string\"},"
                    + "{\"name\": \"author\", \"type\": [\"null\", \"string\"], \"default\": null},"
                    + "{\"name\": \"title\", \"type\": [\"null\", \"string\"], \"default\": null},"
                    + "{\"name\": \"content_type\", \"type\": [\"null\", \"string\"], \"default\": null}"
                    + "]"
                    + "}";

            Schema schema=new Schema.Parser().parse(schemaJson);
            Because GenericRecord is not serializable and Spark wants Serializable to objects to transfer to various nodes across network
            GenericRecord record=new GenericData.Record(schema);
            record.put("file_name",file.getName());
            record.put("author",metadata.get("author")!=null ? metadata.get("author"):null);
            record.put("title",metadata.get("title")!=null? metadata.get("title"):null);
            record.put("content_type",metadata.get("Content-Type")!=null?metadata.get("Content-Type"):null);
            */
            return new MetaData(file.getName(),
                    metadata.get("author"),
                    metadata.get("title"),
                    metadata.get("Content-Type"));
        });

        List<MetaData> records=metadataRdd.collect();

        String schemaJson = "{"
                + "\"type\": \"record\","
                + "\"name\": \"MetadataRecord\","
                + "\"fields\": ["
                + "{\"name\": \"file_name\", \"type\": \"string\"},"
                + "{\"name\": \"author\", \"type\": [\"null\", \"string\"], \"default\": null},"
                + "{\"name\": \"title\", \"type\": [\"null\", \"string\"], \"default\": null},"
                + "{\"name\": \"content_type\", \"type\": [\"null\", \"string\"], \"default\": null}"
                + "]"
                + "}";

        Schema schema=new Schema.Parser().parse(schemaJson);

        File avroFile=new File("src/main/resources/output_metadata.avro");

        try{
            DatumWriter<GenericRecord> datumWriter=new SpecificDatumWriter<>(schema);
            DataFileWriter<GenericRecord> dataFileWriter=new DataFileWriter<>(datumWriter);
            dataFileWriter.create(schema,avroFile);

            for(MetaData record:records){
                GenericRecord genericRecord=new GenericData.Record(schema);
                genericRecord.put("file_name",record.getFileName());
                genericRecord.put("author",record.getAuthor()!=null ? record.getAuthor():null);
                genericRecord.put("title",record.getTitle()!=null? record.getTitle():null);
                genericRecord.put("content_type",record.getContentType()!=null?record.getContentType():null);
                dataFileWriter.append(genericRecord);
            }

            dataFileWriter.close();
        }catch(IOException e){
            e.printStackTrace();
        }

        sc.close();
    }
}