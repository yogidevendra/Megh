/**
 * Put your copyright and license info here.
 */
package com.datatorrent.moduleApps;

import org.apache.hadoop.conf.Configuration;

import com.datatorrent.api.annotation.ApplicationAnnotation;
import com.datatorrent.api.StreamingApplication;
import com.datatorrent.api.DAG;
import com.datatorrent.module.io.fs.HDFSOutputModule;
import com.datatorrent.modules.DedupModule;
import com.datatorrent.modules.delimitedToPojo.DelimitedToPojoConverterModule;
import com.datatorrent.modules.delimitedparser.DelimitedParserModule;
import com.datatorrent.modules.kafkainput.KafkaInputModule;

@ApplicationAnnotation(name = "DedupDemoApp")
public class Application implements StreamingApplication
{

  @Override
  public void populateDAG(DAG dag, Configuration conf)
  {
    KafkaInputModule kafka = dag.addModule("Kafka", KafkaInputModule.class);
    DelimitedParserModule parser = dag.addModule("Parser", DelimitedParserModule.class);
    DelimitedToPojoConverterModule converter = dag.addModule("Converter", DelimitedToPojoConverterModule.class);
    DedupModule dedup = dag.addModule("Dedup", DedupModule.class);
    HDFSOutputModule hdfsParserError = dag.addModule("HdfsParserError", HDFSOutputModule.class);
    HDFSOutputModule hdfsConverterError = dag.addModule("HdfsConverterError", HDFSOutputModule.class);
//    HDFSOutputModule hdfsParsedData = dag.addModule("HdfsParsedData", HDFSOutputModule.class);
    HDFSOutputModule hdfsUnique = dag.addModule("HdfsUnique", HDFSOutputModule.class);
    HDFSOutputModule hdfsDup = dag.addModule("HdfsDuplicate", HDFSOutputModule.class);
    HDFSOutputModule hdfsExpired = dag.addModule("HdfsExpired", HDFSOutputModule.class);
    HDFSOutputModule hdfsError = dag.addModule("HdfsError", HDFSOutputModule.class);

    dag.addStream("Kafka-Parser", kafka.output, parser.input);
    dag.addStream("Parser-Converter", parser.validatedData, converter.input);
//    dag.addStream("Parser-Hdfs-Parsed", parser.parsedData, hdfsParsedData.input);
    dag.addStream("Parser-Hdfs-Error", parser.error, hdfsParserError.input);
    dag.addStream("Converter-Hdfs-Error", converter.error, hdfsConverterError.input);
    
    dag.addStream("Converter-Dedup", converter.output, dedup.input);
    dag.addStream("Dedup-Hdfs-unique", dedup.unique, hdfsUnique.input);
    dag.addStream("Dedup-Hdfs-duplicate", dedup.duplicate, hdfsDup.input);
    dag.addStream("Dedup-Hdfs-expired", dedup.expired, hdfsExpired.input);
    dag.addStream("Dedup-Hdfs-error", dedup.error, hdfsError.input);
    
  }
}
