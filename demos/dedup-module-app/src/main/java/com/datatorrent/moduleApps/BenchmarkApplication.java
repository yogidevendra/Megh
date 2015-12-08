/**
 * Put your copyright and license info here.
 */
package com.datatorrent.moduleApps;

import org.apache.hadoop.conf.Configuration;

import com.datatorrent.api.DAG.Locality;
import com.datatorrent.api.annotation.ApplicationAnnotation;
import com.datatorrent.api.StreamingApplication;
import com.datatorrent.api.DAG;
import com.datatorrent.lib.stream.DevNull;
import com.datatorrent.modules.DedupModule;
import com.datatorrent.modules.delimitedToPojo.DelimitedToPojoConverterModule;
import com.datatorrent.modules.delimitedparser.DelimitedParserModule;
import com.datatorrent.modules.kafkainput.KafkaInputModule;

@ApplicationAnnotation(name = "DedupBenchmarkApp")
public class BenchmarkApplication implements StreamingApplication
{

  @Override
  public void populateDAG(DAG dag, Configuration conf)
  {
    KafkaInputModule kafka = dag.addModule("Kafka", KafkaInputModule.class);
    DelimitedParserModule parser = dag.addModule("Parser", DelimitedParserModule.class);
    DelimitedToPojoConverterModule converter = dag.addModule("Converter", DelimitedToPojoConverterModule.class);
    DedupModule dedup = dag.addModule("Dedup", DedupModule.class);

    DevNull<Object> hdfsUnique = dag.addOperator("HdfsUnique", DevNull.class);
    DevNull<Object> hdfsDup = dag.addOperator("HdfsDuplicate", DevNull.class);
    DevNull<Object> hdfsExpired = dag.addOperator("HdfsExpired", DevNull.class);

    dag.addStream("Kafka-Parser", kafka.output, parser.input).setLocality(Locality.THREAD_LOCAL);
    dag.addStream("Parser-Converter", parser.validatedData, converter.input).setLocality(Locality.THREAD_LOCAL);
    
    dag.addStream("Converter-Dedup", converter.output, dedup.input).setLocality(Locality.THREAD_LOCAL);
    dag.addStream("Dedup-Hdfs-unique", dedup.unique, hdfsUnique.data).setLocality(Locality.THREAD_LOCAL);
    dag.addStream("Dedup-Hdfs-duplicate", dedup.duplicate, hdfsDup.data).setLocality(Locality.THREAD_LOCAL);
    dag.addStream("Dedup-Hdfs-expired", dedup.expired, hdfsExpired.data).setLocality(Locality.THREAD_LOCAL);
  }
}
