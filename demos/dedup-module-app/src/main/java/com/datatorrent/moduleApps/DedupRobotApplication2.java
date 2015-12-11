/**
 * Put your copyright and license info here.
 */
package com.datatorrent.moduleApps;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import com.datatorrent.api.DAG;
import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.InputOperator;
import com.datatorrent.api.StreamingApplication;
import com.datatorrent.api.Context.OperatorContext;
import com.datatorrent.api.DAG.Locality;
import com.datatorrent.api.annotation.ApplicationAnnotation;
import com.datatorrent.common.util.BaseOperator;
import com.datatorrent.lib.io.fs.AbstractFileOutputOperator;
import com.datatorrent.modules.DedupModule;
import com.datatorrent.modules.delimitedToPojo.DelimitedToPojoConverterModule;
import com.datatorrent.modules.utils.BaseDataGenerator;
import com.datatorrent.modules.utils.OrderedDataGenerator;
import com.datatorrent.modules.utils.SystemTimeDataGenerator;
import com.datatorrent.modules.utils.TimeDataGenerator;


@ApplicationAnnotation(name = "DedupRobotApp2")
public class DedupRobotApplication2 implements StreamingApplication
{

  @Override
  public void populateDAG(DAG dag, Configuration conf)
  {
    // Add Operators and Modules
    Input i = dag.addOperator("Input", Input.class);
    DelimitedToPojoConverterModule converter = dag.addModule("Converter", DelimitedToPojoConverterModule.class);
    DedupModule dedup = dag.addModule("DedupModule", DedupModule.class);
    FileOutputOperator unique = dag.addOperator("Unique", FileOutputOperator.class);
    FileOutputOperator duplicate = dag.addOperator("Duplicate", FileOutputOperator.class);
    FileOutputOperator expired = dag.addOperator("Expired", FileOutputOperator.class);
    FileOutputOperator error = dag.addOperator("Error", FileOutputOperator.class);

    // Add streams
    dag.addStream("Input-Converter", i.output, converter.input).setLocality(Locality.THREAD_LOCAL);
    dag.addStream("Converter-Dedup", converter.output, dedup.input).setLocality(Locality.THREAD_LOCAL);
    dag.addStream("Dedup-Verifier-Unique", dedup.unique, unique.input);
    dag.addStream("Dedup-Verifier-Duplicate", dedup.duplicate, duplicate.input);
    dag.addStream("Dedup-Verifier-Expired", dedup.expired, expired.input);
    dag.addStream("Dedup-Verifier-Error", dedup.error, error.input);
  }

  public static class Input extends BaseOperator implements InputOperator
  {
    private int useCaseId;
    private int numTuples = 1000000000;
    private int expiryPeriod;
    private int count = 0;
    private transient BaseDataGenerator dg;
    public transient DefaultOutputPort<byte[]> output = new DefaultOutputPort<byte[]>();

    @Override
    public void setup(OperatorContext context)
    {
      switch(useCaseId)
      {
        case 1: 
          dg = new BaseDataGenerator(numTuples);
          break;
        case 2: 
          dg = new OrderedDataGenerator(numTuples, expiryPeriod);
          break;
        case 3: 
          dg = new TimeDataGenerator(numTuples, expiryPeriod);
          break;
        case 4: 
          dg = new SystemTimeDataGenerator(numTuples, expiryPeriod);
          break;
        default:
          throw new RuntimeException("Data generator for usecase " + useCaseId + " not implemented yet");
      }
    }

    @Override
    public void emitTuples()
    {
      String tuple = dg.generateNextTuple();
      if(tuple.trim().length() > 0) {
        output.emit(tuple.getBytes());
        count++;
        if(count >= numTuples) {
          
        }
      }
    }
    
    public int getUseCaseId()
    {
      return useCaseId;
    }

    public void setUseCaseId(int useCaseId)
    {
      this.useCaseId = useCaseId;
    }

    public int getNumTuples()
    {
      return numTuples;
    }

    public void setNumTuples(int numTuples)
    {
      this.numTuples = numTuples;
    }

    public int getExpiryPeriod()
    {
      return expiryPeriod;
    }

    public void setExpiryPeriod(int expiryPeriod)
    {
      this.expiryPeriod = expiryPeriod;
    }
  }

  public static class FileInputOperator extends BaseOperator implements InputOperator
  {
    private transient BufferedReader bufferedReader;
    private transient FileSystem fs;
    public transient final DefaultOutputPort<byte[]> output = new DefaultOutputPort<byte[]>();
    String path;
    long shutdown = 0;

    @Override
    public void setup(OperatorContext context)
    {
      try {
        fs = FileSystem.newInstance(new Configuration());
        bufferedReader = new BufferedReader(new InputStreamReader(fs.open(new Path(path))));
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
      shutdown = 0;
    }

    @Override
    public void emitTuples()
    {
      try {
        String s = bufferedReader.readLine();
        if(s != null) {
          output.emit(s.getBytes());
        }
        else {
          shutdown++;
          if(shutdown > 500) {
            throw new ShutdownException();
          }
        }
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
      
    }
  }

  public static class FileOutputOperator extends BaseOperator
  {
    private transient BufferedWriter bufferedWriter;
    private transient FileSystem fs;
    private String path;
    public transient final DefaultInputPort<Object> input = new DefaultInputPort<Object>()
    {
      @Override
      public void process(Object tuple)
      {
        try {
          bufferedWriter.write(tuple.toString()+"\n");
        } catch (IOException e) {
          throw new RuntimeException(e);
        }
      }
    };

    @Override
    public void setup(OperatorContext context)
    {
      try {
        fs = FileSystem.newInstance(new Configuration());
        bufferedWriter = new BufferedWriter(new OutputStreamWriter(fs.create(new Path(path), true)));
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }

    @Override
    public void endWindow()
    {
      try {
        bufferedWriter.flush();
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }

    public String getPath()
    {
      return path;
    }

    public void setPath(String path)
    {
      this.path = path;
    }
  }
//  public static class Verifier extends BaseOperator
//  {
//    public transient Getter<Object, Object> getter = null;
//    long numUnique = 0;
//    long numDuplicate = 0;
//    long numExpired = 0;
//    long numError = 0;
//    public String outputPath = "target";
//    public transient FileSystem fs;
//    transient BufferedWriter errorW = null;
//
//    public final transient DefaultInputPort<Object> unique = new DefaultInputPort<Object>()
//    {
//      @Override
//      public void process(Object tuple)
//      {
//        if(! tuple.toString().contains("UNIQUE")) {
//          addToFile(errorW, tuple.toString() + " In Unique");
//        }
//        numUnique++;
//      }
//    };
//    public final transient DefaultInputPort<Object> duplicate = new DefaultInputPort<Object>()
//    {
//      @Override
//      public void process(Object tuple)
//      {
//        if(! tuple.toString().contains("DUPLICATE")) {
//          addToFile(errorW, tuple.toString() + " In Duplicate");
//        }
//        numDuplicate++;
//      }
//    };
//    public final transient DefaultInputPort<Object> expired = new DefaultInputPort<Object>()
//    {
//      @Override
//      public void process(Object tuple)
//      {
//        if(! tuple.toString().contains("EXPIRED")) {
//          addToFile(errorW, tuple.toString() + " In Expired");
//        }
//        numExpired++;
//      }
//    };
//    public final transient DefaultInputPort<Object> error = new DefaultInputPort<Object>()
//    {
//      @Override
//      public void process(Object tuple)
//      {
//      }
//    };
//
//    public void setup(OperatorContext context)
//    {
//      Path outDir = new Path(outputPath);
//      try {
//        fs = FileSystem.newInstance(outDir.toUri(), new Configuration());
//        errorW = new BufferedWriter(new OutputStreamWriter(fs.create(new Path(outputPath + "/error"))));
//      } catch (IOException e) {
//        throw new RuntimeException("IO Exception", e);
//      }
//    }
//
//    public void addToFile(BufferedWriter bw, String s)
//    {
//      try {
//        bw.write(s);
//      } catch (IOException e) {
//        throw new RuntimeException("IO Exception", e);
//      }
//    }
//
//    public void endWindow()
//    {
//      try {
//        errorW.flush();
//      } catch (IOException e) {
//        throw new RuntimeException("Exception in flushing", e);
//      }
//    }
//
//    public String getOutputPath()
//    {
//      return outputPath;
//    }
//
//    public void setOutputPath(String outputPath)
//    {
//      this.outputPath = outputPath;
//    }
//  }
}
