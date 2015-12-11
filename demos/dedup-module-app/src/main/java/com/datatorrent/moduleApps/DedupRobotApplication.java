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
import com.datatorrent.api.annotation.ApplicationAnnotation;
import com.datatorrent.common.util.BaseOperator;
import com.datatorrent.lib.io.fs.AbstractFileOutputOperator;
import com.datatorrent.modules.DedupModule;
import com.datatorrent.modules.delimitedToPojo.DelimitedToPojoConverterModule;


@ApplicationAnnotation(name = "DedupRobotApp")
public class DedupRobotApplication implements StreamingApplication
{

  @Override
  public void populateDAG(DAG dag, Configuration conf)
  {
    // Add Operators and Modules
    FileInputOperator i = dag.addOperator("Input", FileInputOperator.class);
    DelimitedToPojoConverterModule converter = dag.addModule("Converter", DelimitedToPojoConverterModule.class);
    DedupModule dedup = dag.addModule("DedupModule", DedupModule.class);
    FileOutputOperator unique = dag.addOperator("Unique", FileOutputOperator.class);
    FileOutputOperator duplicate = dag.addOperator("Duplicate", FileOutputOperator.class);
    FileOutputOperator expired = dag.addOperator("Expired", FileOutputOperator.class);
    FileOutputOperator error = dag.addOperator("Error", FileOutputOperator.class);

    // Add streams
    dag.addStream("Input-Converter", i.output, converter.input);
    dag.addStream("Converter-Dedup", converter.output, dedup.input);
    dag.addStream("Dedup-Verifier-Unique", dedup.unique, unique.input);
    dag.addStream("Dedup-Verifier-Duplicate", dedup.duplicate, duplicate.input);
    dag.addStream("Dedup-Verifier-Expired", dedup.expired, expired.input);
    dag.addStream("Dedup-Verifier-Error", dedup.error, error.input);
  }

//  public static class Input implements InputOperator
//  {
//    public final transient DefaultOutputPort<byte[]> output = new DefaultOutputPort<byte[]>();
//    private Queue<String> queue;
//    public int usecaseId;
////    public String outputPath = "target";
//    public String generatorScriptLocation = "src/test/resources";
//    public String baseDataFile = "";
////    BufferedWriter input = null;
//
//    @Override
//    public void beginWindow(long windowId)
//    {
//    }
//
//    @Override
//    public void endWindow()
//    {
//    }
//
//    @Override
//    public void setup(OperatorContext context)
//    {
////      try {
////        input = new BufferedWriter(new FileWriter(outputPath+"/input"));
////      } catch (IOException ie) {
////        throw new RuntimeException("Exception in writing data", ie);
////      }
//      queue = Queues.newConcurrentLinkedQueue();
//      TimerTask t = new TimerTask()
//      {
//        @Override
//        public void run()
//        {
//          try {
//            Runtime rt = Runtime.getRuntime();
//            String[] commands = new String[3];
//            switch(usecaseId) {
//              case 1:
//                commands[0] = "/bin/sh";
//                commands[1] = "-c";
//                commands[2] = "hadoop fs -cat " + baseDataFile + " | awk -f src/test/resources/genRandomOrdered.awk";
//                break;
//            }
//            Process proc = rt.exec(commands);
//
//            BufferedReader stdInput = new BufferedReader(new InputStreamReader(proc.getInputStream()));
//
//            BufferedReader stdError = new BufferedReader(new InputStreamReader(proc.getErrorStream()));
//
//            // read the output from the command
//            String s = "";
//
//            while ((s = stdInput.readLine()) != null) {
//              queue.add(s);
//              System.out.println(s);
////              input.write(s+"\n");
////              input.flush();
//            }
//
//            // read any errors from the attempted command
//            while ((s = stdError.readLine()) != null) {
//            }
//
//          } catch (IOException e) {
//            throw new RuntimeException("exception in data generation", e);
//          }
//        }
//      };
//      new Timer().schedule(t, 0);
//    }
//
//    @Override
//    public void teardown()
//    {
////      try {
////        input.flush();
////        input.close();
////      } catch (IOException e) {
////        throw new RuntimeException("Exception in closing writer", e);
////      }
//    }
//
//    @Override
//    public void emitTuples()
//    {
//      if (!queue.isEmpty()) {
//        String s = queue.poll();
//        output.emit(s.getBytes());
//      }
//    }
//
//    public int getUsecaseId()
//    {
//      return usecaseId;
//    }
//
//    public void setUsecaseId(int usecaseId)
//    {
//      this.usecaseId = usecaseId;
//    }
//  }

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
    String path;

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
