/**
 * Put your copyright and license info here.
 */
package com.datatorrent.moduleApps;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.util.Queue;
import java.util.Timer;
import java.util.TimerTask;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import com.google.common.collect.Queues;

import com.datatorrent.api.Context.OperatorContext;
import com.datatorrent.api.annotation.ApplicationAnnotation;
import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.InputOperator;
import com.datatorrent.api.StreamingApplication;
import com.datatorrent.api.DAG;
import com.datatorrent.api.DAG.Locality;
import com.datatorrent.common.util.BaseOperator;
import com.datatorrent.lib.io.fs.AbstractFileInputOperator;
import com.datatorrent.lib.util.PojoUtils.Getter;
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
    Verifier verifier = dag.addOperator("Verifier", Verifier.class);

    // Add streams
    dag.addStream("Input-Converter", i.output, converter.input).setLocality(Locality.THREAD_LOCAL);
    dag.addStream("Converter-Dedup", converter.output, dedup.input).setLocality(Locality.THREAD_LOCAL);
    dag.addStream("Dedup-Verifier-Unique", dedup.unique, verifier.unique).setLocality(Locality.THREAD_LOCAL);
    dag.addStream("Dedup-Verifier-Duplicate", dedup.duplicate, verifier.duplicate).setLocality(Locality.THREAD_LOCAL);
    dag.addStream("Dedup-Verifier-Expired", dedup.expired, verifier.expired).setLocality(Locality.THREAD_LOCAL);
    dag.addStream("Dedup-Verifier-Error", dedup.error, verifier.error).setLocality(Locality.THREAD_LOCAL);
  }

  public static class Input implements InputOperator
  {
    public final transient DefaultOutputPort<byte[]> output = new DefaultOutputPort<byte[]>();
    private Queue<String> queue;
    public int usecaseId;
//    public String outputPath = "target";
    public String generatorScriptLocation = "src/test/resources";
    public String baseDataFile = "";
//    BufferedWriter input = null;

    @Override
    public void beginWindow(long windowId)
    {
    }

    @Override
    public void endWindow()
    {
    }

    @Override
    public void setup(OperatorContext context)
    {
//      try {
//        input = new BufferedWriter(new FileWriter(outputPath+"/input"));
//      } catch (IOException ie) {
//        throw new RuntimeException("Exception in writing data", ie);
//      }
      queue = Queues.newConcurrentLinkedQueue();
      TimerTask t = new TimerTask()
      {
        @Override
        public void run()
        {
          try {
            Runtime rt = Runtime.getRuntime();
            String[] commands = new String[3];
            switch(usecaseId) {
              case 1:
                commands[0] = "/bin/sh";
                commands[1] = "-c";
                commands[2] = "hadoop fs -cat " + baseDataFile + " | awk -f src/test/resources/genRandomOrdered.awk";
                break;
            }
            Process proc = rt.exec(commands);

            BufferedReader stdInput = new BufferedReader(new InputStreamReader(proc.getInputStream()));

            BufferedReader stdError = new BufferedReader(new InputStreamReader(proc.getErrorStream()));

            // read the output from the command
            String s = "";

            while ((s = stdInput.readLine()) != null) {
              queue.add(s);
              System.out.println(s);
//              input.write(s+"\n");
//              input.flush();
            }

            // read any errors from the attempted command
            while ((s = stdError.readLine()) != null) {
            }

          } catch (IOException e) {
            throw new RuntimeException("exception in data generation", e);
          }
        }
      };
      new Timer().schedule(t, 0);
    }

    @Override
    public void teardown()
    {
//      try {
//        input.flush();
//        input.close();
//      } catch (IOException e) {
//        throw new RuntimeException("Exception in closing writer", e);
//      }
    }

    @Override
    public void emitTuples()
    {
      if (!queue.isEmpty()) {
        String s = queue.poll();
        output.emit(s.getBytes());
      }
    }

    public int getUsecaseId()
    {
      return usecaseId;
    }

    public void setUsecaseId(int usecaseId)
    {
      this.usecaseId = usecaseId;
    }
  }

  public static class FileInputOperator extends AbstractFileInputOperator<String>
  {
    private transient BufferedReader bufferedReader;

    public transient final DefaultOutputPort<byte[]> output = new DefaultOutputPort<byte[]>();

    @Override
    protected InputStream openFile(Path path) throws IOException {
        InputStream inputStream = super.openFile(path);
        bufferedReader = new BufferedReader(new InputStreamReader(inputStream));
        return inputStream;
    }

    @Override
    protected void closeFile(InputStream is) throws IOException {
        super.closeFile(is);
        bufferedReader.close();
    }

    @Override
    protected String readEntity() throws IOException {
        return bufferedReader.readLine();
    }

    @Override
    protected void emit(String s) {
        output.emit(s.getBytes());
    }
  }

  public static class Verifier extends BaseOperator
  {
    public transient Getter<Object, Object> getter = null;
    long numUnique = 0;
    long numDuplicate = 0;
    long numExpired = 0;
    long numError = 0;
    public String outputPath = "target";
    public transient FileSystem fs;
    transient BufferedWriter errorW = null;

    public final transient DefaultInputPort<Object> unique = new DefaultInputPort<Object>()
    {
      @Override
      public void process(Object tuple)
      {
        if(! tuple.toString().contains("UNIQUE")) {
          addToFile(errorW, tuple.toString() + " In Unique");
        }
        numUnique++;
      }
    };
    public final transient DefaultInputPort<Object> duplicate = new DefaultInputPort<Object>()
    {
      @Override
      public void process(Object tuple)
      {
        if(! tuple.toString().contains("DUPLICATE")) {
          addToFile(errorW, tuple.toString() + " In Duplicate");
        }
        numDuplicate++;
      }
    };
    public final transient DefaultInputPort<Object> expired = new DefaultInputPort<Object>()
    {
      @Override
      public void process(Object tuple)
      {
        if(! tuple.toString().contains("EXPIRED")) {
          addToFile(errorW, tuple.toString() + " In Expired");
        }
        numExpired++;
      }
    };
    public final transient DefaultInputPort<Object> error = new DefaultInputPort<Object>()
    {
      @Override
      public void process(Object tuple)
      {
      }
    };

    public void setup(OperatorContext context)
    {
      Path outDir = new Path(outputPath);
      try {
        fs = FileSystem.newInstance(outDir.toUri(), new Configuration());
        errorW = new BufferedWriter(new OutputStreamWriter(fs.create(new Path(outputPath + "/error"))));
      } catch (IOException e) {
        throw new RuntimeException("IO Exception", e);
      }
    }

    public void addToFile(BufferedWriter bw, String s)
    {
      try {
        bw.write(s);
      } catch (IOException e) {
        throw new RuntimeException("IO Exception", e);
      }
    }

    public void endWindow()
    {
      try {
        errorW.flush();
      } catch (IOException e) {
        throw new RuntimeException("Exception in flushing", e);
      }
    }

    public String getOutputPath()
    {
      return outputPath;
    }

    public void setOutputPath(String outputPath)
    {
      this.outputPath = outputPath;
    }
  }
}
