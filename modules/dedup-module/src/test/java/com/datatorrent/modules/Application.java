package com.datatorrent.modules;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Queue;
import java.util.Timer;
import java.util.TimerTask;

import org.junit.Assert;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import com.google.common.collect.Queues;

import com.datatorrent.api.Context.OperatorContext;
import com.datatorrent.api.DAG;
import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.InputOperator;
import com.datatorrent.api.StreamingApplication;
import com.datatorrent.api.annotation.ApplicationAnnotation;
import com.datatorrent.common.util.BaseOperator;
import com.datatorrent.lib.util.PojoUtils;
import com.datatorrent.lib.util.PojoUtils.Getter;
import com.datatorrent.modules.DedupModule;
import com.datatorrent.modules.delimitedToPojo.DelimitedToPojoConverterModule;
import com.datatorrent.stram.plan.logical.requests.AddStreamSinkRequest;

@ApplicationAnnotation(name = "Dedup")
public class Application implements StreamingApplication
{
  @Override
  public void populateDAG(DAG dag, Configuration conf)
  {
    // Add Operators and Modules
    Input i = dag.addOperator("Input", Input.class);
    DelimitedToPojoConverterModule converter = dag.addModule("Converter", DelimitedToPojoConverterModule.class);
    DedupModule dedup = dag.addModule("DedupModule", DedupModule.class);
    Verifier verifier = dag.addOperator("Verifier", Verifier.class);

    // Add streams
    dag.addStream("Input-Converter", i.output, converter.input);
    dag.addStream("Converter-Dedup", converter.output, dedup.input);
    dag.addStream("Dedup-Verifier-Unique", dedup.unique, verifier.unique);
    dag.addStream("Dedup-Verifier-Duplicate", dedup.duplicate, verifier.duplicate);
    dag.addStream("Dedup-Verifier-Expired", dedup.expired, verifier.expired);
    dag.addStream("Dedup-Verifier-Error", dedup.error, verifier.error);
  }

  public static class Input implements InputOperator
  {
    public final transient DefaultOutputPort<byte[]> output = new DefaultOutputPort<byte[]>();
    private Queue<String> queue;
    public int usecaseId;
    public String outputPath = "target";
    BufferedWriter input = null;

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
      try {
        input = new BufferedWriter(new FileWriter(outputPath+"/input"));
      } catch (IOException ie) {
        throw new RuntimeException("Exception in writing data", ie);
      }
      queue = Queues.newConcurrentLinkedQueue();
      TimerTask t = new TimerTask()
      {
        @Override
        public void run()
        {
          try {
            Runtime rt = Runtime.getRuntime();
            System.out.println("Use case id: " + usecaseId);
            String[] commands = {"src/test/resources/genData.sh", " " + usecaseId };
            Process proc = rt.exec(commands);

            BufferedReader stdInput = new BufferedReader(new InputStreamReader(proc.getInputStream()));

            BufferedReader stdError = new BufferedReader(new InputStreamReader(proc.getErrorStream()));

            // read the output from the command
            System.out.println("Sending data");
            String s = "";

            while ((s = stdInput.readLine()) != null) {
              queue.add(s);
              input.write(s+"\n");
              input.flush();
            }

            // read any errors from the attempted command
            while ((s = stdError.readLine()) != null) {
              System.out.println(s);
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
      try {
        input.flush();
        input.close();
      } catch (IOException e) {
        throw new RuntimeException("Exception in closing writer", e);
      }
    }

    @Override
    public void emitTuples()
    {
      if (!queue.isEmpty()) {
        String s = queue.poll();
        output.emit(s.getBytes());
//         System.out.println("Emitted: " + s);
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

  public static class Verifier extends BaseOperator
  {
    public transient Getter<Object, Object> getter = null;
    long numUnique = 0;
    long numDuplicate = 0;
    long numExpired = 0;
    long numError = 0;
    public String outputPath = "target";
    public FileSystem fs;
    BufferedWriter uniqueW = null;
    BufferedWriter duplicateW = null;
    BufferedWriter expiredW = null;
    BufferedWriter errorW = null;

    public final transient DefaultInputPort<Object> unique = new DefaultInputPort<Object>()
    {
      @Override
      public void process(Object tuple)
      {
        addToFile(uniqueW, tuple.toString());
        numUnique++;
      }
    };
    public final transient DefaultInputPort<Object> duplicate = new DefaultInputPort<Object>()
    {
      @Override
      public void process(Object tuple)
      {
        addToFile(duplicateW, tuple.toString());
        numDuplicate++;
      }
    };
    public final transient DefaultInputPort<Object> expired = new DefaultInputPort<Object>()
    {
      @Override
      public void process(Object tuple)
      {
        addToFile(expiredW, tuple.toString());
        numExpired++;
      }
    };
    public final transient DefaultInputPort<Object> error = new DefaultInputPort<Object>()
    {
      @Override
      public void process(Object tuple)
      {
        addToFile(errorW, tuple.toString());
        numError++;
      }
    };

    public void setup(OperatorContext context)
    {
      Path outDir = new Path(outputPath);
      try {
        fs = FileSystem.newInstance(outDir.toUri(), new Configuration());
        uniqueW = new BufferedWriter(new FileWriter(outputPath+"/unique"));
        duplicateW = new BufferedWriter(new FileWriter(outputPath+"/duplicate"));
        expiredW = new BufferedWriter(new FileWriter(outputPath+"/expired"));
        errorW = new BufferedWriter(new FileWriter(outputPath+"/error"));
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
      System.out.println("Counts:" + numUnique + " " + numDuplicate + " " + numExpired + " " + numError);
      try {
        uniqueW.flush();
        duplicateW.flush();
        expiredW.flush();
        errorW.flush();
      } catch (IOException e) {
        throw new RuntimeException("Exception in flushing", e);
      }
    }
  }
}
