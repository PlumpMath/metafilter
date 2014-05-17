package com.lemonodor.cascading.scheme;

import java.io.IOException;

import cascading.flow.FlowProcess;
import cascading.scheme.SinkCall;
import cascading.scheme.SourceCall;
import cascading.scheme.Scheme;
import cascading.tap.Tap;
import cascading.tuple.Fields;
import cascading.tuple.TupleEntry;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.RecordReader;
import org.commoncrawl.hadoop.io.mapred.ARCFileInputFormat;

public class ARC extends Scheme<JobConf, RecordReader, OutputCollector, Object[], Object[]>
{

    public static final Fields DEFAULT_SOURCE_FIELDS = new Fields(
        "url", "item");

    public ARC()
    {
        super(DEFAULT_SOURCE_FIELDS);
    }

    @Override
    public void sinkConfInit(FlowProcess<JobConf> flowprocess,
                             Tap<JobConf,RecordReader,OutputCollector> tap,
                             JobConf conf)
    {
    }

    @Override
    public void sourceConfInit(FlowProcess<JobConf> flowProcess,
                               Tap<JobConf, RecordReader, OutputCollector> tap,
                               JobConf conf)
    {
        conf.setInputFormat(ARCFileInputFormat.class);
    }


    @Override
    public void sourcePrepare(
        FlowProcess<JobConf> flowProcess,
        SourceCall<Object[], RecordReader> sourceCall)
    {
        if (sourceCall.getContext() == null)
            sourceCall.setContext(new Object[2]);

        sourceCall.getContext()[0] = sourceCall.getInput().createKey();
        sourceCall.getContext()[1] = sourceCall.getInput().createValue();
    }

    @Override
    public boolean source(FlowProcess<JobConf> flowProcess,
                          SourceCall<Object[], RecordReader> sourceCall)
        throws IOException
    {
        if (!sourceReadInput(sourceCall))
            return false;
        sourceHandleInput(sourceCall);
        return true;
    }

    private boolean sourceReadInput(
        SourceCall<Object[], RecordReader> sourceCall)
        throws IOException
    {
        Object[] context = sourceCall.getContext();

        return sourceCall.getInput().next(context[0], context[1]);
    }

    protected void sourceHandleInput(
        SourceCall<Object[], RecordReader> sourceCall)
    {
        TupleEntry result = sourceCall.getIncomingEntry();
        int index = 0;
        Object[] context = sourceCall.getContext();

        result.setString(0, ((Text) context[0]).toString());
        result.setRaw(1, context[1]);
    }

    @Override
    public void sink(FlowProcess<JobConf> flowProcess, SinkCall<Object[],
                     OutputCollector> sinkCall)
        throws IOException
    {
        throw new IOException("Sorry, sink isn't implemented.");
    }
}
