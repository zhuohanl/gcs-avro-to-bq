package com.demo;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;

import org.apache.beam.sdk.io.AvroIO;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;

import com.google.api.client.util.Charsets;
import com.google.common.io.Resources;

import java.io.IOException;


public class GcsAvroSource extends PTransform<PBegin, PCollection<GenericRecord>> {

    private String dataFilePattern;
    private Schema schema;

    public GcsAvroSource(String schemaFileName, String dataFilePattern) throws IOException {
        this.dataFilePattern = dataFilePattern;
        String schemaFile = Resources.toString(Resources.getResource(schemaFileName), Charsets.UTF_8);
        this.schema = new Schema.Parser().parse(schemaFile);
    }


    @Override
    public PCollection<GenericRecord> expand(PBegin input) {

        return input
                .apply(AvroIO.readGenericRecords(schema)
                        .from(dataFilePattern))
                .apply("PrintCheck", ParDo.of(new DoFn<GenericRecord, GenericRecord>() {
                    @ProcessElement
                    public void apply(ProcessContext c, OutputReceiver<GenericRecord> out) {
                        System.out.println(c.element());
                        out.output(c.element());
                    }
                }));

    }
}
