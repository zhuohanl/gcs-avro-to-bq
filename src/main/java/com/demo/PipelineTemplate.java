package com.demo;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.options.PipelineOptionsFactory;

public class PipelineTemplate {

    public static void main(String[] args) throws Exception {

        PipelineTemplateOptions options = PipelineOptionsFactory
                .fromArgs(args).withValidation().as(PipelineTemplateOptions.class);

        Pipeline pipeline = Pipeline.create(options);

        GcsAvroSource source = new GcsAvroSource(options.getSchemaFileName(), options.getDataFilePattern());

        pipeline
                .apply(source);

        pipeline.run();
    }
}
