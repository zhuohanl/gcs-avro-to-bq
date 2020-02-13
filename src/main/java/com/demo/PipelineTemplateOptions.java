package com.demo;

import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;

public interface PipelineTemplateOptions extends DataflowPipelineOptions {

    String getSchemaFileName();
    void setSchemaFileName(String schemaFileName);

    String getDataFilePattern();
    void setDataFilePattern(String dataFilePattern);
}
