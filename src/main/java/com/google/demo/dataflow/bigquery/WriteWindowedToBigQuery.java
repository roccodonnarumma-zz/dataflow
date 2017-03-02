/*
    Copyright 2017, Google, Inc.

    Licensed under the Apache License, Version 2.0 (the "License");
    you may not use this file except in compliance with the License.
    You may obtain a copy of the License at

      http://www.apache.org/licenses/LICENSE-2.0

    Unless required by applicable law or agreed to in writing, software
    distributed under the License is distributed on an "AS IS" BASIS,
    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
    See the License for the specific language governing permissions and
    limitations under the License.
*/
package com.google.demo.dataflow.bigquery;

import com.google.api.services.bigquery.model.TableRow;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PDone;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Map;

public class WriteWindowedToBigQuery<T> extends WriteToBigQuery<T> {

    private Logger logger = LogManager.getLogger();

    public WriteWindowedToBigQuery(String tableName, Map<String, FieldInfo<T>> fieldInfo) {
        super(tableName, fieldInfo);
    }

    /** Convert each key/value pair into a BigQuery TableRow. */
    protected class BuildRowFn extends DoFn<T, TableRow> {
        @ProcessElement
        public void processElement(ProcessContext c, BoundedWindow window) {

            TableRow row = new TableRow();
            for (Map.Entry<String, FieldInfo<T>> entry : fieldInfo.entrySet()) {
                String key = entry.getKey();
                FieldInfo<T> fcnInfo = entry.getValue();
                row.set(key, fcnInfo.getFieldFn().apply(c, window));
            }
            c.output(row);
        }
    }

    @Override
    public PDone expand(PCollection<T> regionAndValue) {
        logger.info("Writing to BQ");
        return regionAndValue
                .apply("ConvertToRow", ParDo.of(new BuildRowFn()))
                .apply(BigQueryIO.Write
                        .to(getTable(tableName))
                        .withSchema(getSchema())
                        .withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_IF_NEEDED)
                        .withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_APPEND));
    }
}
