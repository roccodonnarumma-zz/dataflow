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
package com.google.demo.dataflow;

import com.google.demo.dataflow.bigquery.WriteToBigQuery;
import com.google.demo.dataflow.bigquery.WriteWindowedToBigQuery;
import com.google.demo.dataflow.model.ServerInfo;
import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.io.PubsubIO;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.StreamingOptions;
import org.apache.beam.sdk.runners.PipelineRunner;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.transforms.windowing.*;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.joda.time.DateTimeZone;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

import java.util.HashMap;
import java.util.Map;
import java.util.TimeZone;

public class Main {

    private Logger logger = LogManager.getLogger();

    private static DateTimeFormatter fmt =
            DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss.SSS")
                    .withZone(DateTimeZone.forTimeZone(TimeZone.getTimeZone("UTC")));

    private static final String TOPIC_NAME = "projects/gcp-rocco/topics/publisher-dataflow-bigquery";
    private static final String TIMESTAMP_ATTRIBUTE = "timestamp_ms";

//    static final Duration FIVE_MINUTES = Duration.standardMinutes(5);
//    static final Duration TEN_MINUTES = Duration.standardMinutes(10);
    static final Duration FIVE_MINUTES = Duration.standardSeconds(3);
    static final Duration TEN_MINUTES = Duration.standardSeconds(5);


//    static final Long REGION_WINDOW = Duration.standardMinutes(1).getStandardMinutes();
//    static final Long REGION_ALLOW_LATENESS = Duration.standardMinutes(5).getStandardMinutes();
    static final Long REGION_WINDOW = Duration.standardSeconds(10).getStandardSeconds();
    static final Long REGION_ALLOW_LATENESS = Duration.standardSeconds(2).getStandardSeconds();

    public static void main(String[] args) {
        Main main = new Main();

        main.start(args);
        System.exit(0);
    }

    private void start(String[] args) {
        logger.log(Level.INFO, "Starting dataflow");

//        StreamingOptions options = PipelineOptionsFactory.fromArgs(args).withValidation().as(StreamingOptions.class);
//        options.setStreaming(true);

        DataflowPipelineOptions options = PipelineOptionsFactory.as(DataflowPipelineOptions.class);

        // For Cloud execution, set the Cloud Platform project, staging location,
        // and specify DataflowPipelineRunner or BlockingDataflowPipelineRunner.
        options.setProject("gcp-rocco");
        options.setStagingLocation("gs://dataflow-rocco");
        options.setRunner(DataflowPipelineRunner.class);

        // Create the Pipeline with the specified options.
        Pipeline p = Pipeline.create(options);

        Pipeline pipeline = Pipeline.create(options);

        // Read events from Pub/Sub using custom timestamps, which are extracted from the pubsub
        // data elements, and parse the data.
        PCollection<ServerInfo> serverEvents = pipeline
                .apply(PubsubIO.<String>read()
                        .timestampLabel(TIMESTAMP_ATTRIBUTE)
                        .topic(TOPIC_NAME)
                        .withCoder(StringUtf8Coder.of()))

                .apply("ParseServerEvent", ParDo.of(new ParseEventFn()));

        serverEvents.apply("CalculateRegionValues",
                new CalculateRegionValues(
                        Duration.standardMinutes(REGION_WINDOW),
                        Duration.standardMinutes(REGION_ALLOW_LATENESS)))

                // Write the results to BigQuery.
                .apply("WriteRegionValueSums",
                        new WriteWindowedToBigQuery<>("regions", configureWindowedTableWrite()));

        // Run the pipeline and wait for the pipeline to finish; capture cancellation requests from the
        // command line.
        PipelineResult result = pipeline.run();
        result.waitUntilFinish();
    }

    static class ParseEventFn extends DoFn<String, ServerInfo> {

        private Logger logger = LogManager.getLogger();

        private final Aggregator<Long, Long> numParseErrors =
                createAggregator("ParseErrors", Sum.ofLongs());

        @ProcessElement
        public void processElement(ProcessContext c) {
            String[] components = c.element().split(",");
            try {
                String region = components[0].trim();
                String server = components[1].trim();
                Integer value = Integer.parseInt(components[2].trim());
                c.output(new ServerInfo(region, server, value));
            } catch (ArrayIndexOutOfBoundsException | NumberFormatException e) {
                numParseErrors.addValue(1L);
                logger.error("Parse error on " + c.element() + " | " + e.getMessage());
            }
        }
    }

    static class CalculateRegionValues
            extends PTransform<PCollection<ServerInfo>, PCollection<KV<String, Integer>>> {
        private final Duration regionDuration;
        private final Duration allowedLateness;

        CalculateRegionValues(Duration regionDuration, Duration allowedLateness) {
            this.regionDuration = regionDuration;
            this.allowedLateness = allowedLateness;
        }

        @Override
        public PCollection<KV<String, Integer>> expand(PCollection<ServerInfo> infos) {
            return infos.apply("RegionValuesFixedWindows",
                    Window.<ServerInfo>into(FixedWindows.of(regionDuration))
                            // We will get early (speculative) results as well as cumulative
                            // processing of late data.
                            .triggering(AfterWatermark.pastEndOfWindow()
                                    .withEarlyFirings(AfterProcessingTime.pastFirstElementInPane()
                                            .plusDelayOf(FIVE_MINUTES))
                                    .withLateFirings(AfterProcessingTime.pastFirstElementInPane()
                                            .plusDelayOf(TEN_MINUTES)))
                            .withAllowedLateness(allowedLateness)
                            .accumulatingFiredPanes())
                    // Extract and sum teamname/score pairs from the event data.
                    .apply("ExtractRegionValue", new ExtractAndSumValue("region"));
        }
    }

    public static class ExtractAndSumValue
            extends PTransform<PCollection<ServerInfo>, PCollection<KV<String, Integer>>> {

        private String key;

        public ExtractAndSumValue(String key) {
            this.key = key;
        }

        @Override
        public PCollection<KV<String, Integer>> expand(PCollection<ServerInfo> serverInfo) {

            return serverInfo
                    .apply(MapElements
                            .via((ServerInfo sInfo) -> KV.of(sInfo.getKey(key), sInfo.getValue()))
                            .withOutputType(
                                    TypeDescriptors.kvs(TypeDescriptors.strings(), TypeDescriptors.integers())))
                    .apply(Sum.<String>integersPerKey());
        }
    }

    /**
     * Create a map of information that describes how to write pipeline output to BigQuery. This map
     * is used to write team score sums and includes event timing information.
     */
    protected static Map<String, WriteWindowedToBigQuery.FieldInfo<KV<String, Integer>>> configureWindowedTableWrite() {

        Map<String, WriteWindowedToBigQuery.FieldInfo<KV<String, Integer>>> tableConfigure =
                new HashMap<>();
        tableConfigure.put(
                "region",
                new WriteWindowedToBigQuery.FieldInfo<KV<String, Integer>>(
                        "STRING", (c, w) -> c.element().getKey()));
        tableConfigure.put(
                "total_value",
                new WriteWindowedToBigQuery.FieldInfo<KV<String, Integer>>(
                        "INTEGER", (c, w) -> c.element().getValue()));
        tableConfigure.put(
                "window_start",
                new WriteWindowedToBigQuery.FieldInfo<KV<String, Integer>>(
                        "STRING",
                        (c, w) -> {
                            IntervalWindow window = (IntervalWindow) w;
                            return fmt.print(window.start());
                        }));
        tableConfigure.put(
                "processing_time",
                new WriteWindowedToBigQuery.FieldInfo<KV<String, Integer>>(
                        "STRING", (c, w) -> fmt.print(Instant.now())));
        tableConfigure.put(
                "timing",
                new WriteWindowedToBigQuery.FieldInfo<KV<String, Integer>>(
                        "STRING", (c, w) -> c.pane().getTiming().toString()));
        return tableConfigure;
    }
}
