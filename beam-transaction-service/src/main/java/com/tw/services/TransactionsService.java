package com.tw.services;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.VarIntCoder;
import org.apache.beam.sdk.io.GenerateSequence;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.state.MapState;
import org.apache.beam.sdk.state.StateSpec;
import org.apache.beam.sdk.state.StateSpecs;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.apache.kafka.common.protocol.types.Field;
import org.joda.time.Duration;

import java.lang.reflect.Array;
import java.util.Map;
import java.util.Random;

public class TransactionsService {

    public static void main(String[] args) {
        PipelineOptions options = PipelineOptionsFactory.create();
        Pipeline p = Pipeline.create(options);

        PCollection<KV<Integer,Integer>> transaction = new TransactionsService().getData(p, 100);
        PCollectionView<Map<Integer,Integer>> position = new TransactionsService().getData(p, 1000)
                .apply(View.asMap());

        PCollectionView<Map<Integer,Integer>> dbData = new TransactionsService().getDbData(p).apply(View.asMap());

        PCollection<String> finalTransactionVal = transaction.apply("MatchTransactionalPositionDataDoFn", ParDo.of(new DoFn<KV<Integer, Integer>,
                KV<Integer, KV<Integer, Integer>>>() {

            @StateId("buffer")
            private final StateSpec<MapState<Integer, Integer>> buffer = StateSpecs.map(VarIntCoder.of(), VarIntCoder.of());

            @ProcessElement
            public void processElement(ProcessContext c, @StateId("buffer") MapState<Integer, Integer> buffer) {
                KV<Integer, Integer> e = c.element();
                Map<Integer, Integer> pos = c.sideInput(position);
                if (pos.containsKey(e.getKey())) {
                    KV<Integer, KV<Integer, Integer>> mappedRecord = KV.of(e.getKey(), KV.of(e.getValue(), pos.get(e.getKey())));

                    c.output(KV.of(e.getKey(), KV.of(e.getValue(), pos.get(e.getKey()))));
//                c.output(e.getKey() +" : "+ KV.of(e.getValue(), pos.get(e.getKey())));
                } else
                    buffer.put(e.getKey(), e.getValue());
            }
        }).withSideInputs(position))
                .apply(ParDo.of(
                        new DoFn<KV<Integer, KV<Integer, Integer>>, String>() {

                            @ProcessElement
                            public void processElement(ProcessContext c) {
                                KV<Integer, KV<Integer, Integer>> e = c.element();
                                Map<Integer, Integer> dataInDb = c.sideInput(dbData);
                                if (dataInDb.containsKey(e.getKey())) {
                                    Integer posBal = e.getValue().getValue();
                                    Integer transactionAmt = e.getValue().getKey();
                                    Integer currentBal = dataInDb.get(e.getKey());
                                    if (currentBal - transactionAmt == posBal) {
                                        c.output(e.getKey() +","+posBal);
                                    }

                                }
                            }
                        }).withSideInputs(dbData));

        finalTransactionVal.apply(TextIO.write().to("src/main/resources/sample-output").withSuffix(".csv").withWindowedWrites().withNumShards(1));

        p.run();

    }

    public PCollection<KV<Integer,Integer>> getData(Pipeline p, Integer amount){
//        final List<Integer> elementsList = Arrays.asList(100, 1000);
        return p.apply(GenerateSequence.from(0).withRate(1, Duration.standardSeconds(1)))
                .apply(Window.into(FixedWindows.of(Duration.standardSeconds(5))))
                .apply(MapElements.into(
                        TypeDescriptors.kvs(TypeDescriptors.integers(),TypeDescriptors.integers()))
                .via((Long x) ->  KV.of(90 + new Random().nextInt((100 - 98) + 1), amount)
                ))
        .apply(Distinct.create())
                ;
    }

    static class splitDataFn extends DoFn<String, KV<Integer,Integer>> {

        @ProcessElement
        public void processElement(ProcessContext c) {
            String row = c.element();
            String[] x = row.split(",");
            // Split the elements
                c.output(KV.of(Integer.parseInt(x[0]), Integer.parseInt(x[1])));
        }
    }



    public PCollection<KV<Integer, Integer>> getDbData(Pipeline p){
        PCollection<String> data = p.apply(TextIO.read().from("src/main/resources/sample.csv"));
        return data.apply(ParDo.of(new splitDataFn()));

    }




}
