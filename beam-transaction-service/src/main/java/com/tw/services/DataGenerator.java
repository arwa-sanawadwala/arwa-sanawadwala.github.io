package com.tw.services;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.GenerateSequence;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.transforms.Distinct;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.joda.time.Duration;

import java.util.Random;

public class DataGenerator {

    public PCollection<KV<Integer,Integer>> getData(Pipeline p, Integer amount){
        return p.apply(GenerateSequence.from(0).withRate(2, Duration.standardSeconds(1)))
                .apply(Window.into(FixedWindows.of(Duration.standardSeconds(5))))
                .apply(MapElements.into(
                        TypeDescriptors.kvs(TypeDescriptors.integers(),TypeDescriptors.integers()))
                        .via((Long x) ->  KV.of(90 + new Random().nextInt((100 - 90) + 1), amount)
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
        PCollection<String> data = p.apply(TextIO.read().from("src/main/resources/db_data.csv"));
        return data.apply(ParDo.of(new splitDataFn()));

    }

}
