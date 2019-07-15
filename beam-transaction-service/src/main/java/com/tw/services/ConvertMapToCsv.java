package com.tw.services;

import com.fasterxml.jackson.dataformat.csv.CsvMapper;
import com.fasterxml.jackson.dataformat.csv.CsvSchema;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.Writer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

public class ConvertMapToCsv {

    public static void main(String[] args) throws IOException {

        List<HashMap<String, Integer>> myArrList = new ArrayList();
        HashMap<String, Integer> map;

        map = new HashMap<>();
        map.put("AccountID", 90);
        map.put("Balance", 1100);
        myArrList.add(map);

        map = new HashMap<>();
        map.put("AccountID", 91);
        map.put("Balance", 1100);
        myArrList.add(map);

        map = new HashMap<>();
        map.put("AccountID", 92);
        map.put("Balance", 1100);
        myArrList.add(map);

        map = new HashMap<>();
        map.put("AccountID", 93);
        map.put("Balance", 1100);
        myArrList.add(map);

        map = new HashMap<>();
        map.put("AccountID", 94);
        map.put("Balance", 1100);
        myArrList.add(map);

        map = new HashMap<>();
        map.put("AccountID", 95);
        map.put("Balance", 1100);
        myArrList.add(map);

        map = new HashMap<>();
        map.put("AccountID", 96);
        map.put("Balance", 1100);
        myArrList.add(map);

        map = new HashMap<>();
        map.put("AccountID", 97);
        map.put("Balance", 1100);
        myArrList.add(map);

        map = new HashMap<>();
        map.put("AccountID", 98);
        map.put("Balance", 1100);
        myArrList.add(map);

        map = new HashMap<>();
        map.put("AccountID", 99);
        map.put("Balance", 1100);
        myArrList.add(map);

        File file = new File("src/main/resources/db_data.csv");
        Writer writer = new FileWriter(file, false);
        csvWriter(myArrList, writer);
    }

    public static void csvWriter(List<HashMap<String, Integer>> listOfMap, Writer writer) throws IOException {
        CsvSchema schema = null;
        CsvSchema.Builder schemaBuilder = CsvSchema.builder();
        if (listOfMap != null && !listOfMap.isEmpty()) {
            for (String col : listOfMap.get(0).keySet()) {
                schemaBuilder.addColumn(col);
            }
            schema = schemaBuilder.build().withLineSeparator("\r");
        }
        CsvMapper mapper = new CsvMapper();
        mapper.writer(schema).writeValues(writer).writeAll(listOfMap);
        writer.flush();
    }

}
