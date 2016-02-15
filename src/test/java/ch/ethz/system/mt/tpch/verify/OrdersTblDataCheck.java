package ch.ethz.system.mt.tpch.verify;


import org.testng.annotations.AfterSuite;
import org.testng.annotations.BeforeSuite;
import org.testng.annotations.Test;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.StringTokenizer;

public class OrdersTblDataCheck {

    HashMap<String, HashMap<String, String>> orderMap = new HashMap<>();
    HashMap<String, List<String>> customerMap = new HashMap<>();

    @BeforeSuite
    public void init() throws IOException {
        init_orderMap();
        init_custMap();
    }

    public void init_orderMap() throws IOException {
        Path fileName = Paths.get("output/orders.tbl");
        InputStream in = Files.newInputStream(fileName);
        BufferedReader bf = new BufferedReader(new InputStreamReader(in));
        String line;
        HashMap<String, String> mediator = new HashMap<>();

        String currentTid = "1";
        while ((line = bf.readLine()) != null) {
            StringTokenizer tokenizer = new StringTokenizer(line, "|");
            int count = 0;

            String tid = null, orderid= null, custid = null;
            while (tokenizer.hasMoreTokens()) {
                count ++;
                switch (count) {
                    case 1:
                        tid = tokenizer.nextToken();
                        break;
                    case 2:
                        orderid = tokenizer.nextToken();
                        break;
                    case 3:
                        custid = tokenizer.nextToken();
                        break;
                }

                if (count ==3) {
                    if (!currentTid.equals(tid)) {
                        orderMap.put(currentTid, mediator); // save current tenant data
                        currentTid = tid; // move on to the next tenant
                        mediator = new HashMap<>();
                    }
                    mediator.put(orderid, custid);
                    break;
                }
            }
        }
        orderMap.put(currentTid, mediator); // save the last tenant data into dataMap
        bf.close();
        in.close();
    }

    public void init_custMap() throws IOException {
        Path fileName = Paths.get("output/customer.tbl");
        InputStream in = Files.newInputStream(fileName);
        BufferedReader bf = new BufferedReader(new InputStreamReader(in));
        String line;
        ArrayList<String> list = new ArrayList<>();

        String currentTid = "1";
        while ((line = bf.readLine()) != null) {
            StringTokenizer tokenizer = new StringTokenizer(line, "|");
            int count = 0;
            String tid = null, custid = null;
            while (tokenizer.hasMoreTokens()) {
                count ++;
                switch (count) {
                    case 1:
                        tid = tokenizer.nextToken();
                        break;
                    case 2:
                        custid = tokenizer.nextToken();
                        break;
                }

                if (count == 2) {
                    if (!currentTid.equals(tid)) {
                        customerMap.put(currentTid, list); // save current tenant data
                        currentTid = tid; // move on to the next tenant
                        list = new ArrayList<>();
                    }
                    list.add(custid);
                    break;
                }
            }
        }
        customerMap.put(currentTid, list); //save the last tenant data
        bf.close();
        in.close();
    }

    @Test
    public void testDataNotEmpyt() {
        System.out.println(orderMap.get("1"));
        System.out.println(orderMap.get("2"));
        System.out.println(orderMap.get("3"));
        System.out.println(orderMap.get("4"));
        System.out.println(orderMap.get("5"));

        System.out.println(customerMap.get("1"));
        System.out.println(customerMap.get("2"));
    }

    @Test
    public void testDataMatch() {

    }

    @AfterSuite
    public void terminate() {

    }
}