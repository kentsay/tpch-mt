package ch.ethz.system.mt.tpch.verify;

import org.testng.Assert;
import org.testng.annotations.BeforeSuite;
import org.testng.annotations.Test;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;

public class OrdersTblDataCheck {

    HashMap<String, HashMap<String, String>> orderMap = new HashMap<>();
    HashMap<String, HashSet<String>> customerMap = new HashMap<>();

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
        HashSet<String> set = new HashSet<>();

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
                        customerMap.put(currentTid, set); // save current tenant data
                        currentTid = tid; // move on to the next tenant
                        set = new HashSet<>();
                    }
                    set.add(custid);
                    break;
                }
            }
        }
        customerMap.put(currentTid, set); //save the last tenant data
        bf.close();
        in.close();
    }

    @Test
    public void testMapNotEmpyt() {
        Assert.assertNotNull(orderMap);
        Assert.assertNotNull(customerMap);
    }

    @Test(dependsOnMethods = {"testMapNotEmpyt"})
    public void testDataMatch() {
        boolean dataCorrect = true;
        for(String tid: orderMap.keySet()) {
            for(String orderKey: orderMap.get(tid).keySet()) {
                String custKey = orderMap.get(tid).get(orderKey);
                if (!customerIdExists(tid, custKey)) {
                    dataCorrect = false;
                    System.out.println("Tenant id: " + tid + " with CustomerKey: " + custKey + " cannot be found");
                }
            }
        }
        if (dataCorrect) {
            System.out.println("##################");
            System.out.println("Check complete, all data exists and are correct");
            System.out.println("##################");
        }
    }

    public boolean customerIdExists(String tid, String id) {
        if (tid != null && id != null) {
            return customerMap.get(tid).contains(id);
        } else return false;
    }

}