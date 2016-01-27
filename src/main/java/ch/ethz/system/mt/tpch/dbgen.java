package ch.ethz.system.mt.tpch;

import com.google.common.collect.Iterators;
import org.apache.commons.cli.*;
import org.apache.commons.math3.distribution.ZipfDistribution;
import util.DbGenUtil;
import util.FileUtil;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.Writer;

/**
 * Created by kentsay on 14/01/2016.
 * Entry point to generation database data for Multi-tenant version TPC-H benchmark
 */

/**
 * TODO List
 *  1. config file for table we want to generate
 *  2. add MT features
 *      - tenant key
 *      - data distribution mode: uniform
 *      - data distribution mode: zipf
 */
public class dbgen {

    public static final String OUTPUT_DIRECTORY = "output";

    public static void main(String args[]) {

        double scaleFactor = 1; //set default value for Scale Factor
        int part = 1;
        int numberOfParts = 1500;
        int tenant = 1;
        int[] dataSize;
        String disMode = null;

        Options options = new Options();
        options.addOption("h", false, "-- display this message");
        options.addOption("s", true,  "-- set Scale Factor (SF) to <n> (default: 1)");
        options.addOption("t", true,  "-- set Number of Tenants to <n> (default: 1)");
        options.addOption("m", true,  "-- set distribution mode to <mode> (default: uniform, others: zipf");
        options.addOption("T", true,  "-- generate tables");

        CommandLineParser parser = new DefaultParser();
        try {
            CommandLine cmd = parser.parse( options, args);
            if (cmd.hasOption("h")) {
                HelpFormatter formatter = new HelpFormatter();
                formatter.printHelp("Main", options);
                System.exit(0);
            }
            if (cmd.hasOption("s")) {
                scaleFactor = Double.parseDouble(cmd.getOptionValue("s"));
            } else {
                scaleFactor = 1;
            }
            if (cmd.hasOption("t")) {
                tenant = Integer.parseInt(cmd.getOptionValue("t"));
            } else {
                tenant = 1; //set default value for tenant number
            }
            if (cmd.hasOption("m")) {
                disMode = cmd.getOptionValue("m");
            } else {
                disMode = "uniform"; //set default value for distribution mode
            }
        } catch (ParseException e) {
            e.printStackTrace();
        }

        Writer writer;
        File file;

        switch(disMode) {
            case "uniform":
                try {
                    System.out.println("### DB Generate Start");

                    /*** Customer Table Generator ***/
                    CustomerGenerator customerGenerator = new CustomerGenerator(scaleFactor, part, numberOfParts, tenant);
                    dataSize = DbGenUtil.dataSizeArray(tenant, customerGenerator.dataPerTenant, customerGenerator.lastTenantData);

                    file = new File(OUTPUT_DIRECTORY + "//customer.tbl");
                    FileUtil.checkParentDirector(file); //check and create output directory
                    writer = new FileWriter(file);
                    DbGenUtil.generator(customerGenerator, dataSize, writer);

                    /*** Supplier Table Generator ***/
                    SupplierGenerator supplierGenerator = new SupplierGenerator(scaleFactor, part, numberOfParts, tenant);
                    dataSize = DbGenUtil.dataSizeArray(tenant, supplierGenerator.dataPerTenant, supplierGenerator.lastTenantData);
                    writer = new FileWriter(OUTPUT_DIRECTORY + "//supplier.tbl");
                    DbGenUtil.generator(supplierGenerator, dataSize, writer);

                    /*** Lineitem Table Generator ***/
                    LineItemGenerator lineItemGenerator = new LineItemGenerator(scaleFactor, part, numberOfParts, tenant);
                    int lineItemRowCount = Iterators.size(lineItemGenerator.iterator()); //get the real size of lineItem from LineItemGenerator
                    lineItemGenerator = new LineItemGenerator(scaleFactor, part, numberOfParts, tenant, lineItemRowCount); //use the real row count to generate new data
                    dataSize = DbGenUtil.dataSizeArray(tenant, lineItemGenerator.dataPerTenant, lineItemGenerator.lastTenantData);
                    writer = new FileWriter(OUTPUT_DIRECTORY + "//lineitem.tbl");

                    DbGenUtil.generator(lineItemGenerator, dataSize, writer);

                    /*** Orders Table Generator ***/
                    OrderGenerator orderGenerator = new OrderGenerator(scaleFactor, part, numberOfParts, tenant);
                    dataSize = DbGenUtil.dataSizeArray(tenant, orderGenerator.dataPerTenant, orderGenerator.lastTenantData);
                    writer = new FileWriter(OUTPUT_DIRECTORY + "//orders.tbl");
                    DbGenUtil.generator(orderGenerator, dataSize, writer);

                    System.out.println("### DB Generate Done");


                    /*** Nation Table Generator ***/
                    writer = new FileWriter(OUTPUT_DIRECTORY + "//nation.tbl");
                    for (Nation entity : new NationGenerator()) {
                        writer.write(entity.toLine());
                        writer.write('\n');
                    }
                    writer.close();

                    /*** Region Table Generator ***/
                    writer = new FileWriter(OUTPUT_DIRECTORY + "//region.tbl");
                    for (Region entity : new RegionGenerator()) {
                        writer.write(entity.toLine());
                        writer.write('\n');
                    }
                    writer.close();

                } catch (IOException e) {
                    e.printStackTrace();
                }
                break;
            case "zipfs":
                //TODO
                ZipfDistribution distribution = new ZipfDistribution(10,1);

                System.out.printf(String.valueOf(distribution.getExponent()));
                break;
        }
    }
}
