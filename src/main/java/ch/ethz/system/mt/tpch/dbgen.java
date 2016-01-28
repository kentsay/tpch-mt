package ch.ethz.system.mt.tpch;

import com.google.common.collect.Iterators;
import org.apache.commons.cli.*;
import util.DbGenUtil;
import util.FileUtil;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.Writer;

/**
 * Entry point to generation database data for Multi-tenant version TPC-H benchmark
 */

/**
 * TODO List
 *  1. config file for table we want to generate
 *  2. fix foreign key issues
 *  3. add checking script/code to guarantee the tuple exists
 *  5. change phone format - random choose format, and then generate data accordingly
 */
public class dbgen {

    public static final String OUTPUT_DIRECTORY = "output";

    public static void main(String args[]) {

        double scaleFactor = 1; //set default value for Scale Factor
        int part = 1;
        int numberOfParts = 1500; //adjust this value for different parts of data
        int tenant = 1; //set default value for tenant number
        int[] dataSize;
        String disMode = "uniform"; //set default value for distribution mode

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
            }
            if (cmd.hasOption("t")) {
                tenant = Integer.parseInt(cmd.getOptionValue("t"));
            }
            if (cmd.hasOption("m")) {
                String mode = cmd.getOptionValue("m");
                if (mode != "uniform" | mode != "zipf" ) {
                    System.out.println("Distribution mode cannot be recognise. Try <uniform> or <zipf>");
                    System.exit(0);
                } else {
                    disMode = mode;
                }
            }
        } catch (ParseException e) {
            e.printStackTrace();
        }

        Writer writer;
        File file;

        System.out.println("### DB Generate Start");

        switch(disMode) {
            case "uniform":
                try {
                    /*** Customer Table Generator ***/
                    CustomerGenerator customerGenerator = new CustomerGenerator(scaleFactor, part, numberOfParts, tenant);
                    dataSize = DbGenUtil.uniformDataDist(tenant, customerGenerator.dataPerTenant, customerGenerator.lastTenantData);

                    file = new File(OUTPUT_DIRECTORY + "//customer.tbl");
                    FileUtil.checkParentDirector(file); //check and create output directory
                    writer = new FileWriter(file);
                    DbGenUtil.generator(customerGenerator, dataSize, writer);

                    /*** Supplier Table Generator ***/
                    SupplierGenerator supplierGenerator = new SupplierGenerator(scaleFactor, part, numberOfParts, tenant);
                    dataSize = DbGenUtil.uniformDataDist(tenant, supplierGenerator.dataPerTenant, supplierGenerator.lastTenantData);
                    writer = new FileWriter(OUTPUT_DIRECTORY + "//supplier.tbl");
                    DbGenUtil.generator(supplierGenerator, dataSize, writer);

                    /*** Lineitem Table Generator ***/
                    LineItemGenerator lineItemGenerator = new LineItemGenerator(scaleFactor, part, numberOfParts, tenant);
                    int lineItemRowCount = Iterators.size(lineItemGenerator.iterator()); //get the real size of lineItem from LineItemGenerator
                    lineItemGenerator = new LineItemGenerator(scaleFactor, part, numberOfParts, tenant, lineItemRowCount); //use the real row count to generate new data
                    dataSize = DbGenUtil.uniformDataDist(tenant, lineItemGenerator.dataPerTenant, lineItemGenerator.lastTenantData);
                    writer = new FileWriter(OUTPUT_DIRECTORY + "//lineitem.tbl");

                    DbGenUtil.generator(lineItemGenerator, dataSize, writer);

                    /*** Orders Table Generator ***/
                    OrderGenerator orderGenerator = new OrderGenerator(scaleFactor, part, numberOfParts, tenant);
                    dataSize = DbGenUtil.uniformDataDist(tenant, orderGenerator.dataPerTenant, orderGenerator.lastTenantData);
                    writer = new FileWriter(OUTPUT_DIRECTORY + "//orders.tbl");
                    DbGenUtil.generator(orderGenerator, dataSize, writer);

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
            case "zipf":
                try {
                    /*** Customer Table Generator ***/
                    CustomerGenerator customerGenerator = new CustomerGenerator(scaleFactor, part, numberOfParts, tenant);
                    int rowCount = Iterators.size(customerGenerator.iterator());
                    dataSize = DbGenUtil.zipfDataDist(tenant, rowCount);

                    file = new File(OUTPUT_DIRECTORY + "//customer.tbl");
                    FileUtil.checkParentDirector(file); //check and create output directory
                    writer = new FileWriter(file);

                    DbGenUtil.generator(customerGenerator, dataSize, writer);

                    /*** Supplier Table Generator ***/
                    SupplierGenerator supplierGenerator = new SupplierGenerator(scaleFactor, part, numberOfParts, tenant);
                    rowCount = Iterators.size(supplierGenerator.iterator());
                    dataSize = DbGenUtil.zipfDataDist(tenant, rowCount);

                    writer = new FileWriter(OUTPUT_DIRECTORY + "//supplier.tbl");

                    DbGenUtil.generator(supplierGenerator, dataSize, writer);

                    /*** Lineitem Table Generator ***/
                    LineItemGenerator lineItemGenerator = new LineItemGenerator(scaleFactor, part, numberOfParts, tenant);
                    int lineItemRowCount = Iterators.size(lineItemGenerator.iterator()); //get the real size of lineItem from LineItemGenerator
                    lineItemGenerator = new LineItemGenerator(scaleFactor, part, numberOfParts, tenant, lineItemRowCount); //use the real row count to generate new data
                    rowCount = lineItemRowCount;
                    dataSize = DbGenUtil.zipfDataDist(tenant, rowCount);

                    writer = new FileWriter(OUTPUT_DIRECTORY + "//lineitem.tbl");

                    DbGenUtil.generator(lineItemGenerator, dataSize, writer);

                    /*** Orders Table Generator ***/
                    OrderGenerator orderGenerator = new OrderGenerator(scaleFactor, part, numberOfParts, tenant);
                    rowCount = Iterators.size(orderGenerator.iterator());
                    dataSize = DbGenUtil.zipfDataDist(tenant, rowCount);

                    writer = new FileWriter(OUTPUT_DIRECTORY + "//orders.tbl");

                    DbGenUtil.generator(orderGenerator, dataSize, writer);

                } catch (IOException e) {
                    e.printStackTrace();
                }
                break;
        }
        System.out.println("### DB Generate Done");
    }
}
