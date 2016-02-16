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
 *  3. change phone format - random choose format, and then generate data accordingly
 */
public class dbgen {

    public static final String OUTPUT_DIRECTORY = "output";

    public static void main(String args[]) {

        double scaleFactor = 1; //set default value for Scale Factor
        int part = 1;
        int numberOfParts = 1; //adjust this value for different parts of data
        int tenant = 1; //set default value for tenant number
        int[] custDataSize     = new int[tenant];
        int[] lineItemDataSize = new int[tenant];
        int[] orderDataSize    = new int[tenant];
        int[] suppDataSize     = new int[tenant];
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
                if (!String.valueOf(mode).equals("uniform") && !String.valueOf(mode).equals("zipf") ) {
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

        CustomerGenerator customerGenerator = new CustomerGenerator(scaleFactor, part, numberOfParts, tenant);
        SupplierGenerator supplierGenerator = new SupplierGenerator(scaleFactor, part, numberOfParts, tenant);

        LineItemGenerator lineItemGenerator = new LineItemGenerator(scaleFactor, part, numberOfParts, tenant);
        int lineItemRowCount = Iterators.size(lineItemGenerator.iterator()); //get the real size of lineItem from LineItemGenerator
        lineItemGenerator = new LineItemGenerator(scaleFactor, part, numberOfParts, tenant, lineItemRowCount); //use the real row count to generate new data

        OrderGenerator orderGenerator = new OrderGenerator(scaleFactor, part, numberOfParts, tenant);

        switch(disMode) {
            case "uniform":
                custDataSize     = DbGenUtil.uniformDataDist(tenant, customerGenerator.dataPerTenant, customerGenerator.lastTenantData);
                suppDataSize     = DbGenUtil.uniformDataDist(tenant, supplierGenerator.dataPerTenant, supplierGenerator.lastTenantData);
                lineItemDataSize = DbGenUtil.uniformDataDist(tenant, lineItemGenerator.dataPerTenant, lineItemGenerator.lastTenantData);
                orderDataSize    = DbGenUtil.uniformDataDist(tenant, orderGenerator.dataPerTenant, orderGenerator.lastTenantData);
                break;
            case "zipf":

                int rowCount = Iterators.size(customerGenerator.iterator());
                custDataSize = DbGenUtil.zipfDataDist(tenant, rowCount);

                rowCount = Iterators.size(supplierGenerator.iterator());
                suppDataSize = DbGenUtil.zipfDataDist(tenant, rowCount);

                lineItemRowCount = Iterators.size(lineItemGenerator.iterator());
                lineItemGenerator = new LineItemGenerator(scaleFactor, part, numberOfParts, tenant, lineItemRowCount); //use the real row count to generate new data
                rowCount = lineItemRowCount;
                lineItemDataSize = DbGenUtil.zipfDataDist(tenant, rowCount);

                rowCount = Iterators.size(orderGenerator.iterator());
                orderDataSize = DbGenUtil.zipfDataDist(tenant, rowCount);
                break;
        }

        try {
            /*** Customer Table Generator ***/
            file = new File(OUTPUT_DIRECTORY + "//customer.tbl");
            FileUtil.checkParentDirector(file); //check and create output directory
            writer = new FileWriter(file);

            System.out.println("Generating data for customers table");
            DbGenUtil.generator(customerGenerator, custDataSize, writer);

            /*** Supplier Table Generator ***/
            writer = new FileWriter(OUTPUT_DIRECTORY + "//supplier.tbl");
            System.out.println("Generating data for supplier table");
            DbGenUtil.generator(supplierGenerator, suppDataSize, writer);

            /*** Lineitem Table Generator ***/
            writer = new FileWriter(OUTPUT_DIRECTORY + "//lineitem.tbl");
            System.out.println("Generating data for lineitem table");
            DbGenUtil.generator(lineItemGenerator, lineItemDataSize, writer);

            /*** Orders Table Generator ***/
            writer = new FileWriter(OUTPUT_DIRECTORY + "//orders.tbl");
            System.out.println("Generating data for orders table");
            DbGenUtil.generator(orderGenerator, orderDataSize, writer);

            /*** Nation Table Generator ***/
            writer = new FileWriter(OUTPUT_DIRECTORY + "//nation.tbl");
            System.out.println("Generating data for nation table");
            for (Nation entity : new NationGenerator()) {
                writer.write(entity.toLine());
                writer.write('\n');
            }
            writer.close();

            /*** Region Table Generator ***/
            writer = new FileWriter(OUTPUT_DIRECTORY + "//region.tbl");
            System.out.println("Generating data for region table");
            for (Region entity : new RegionGenerator()) {
                writer.write(entity.toLine());
                writer.write('\n');
            }
            writer.close();

        } catch (IOException e) {
            e.printStackTrace();
        }

        System.out.println("### DB Generate Done");
    }
}
