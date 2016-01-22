package ch.ethz.system.mt.tpch;

import org.apache.commons.cli.*;
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
 *  1. command line tool (Scale Factor, Number of Tenant, Distribution mode)(done)
 *  2. config file for table we want to generate
 *  3. move repository under mine GitHub(done)
 *  4. add MT features
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
        String disMode = null;

        Options options = new Options();
        options.addOption("h", false, "-- display this message");
        options.addOption("s", true,  "-- set Scale Factor (SF) to <n> (default: 1)");
        options.addOption("t", true,  "-- set Number of Tenants to <n> (default: 1)");
        options.addOption("m", true,  "-- set distribution mode to <mode> (default: uniform");
        options.addOption("T", true,  "-- generate tables");

        CommandLineParser parser = new DefaultParser();
        try {
            CommandLine cmd = parser.parse( options, args);
            if (cmd.hasOption("h")) {
                HelpFormatter formatter = new HelpFormatter();
                formatter.printHelp("Main", options);
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

        Writer writer = null;
        File file = null;
        try {

            System.out.println("### DB Generate Start");
            CustomerGenerator customerGenerator = new CustomerGenerator(scaleFactor, part, numberOfParts, tenant);
            int datasize = customerGenerator.dataPerTenant;

            file = new File(OUTPUT_DIRECTORY + "//customer.tbl");
            FileUtil.checkParentDirector(file);
            writer = new FileWriter(file);

            int counter = 0;
            int tenant_index = 1;
            for (Customer entity : customerGenerator) {
                if (counter < datasize) {
                    writer.write(tenant_index + "|" + entity.toLine() + "\n");
                } else {
                    tenant_index++;
                    counter = 0;
                    writer.write(tenant_index + "|" + entity.toLine() + "\n");
                }
                counter++;
            }
            writer.close();


            writer = new FileWriter(OUTPUT_DIRECTORY + "//supplier.tbl");
            for (Supplier entity : new SupplierGenerator(scaleFactor, part, numberOfParts)) {
                writer.write(entity.toLine());
                writer.write('\n');
            }
            writer.close();
/*
            writer = new FileWriter("lineitem.tbl");
            for (LineItem entity : new LineItemGenerator(scaleFactor, part, numberOfParts)) {
                writer.write(entity.toLine());
                writer.write('\n');
            }
            writer.close();

            writer = new FileWriter("orders.tbl");
            for (Order entity : new OrderGenerator(scaleFactor, part, numberOfParts)) {
                writer.write(entity.toLine());
                writer.write('\n');
            }
            writer.close();

            writer = new FileWriter("part.tbl");
            for (Part entity : new PartGenerator(scaleFactor, part, numberOfParts)) {
                writer.write(entity.toLine());
                writer.write('\n');
            }
            writer.close();

            writer = new FileWriter("partsupp.tbl");
            for (PartSupplier entity : new PartSupplierGenerator(scaleFactor, part, numberOfParts)) {
                writer.write(entity.toLine());
                writer.write('\n');
            }
            writer.close();

//            writer = new FileWriter("nation.tbl");
//            for (Nation entity : new NationGenerator(scaleFactor, part, numberOfParts)) {
//                writer.write(entity.toLine());
//                writer.write('\n');
//            }
//            writer.close();

//            writer = new FileWriter("region.tbl");
//            for (Region entity : new RegionGenerator(scaleFactor, part, numberOfParts)) {
//                writer.write(entity.toLine());
//                writer.write('\n');
//            }
//            writer.close();

*/
            System.out.println("### DB Generate Done");
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
