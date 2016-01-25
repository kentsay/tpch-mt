package ch.ethz.system.mt.tpch;

import org.apache.commons.cli.*;
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
        int numberOfParts = 150;
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
            /*** Customer Table Generator ***/
            CustomerGenerator customerGenerator = new CustomerGenerator(scaleFactor, part, numberOfParts, tenant);
            int custDataSize = customerGenerator.dataPerTenant;

            file = new File(OUTPUT_DIRECTORY + "//customer.tbl");
            FileUtil.checkParentDirector(file); //check and create output directory
            writer = new FileWriter(file);

            DbGenUtil.generator(customerGenerator, custDataSize, writer);

            /*** Supplier Table Generator ***/
            SupplierGenerator supplierGenerator = new SupplierGenerator(scaleFactor, part, numberOfParts, tenant);
            int suppDataSize = supplierGenerator.dataPerTenant;
            writer = new FileWriter(OUTPUT_DIRECTORY + "//supplier.tbl");

            DbGenUtil.generator(supplierGenerator, suppDataSize, writer);

            /*** Lineitem Table Generator ***/
            // TODO: 24/01/2016  
            writer = new FileWriter(OUTPUT_DIRECTORY + "//lineitem.tbl");
            for (LineItem entity : new LineItemGenerator(scaleFactor, part, numberOfParts)) {
                writer.write(entity.toLine());
                writer.write('\n');
            }
            writer.close();

            /*** Orders Table Generator ***/
            // TODO: 24/01/2016  
            writer = new FileWriter(OUTPUT_DIRECTORY + "//orders.tbl");
            for (Order entity : new OrderGenerator(scaleFactor, part, numberOfParts)) {
                writer.write(entity.toLine());
                writer.write('\n');
            }
            writer.close();

            // TODO: 24/01/2016  
//            writer = new FileWriter("nation.tbl");
//            for (Nation entity : new NationGenerator(scaleFactor, part, numberOfParts)) {
//                writer.write(entity.toLine());
//                writer.write('\n');
//            }
//            writer.close();
//
//            writer = new FileWriter("region.tbl");
//            for (Region entity : new RegionGenerator(scaleFactor, part, numberOfParts)) {
//                writer.write(entity.toLine());
//                writer.write('\n');
//            }
//            writer.close();

            System.out.println("### DB Generate Done");
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
