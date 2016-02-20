package ch.ethz.system.mt.tpch;

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
public class dbgen {

    public static final String OUTPUT_DIRECTORY = "output";

    public static void main(String args[]) {

        double scaleFactor = 1; //set default value for Scale Factor
        int part = 1;
        int numberOfParts = 1; //adjust this value for different parts of data
        int tenant = 1; //set default value for tenant number
        int[] custDataSize     = new int[tenant];
        int[] orderDataSize    = new int[tenant];
        int[] suppDataSize     = new int[tenant];
        int customerRowCount, supplierRowCount, orderRowCount;
        String disMode = "uniform"; //set default value for distribution mode

        Options options = new Options();
        options.addOption("h", false, "-- display this message");
        options.addOption("s", true,  "-- set scale factor (SF) (default: 1)");
        options.addOption("t", true,  "-- set number of tenants (default: 1)");
        options.addOption("m", true,  "-- set distribution mode (uniform, zipf");

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

        System.out.println("### DB Generate Start (mode: " + disMode + ")");

        //Calculate actually row count
        customerRowCount     = (int) GenerateUtils.calculateRowCount(CustomerGenerator.SCALE_BASE, scaleFactor, part, numberOfParts);
        supplierRowCount     = (int) GenerateUtils.calculateRowCount(SupplierGenerator.SCALE_BASE, scaleFactor, part, numberOfParts);
        orderRowCount    = (int) GenerateUtils.calculateRowCount(OrderGenerator.SCALE_BASE, scaleFactor, part, numberOfParts);
        //The row count of lineitem depends on order row count and cannot be pre-calculate

        switch(disMode) {
            case "uniform":
                custDataSize     = DbGenUtil.uniformDataDist(tenant, customerRowCount);
                suppDataSize     = DbGenUtil.uniformDataDist(tenant, supplierRowCount);
                orderDataSize    = DbGenUtil.uniformDataDist(tenant, orderRowCount);
                break;
            case "zipf":
                custDataSize = DbGenUtil.zipfDataDist(tenant, customerRowCount);
                suppDataSize = DbGenUtil.zipfDataDist(tenant, supplierRowCount);
                orderDataSize = DbGenUtil.zipfDataDist(tenant, orderRowCount);
                break;
        }

        //Generate the data
        CustomerGenerator customerGenerator = new CustomerGenerator(scaleFactor, part, numberOfParts, custDataSize);
        SupplierGenerator supplierGenerator = new SupplierGenerator(scaleFactor, part, numberOfParts, suppDataSize);
        OrderGenerator orderGenerator       = new OrderGenerator(scaleFactor, part, numberOfParts, orderDataSize, custDataSize);
        LineItemGenerator lineItemGenerator = new LineItemGenerator(scaleFactor, part, numberOfParts, orderDataSize, suppDataSize);

        try {
            file = new File(OUTPUT_DIRECTORY + "//customer.tbl");
            FileUtil.checkParentDirector(file); //check and create output directory
            writer = new FileWriter(file);

            System.out.print("Generating data for customers table");
            DbGenUtil.tenantGenerator(customerGenerator, writer);
            System.out.println("...done");

            writer = new FileWriter(OUTPUT_DIRECTORY + "//supplier.tbl");
            System.out.print("Generating data for supplier table");
            DbGenUtil.tenantGenerator(supplierGenerator, writer);
            System.out.println("...done");

            writer = new FileWriter(OUTPUT_DIRECTORY + "//orders.tbl");
            System.out.print("Generating data for orders table");
            DbGenUtil.tenantGenerator(orderGenerator, writer);
            System.out.println("...done");

            writer = new FileWriter(OUTPUT_DIRECTORY + "//lineitem.tbl");
            System.out.print("Generating data for lineitem table");
            DbGenUtil.tenantGenerator(lineItemGenerator, writer);
            System.out.println("...done");

            writer = new FileWriter(OUTPUT_DIRECTORY + "//nation.tbl");
            System.out.print("Generating data for nation table");
            for (Nation entity : new NationGenerator()) {
                writer.write(entity.toLine());
                writer.write('\n');
            }
            System.out.println("...done");
            writer.close();

            writer = new FileWriter(OUTPUT_DIRECTORY + "//region.tbl");
            System.out.print("Generating data for region table");
            for (Region entity : new RegionGenerator()) {
                writer.write(entity.toLine());
                writer.write('\n');
            }
            System.out.println("...done");
            writer.close();

        } catch (IOException e) {
            e.printStackTrace();
        }

        System.out.println("### DB Generate Done");
    }
}