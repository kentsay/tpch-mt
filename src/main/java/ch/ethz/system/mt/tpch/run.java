package ch.ethz.system.mt.tpch;

import org.apache.commons.cli.*;

import java.io.FileWriter;
import java.io.IOException;
import java.io.Writer;

/**
 * Created by kentsay on 14/01/2016.
 * Entry point to run the data generation of TPCH benchmark
 */

/**
 * TODO List
 *  1. command line tool (Scale Factor, Number of Tenant, Distribution mode)
 *  2. config file for table we want to generate
 *  3. move repository under mine
 *  4. add MT features
 */
public class run {

    public static void main(String args[]) {

        double scaleFactor = 1;
        int part = 1;
        int numberOfParts = 1500;

        // create Options object
        Options options = new Options();
        // add t option
        options.addOption("t", false, "display current time");
        CommandLineParser parser = new DefaultParser();
        try {
            CommandLine cmd = parser.parse( options, args);
            if(cmd.hasOption("t")) {
                // System.out.println("print date and time");
                // print the date and time
            }
            else {
                // print the date
            }
        } catch (ParseException e) {
            e.printStackTrace();
        }


        Writer writer = null;
        try {
            int tenant = 2;
            CustomerGenerator customerGenerator = new CustomerGenerator(scaleFactor, part, numberOfParts, tenant);
            int datasize = customerGenerator.dataPerTenant;

            //todo: each tenant should have different customers instead of same customers
            //solution: divide the customers generator data by the numbers of tenant
            //          add the rest to the last tenant
            //this will ensure the scaling factor remain the same
            //todo: each customer key for each tenant should start from 1

            writer = new FileWriter("customer.tbl");

            int counter = 0;
            int tenant_index =1;
            for (Customer entity : customerGenerator) {
                if (counter < datasize) {
                    writer.write(tenant_index + "|" + entity.toLine() + "\n");
                } else {
                    tenant_index ++;
                    counter = 0;
                    writer.write(tenant_index + "|" + entity.toLine() + "\n");
                }
                counter++;
            }
            writer.close();

/*            writer = new FileWriter("lineitem.tbl");
            for (LineItem entity : new LineItemGenerator(scaleFactor, part, numberOfParts)) {
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

//            writer = new FileWriter("region.tbl");
//            for (Region entity : new RegionGenerator(scaleFactor, part, numberOfParts)) {
//                writer.write(entity.toLine());
//                writer.write('\n');
//            }
//            writer.close();

            writer = new FileWriter("supplier.tbl");
            for (Supplier entity : new SupplierGenerator(scaleFactor, part, numberOfParts)) {
                writer.write(entity.toLine());
                writer.write('\n');
            }
            writer.close();

*/

        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
