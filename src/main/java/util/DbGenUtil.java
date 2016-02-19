package util;

import ch.ethz.system.mt.tpch.LineItem;
import ch.ethz.system.mt.tpch.TpchSchemaInterface;
import ch.ethz.system.mt.tpch.TpchEntity;
import com.google.common.collect.Iterators;
import org.apache.commons.math3.distribution.ZipfDistribution;

import java.io.IOException;
import java.io.Writer;


public class DbGenUtil {
    public static <T extends TpchEntity> void tenantGenerator(TpchSchemaInterface<T> schemaGenerator, int[] dataSize, Writer writer) {
        int counter = 0;
        int current_key = 1, previous_key = 1;
        int tenant_index = 1;
        int index = 0;
        try {
            for (T entity: schemaGenerator) {
                previous_key = current_key;
                current_key = (int) entity.getRowNumber();
                if (current_key == 1 && previous_key > 1) {
                    tenant_index++;
                    writer.write(tenant_index + "|" + entity.toLine() + "\n");
                } else {
                    writer.write(tenant_index + "|" + entity.toLine() + "\n");
                }
                /**
                 * case 1: 1,2,3,1,2,3
                 * case 2: 1,1,1,2,3,1
                 *
                 */
//                if (index < dataSize.length && counter < dataSize[index]) {
//                    writer.write(tenant_index + "|" + entity.toLine() + "\n");
//                } else {
//                    tenant_index++;
//                    writer.write(tenant_index + "|" + entity.toLine() + "\n");
//                    counter = 0;
//                    index++;
//                }
//                counter++;
            }
            writer.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static int[] uniformDataDist(int numOfTenant, int rowCount) {
        int normalSize = rowCount/numOfTenant;
        int lastSize = normalSize + (rowCount % numOfTenant);
        int[] dataSize = new int[numOfTenant];

        for (int i = 0; i < numOfTenant; i++) {
            dataSize[i] = normalSize;
        }
        dataSize[numOfTenant-1] = lastSize;
        return dataSize;
    }

    public static int[] zipfDataDist(int numOfTenant, int rowCount) {
        int[] dataDist = new int[numOfTenant];
        ZipfDistribution distribution = new ZipfDistribution(numOfTenant,1);
        int sum = 0;
        int probability = 0;
        for (int i = 1; i <= numOfTenant; i++) {
            probability = (int) Math.floor(distribution.probability(i)*rowCount);
            dataDist[i-1] = probability;
            sum = sum + probability;
        }
        dataDist[0] = dataDist[0] + (rowCount-sum);
        return dataDist;
    }
}
