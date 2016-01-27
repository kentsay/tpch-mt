package util;

import ch.ethz.system.mt.tpch.TpchSchemaInterface;
import ch.ethz.system.mt.tpch.TpchEntity;
import com.google.common.collect.Iterators;

import java.io.IOException;
import java.io.Writer;


public class DbGenUtil {
    public static <T extends TpchEntity> void generator(TpchSchemaInterface<T> schemaGenerator, int[] dataSize, Writer writer) {
        int counter = 0;
        int tenant_index = 1;
        int index = 0;
        try {
            for (T entity: schemaGenerator) {
                if (index < dataSize.length && counter < dataSize[index]) {
                    writer.write(tenant_index + "|" + entity.toLine() + "\n");
                } else {
                    tenant_index++;
                    writer.write(tenant_index + "|" + entity.toLine() + "\n");
                    counter = 0;
                    index++;
                }
                counter++;
            }
            writer.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static int[] dataSizeArray(int numOfTenant, int normalSize, int lastSize) {
        int[] dataSize = new int[numOfTenant];
        for (int i = 0; i < numOfTenant; i++) {
            dataSize[i] = normalSize;
        }
        dataSize[numOfTenant-1] = lastSize;
        return dataSize;
    }
}
