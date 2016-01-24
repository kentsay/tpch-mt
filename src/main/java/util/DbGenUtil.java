package util;

import ch.ethz.system.mt.tpch.TpchSchemaInterface;
import ch.ethz.system.mt.tpch.TpchEntity;

import java.io.IOException;
import java.io.Writer;


public class DbGenUtil {
    public static <T extends TpchEntity> void generator(TpchSchemaInterface<T> schemaGenerator, int dataSize, Writer writer) {
        int counter = 0;
        int tenant_index = 1;

        try {
            for (T entity: schemaGenerator) {
                if (counter < dataSize) {
                    writer.write(tenant_index + "|" + entity.toLine() + "\n");
                } else {
                tenant_index++;
                counter = 0;
                writer.write(tenant_index + "|" + entity.toLine() + "\n");
            }
            counter++;
            }
            writer.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
