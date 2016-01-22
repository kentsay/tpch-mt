package util;

import java.io.File;

/**
 * Created by kentsay on 22.01.16.
 */
public class FileUtil {

    public static void checkParentDirector(File file) {
        final File parent_directory = file.getParentFile();

        if (null != parent_directory) {
            parent_directory.mkdirs();
        }
    }
}
