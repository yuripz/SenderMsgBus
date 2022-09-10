package ru.hermes.msgbus.ws.common;

import java.io.File;
import java.io.IOException;

/**
 * This class was taken from Axis2 code. It's a Wsdl11Writer dependency.
 *
 * @author Tom Bujok
 * @since 1.0.0
 */
class FileWriter {

    /**
     * Creates/ returns a file object
     *
     * @param rootLocation - Location to be written
     * @param packageName  - package, can be '.' separated
     * @param fileName     name of the file
     * @param extension    type of the file, java, cpp etc
     * @return the File that was created
     * @throws java.io.IOException
     * @throws Exception
     */
    public File createClassFile(File rootLocation, String packageName, String fileName, String extension) throws IOException,
            Exception {
        File returnFile = null;
        File root = rootLocation;

        if (packageName != null) {
            String directoryNames[] = packageName.split("\\.");
            File tempFile = null;
            int length = directoryNames.length;
            for (int i = 0; i < length; i++) {
                tempFile = new File(root, directoryNames[i]);
                root = tempFile;
                if (!tempFile.exists()) {
                    tempFile.mkdir();
                }
            }
        }

        if ((extension != null) && !fileName.endsWith(extension)) {
            fileName = fileName + extension;
        }

        returnFile = new File(root, fileName);
        if (!returnFile.exists()) {
            // returnFile.createNewFile();
        }
        return returnFile;
    }


}
