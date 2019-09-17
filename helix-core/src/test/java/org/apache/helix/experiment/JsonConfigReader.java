package org.apache.helix.experiment;

import java.io.BufferedInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Map;
import org.apache.helix.util.TestInputLoader;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.map.ObjectReader;


public class JsonConfigReader {

  public String loadInputs(String filename) throws IOException {
    InputStream inputStream = getClass().getClassLoader().getResourceAsStream(filename);
    BufferedInputStream bis = new BufferedInputStream(inputStream);
    ByteArrayOutputStream buf = new ByteArrayOutputStream();
    int result = bis.read();
    while (result != -1) {
      byte b = (byte) result;
      buf.write(b);
      result = bis.read();
    }
    return buf.toString();
  }

  public void writeOutput() throws IOException, URISyntaxException {
    URL currentTestResourceFolder = getClass().getClassLoader().getResource("experiment");
    System.out.println(currentTestResourceFolder.getPath());
    String fileName = currentTestResourceFolder.getPath() + "/cluster-0.json";
    System.out.println(fileName);
    PrintWriter writer = new PrintWriter(fileName, "UTF-8");
    writer.println("The first line");
    writer.println("The second line");
    writer.close();
  }

  public static void main(String[] args) throws IOException, URISyntaxException {
    JsonConfigReader reader = new JsonConfigReader();
    reader.writeOutput();
  }
}
