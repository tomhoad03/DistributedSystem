import java.io.BufferedReader;
import java.io.File;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.InetAddress;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Base64;
import java.util.Random;

class ClientTest {
    public static Socket socket;
    public static BufferedReader in;
    public static PrintWriter out;
    public static String line;

    public static void main(String[] args) {
        try {
            socket = new Socket(InetAddress.getLocalHost(),6000);
            in = new BufferedReader(new InputStreamReader(socket.getInputStream()));
            out = new PrintWriter(socket.getOutputStream(), true);

            // storing
            File downloadFolder = new File("src/UploadFiles");
            File[] downloadFiles = downloadFolder.listFiles();
            if (downloadFiles != null) {
                for (File downloadFile : downloadFiles) {
                    testStore(downloadFile);
                }
            }

            // loading
            if (downloadFiles != null) {
                for (File downloadFile : downloadFiles) {
                    testLoad(downloadFile, "LOAD ");
                }
            }

            // listing
            testList();

            // removing
            for (int i = 0; i < 6; i++) {
                // testRemove(files[i]); // must test
            }
            socket.close();
        }
        catch (Exception e) {
            System.out.println("Error: " + e);
        }
    }

    public static void testStore(File file) throws Exception {
        System.out.println("STORE " + file.getName() + " " + file.length());
        out.println("STORE " + file.getName() + " " + file.length());

        while ((line = in.readLine()) != null) {
            if (line.startsWith("STORE_TO ")) {
                ArrayList<String> ports = new ArrayList<>(Arrays.asList(line.split(" ")));
                ports.remove(0);

                for (String port : ports) {
                    Socket dstoreSocket = new Socket(InetAddress.getLocalHost(), Integer.parseInt(port));
                    BufferedReader clientIn = new BufferedReader(new InputStreamReader(dstoreSocket.getInputStream()));
                    PrintWriter clientOut = new PrintWriter(dstoreSocket.getOutputStream(), true);
                    String clientLine;

                    clientOut.println("STORE " + file.getName() + " " + file.length());

                    while ((clientLine = clientIn.readLine()) != null) {
                        if (clientLine.equals("ACK")) {
                            dstoreSocket.getOutputStream().write(Files.readAllBytes(file.toPath()));
                            break;
                        }
                    }
                    dstoreSocket.close();
                }
                while ((line = in.readLine()) != null) {
                    if (line.equals("STORE_COMPLETE")) {
                        return;
                    }
                }
            } else if (line.equals("ERROR_FILE_ALREADY_EXISTS")) {
                System.out.println(line);
                return;
            } else if (line.equals("ERROR_NOT_ENOUGH_DSTORES")) {
                System.out.println(line);
                return;
            }
        }
    }

    public static void testLoad(File file, String load) throws Exception {
        System.out.println(load + file.getName());
        out.println(load + file.getName());

        while ((line = in.readLine()) != null) {
            if (line.startsWith("LOAD_FROM ")) {
                try {
                    String port = line.split(" ")[1];
                    String fileSize = line.split(" ")[2];

                    Socket dataSocket = new Socket(InetAddress.getLocalHost(), Integer.parseInt(port));
                    PrintWriter dataOut = new PrintWriter(dataSocket.getOutputStream(), true);

                    dataOut.println("LOAD_DATA " + file.getName());
                    byte[] contents = dataSocket.getInputStream().readNBytes(Integer.parseInt(fileSize));

                    if ("ERROR_FILE_DOES_NOT_EXIST".contains(new String(contents, StandardCharsets.UTF_8))) {
                        testLoad(file, "RELOAD ");
                        break;
                    }
                    Random rand = new Random();
                    File newFile = new File("src/DownloadFiles/test" + (rand.nextInt(999) + 6) + ".txt");
                    Files.write(newFile.toPath(), Arrays.copyOfRange(contents, 0, Integer.parseInt(fileSize)));
                    dataSocket.close();
                    break;
                } catch (Exception e) {
                    System.out.println("Log: Malformed load message from the client");
                    break;
                }
            } else if (line.equals("ERROR_FILE_DOES_NOT_EXIST")) {
                System.out.println(line);
                return;
            } else if (line.equals("ERROR_LOAD")) {
                System.out.println(line);
                return;
            }
        }
    }

    public static void testRemove(String file) throws Exception {
        out.println("REMOVE " + file);

        while ((line = in.readLine()) != null) {
            if (line.startsWith("REMOVE_COMPLETE")) {
                System.out.println("remove complete (" + file + ")");
                break;
            }
        }
    }

    public static void testList() throws Exception {
        System.out.println("LIST");
        out.println("LIST");

        System.out.println(in.readLine());
    }
}