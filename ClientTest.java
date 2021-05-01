import java.io.BufferedReader;
import java.io.File;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.InetAddress;
import java.net.Socket;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Arrays;

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
            File downloadFolder = new File("TestFiles");
            File[] downloadFiles = downloadFolder.listFiles();
            if (downloadFiles != null) {
                for (File downloadFile : downloadFiles) {
                    System.out.println("Store " + downloadFile.getName());
                    testStore(downloadFile);
                }
            }

            // loading
            for (int i = 0; i < 6; i++) {
                //testLoad(files[i]);
            }

            // listing
            //testList();

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
        out.println("STORE " + file.getName());

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
                return;
            } else if (line.equals("ERROR_NOT_ENOUGH_DSTORES")) {
                return;
            }
        }
    }

    public static void testLoad(String file) throws Exception {
        out.println("LOAD " + file);

        while ((line = in.readLine()) != null) {
            if (line.startsWith("LOAD_FROM ")) {
                String port = line.split(" ")[1];
                String filesize = line.split(" ")[2];

                Socket dataSocket = new Socket(InetAddress.getLocalHost(), Integer.parseInt(port));
                BufferedReader dataIn = new BufferedReader(new InputStreamReader(dataSocket.getInputStream()));
                PrintWriter dataOut = new PrintWriter(dataSocket.getOutputStream(), true);

                dataOut.println("LOAD_DATA " + file);
                System.out.println(dataIn.readLine());
                dataSocket.close();
                break;
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
        out.println("LIST");
        System.out.println(in.readLine());
    }
}