import java.io.*;
import java.net.*;
import java.util.ArrayList;
import java.util.Arrays;

class Client {
    public static Socket socket;
    public static BufferedReader in;
    public static PrintWriter out;
    public static String line;

    public static void main(String [] args) {
        try {
            socket = new Socket(InetAddress.getLocalHost(),6400);
            in = new BufferedReader(new InputStreamReader(socket.getInputStream()));
            out = new PrintWriter(socket.getOutputStream(), true);

            // storing
            String[] files = {"test1.txt", "test2.txt", "test3.txt", "test4.txt", "test5.txt", "test6.txt", "test7.txt"};
            String[] contents = {"Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"};
            for (int i = 0; i < 6; i++) {
                testStore(files[i], contents[i]);
            }

            // loading
            for (int i = 0; i < 6; i++) {
                testLoad(files[i]);
            }

            // listing
            testList();

            // removing
            for (int i = 0; i < 6; i++) {
                //testRemove(files[i]);
            }
            socket.close();
        }
        catch (Exception e) {
            System.out.println("Error: " + e);
        }
    }

    public static void testStore(String file, String fileContents) throws Exception {
        out.println("STORE " + file);

        while ((line = in.readLine()) != null) {
            if (line.startsWith("STORE_TO ")) {
                ArrayList<String> ports = new ArrayList<>(Arrays.asList(line.split(" ")));
                ports.remove(0);

                for (String port : ports) {
                    Socket dstoreSocket = new Socket(InetAddress.getLocalHost(), Integer.parseInt(port));
                    BufferedReader clientIn = new BufferedReader(new InputStreamReader(dstoreSocket.getInputStream()));
                    PrintWriter clientOut = new PrintWriter(dstoreSocket.getOutputStream(), true);
                    String clientLine;

                    clientOut.println("STORE " + file + " 6.4mb");

                    while ((clientLine = clientIn.readLine()) != null) {
                        if (clientLine.equals("ACK")) {
                            clientOut.println(fileContents);
                            break;
                        }
                    }
                    dstoreSocket.close();

                    while ((line = in.readLine()) != null) {
                        if (line.equals("STORE_COMPLETE")) {
                            System.out.println("store complete (" + file + ")");
                            break;
                        }
                    }
                }
                break;
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