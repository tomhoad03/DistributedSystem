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
            String[] files = {"test1.txt", "test2.txt", "test3.txt", "test4.txt", "test5.txt", "test6.txt"};
            for (String file : files) {
                testStore(file);
            }

            socket.close();
        }
        catch (Exception e) {
            System.out.println("error" + e);
        }
    }

    public static void testStore(String file) throws Exception {
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
                            clientOut.println("Hello world!");
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
}