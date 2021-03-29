import java.io.*;
import java.net.*;
import java.util.ArrayList;
import java.util.Arrays;

class Client {
    public static void main(String [] args) {
        try {
            Socket socket = new Socket(InetAddress.getLocalHost(),6400);
            BufferedReader in = new BufferedReader(new InputStreamReader(socket.getInputStream()));
            PrintWriter out = new PrintWriter(socket.getOutputStream(), true);
            String line;

            // storing
            out.println("STORE test1.txt");

            while ((line = in.readLine()) != null) {
                if (line.startsWith("STORE_TO ")) {
                    ArrayList<String> ports = new ArrayList<>(Arrays.asList(line.split(" ")));
                    ports.remove(0);

                    for (String port : ports) {
                        Socket dstoreSocket = new Socket(InetAddress.getLocalHost(), Integer.parseInt(port));
                        BufferedReader clientIn = new BufferedReader(new InputStreamReader(dstoreSocket.getInputStream()));
                        PrintWriter clientOut = new PrintWriter(dstoreSocket.getOutputStream(), true);
                        String clientLine;

                        clientOut.println("STORE test1.txt 6.4mb");

                        while ((clientLine = clientIn.readLine()) != null) {
                            if (clientLine.equals("ACK")) {
                                clientOut.println("Hello world!");
                                System.out.println("Hello world!");
                                break;
                            }
                        }
                        dstoreSocket.close();

                        while ((line = in.readLine()) != null) {
                            if (line.equals("STORE_COMPLETE")) {
                                System.out.println("store complete");
                                break;
                            }
                        }
                    }
                    break;
                }
            }

            out.println("STORE test2.txt");

            while ((line = in.readLine()) != null) {
                if (line.startsWith("STORE_TO ")) {
                    ArrayList<String> ports = new ArrayList<>(Arrays.asList(line.split(" ")));
                    ports.remove(0);

                    for (String port : ports) {
                        Socket dstoreSocket = new Socket(InetAddress.getLocalHost(), Integer.parseInt(port));
                        BufferedReader clientIn = new BufferedReader(new InputStreamReader(dstoreSocket.getInputStream()));
                        PrintWriter clientOut = new PrintWriter(dstoreSocket.getOutputStream(), true);
                        String clientLine;

                        clientOut.println("STORE test2.txt 6.4mb");

                        while ((clientLine = clientIn.readLine()) != null) {
                            if (clientLine.equals("ACK")) {
                                clientOut.println("Hello earth!");
                                System.out.println("Hello earth!");
                                break;
                            }
                        }
                        dstoreSocket.close();

                        while ((line = in.readLine()) != null) {
                            if (line.equals("STORE_COMPLETE")) {
                                System.out.println("store complete");
                                break;
                            }
                        }
                    }
                    break;
                }
            }

            socket.close();
        }
        catch (Exception e) {
            System.out.println("error" + e);
        }
    }
}