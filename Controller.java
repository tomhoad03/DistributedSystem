public class Controller {
    public static void main(String[] args) {
        try {
            int cPort = Integer.parseInt(args[0]); // port to listen on
            int rFactor = Integer.parseInt(args[1]); // replication factor
            int timeout = Integer.parseInt(args[2]); // timeout wait time
            int rPeriod = Integer.parseInt(args[3]); // rebalance wait time

            System.out.println("The controller port is: " + cPort);
            System.out.println("The replication factor is: " + rFactor);
            System.out.println("The timeout wait time is: " + timeout);
            System.out.println("The rebalancing period is: " + rPeriod);

        } catch (Exception e) {
            System.out.println("Invalid Arguments!");
        }
    }
}