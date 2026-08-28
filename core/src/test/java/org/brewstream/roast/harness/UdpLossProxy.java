package org.brewstream.roast.harness;

import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.concurrent.ThreadLocalRandom;

/**
 * Single-client UDP relay that randomly drops packets in both directions, for
 * exercising SRT's ARQ path in interop tests without OS-level netem/pfctl rules.
 */
public final class UdpLossProxy {

    public static void main(String[] args) throws Exception {
        if (args.length != 4) {
            System.err.println("Usage: UdpLossProxy <listenPort> <targetHost> <targetPort> <dropRate>");
            System.exit(1);
        }
        int listenPort = Integer.parseInt(args[0]);
        SocketAddress target = new InetSocketAddress(args[1], Integer.parseInt(args[2]));
        double dropRate = Double.parseDouble(args[3]);

        try (DatagramSocket socket = new DatagramSocket(listenPort)) {
            byte[] buf = new byte[2048];
            SocketAddress clientAddress = null;

            while (true) {
                DatagramPacket packet = new DatagramPacket(buf, buf.length);
                socket.receive(packet);
                SocketAddress from = packet.getSocketAddress();

                SocketAddress destination;
                if (from.equals(target)) {
                    if (clientAddress == null) {
                        continue;
                    }
                    destination = clientAddress;
                } else {
                    clientAddress = from;
                    destination = target;
                }

                if (ThreadLocalRandom.current().nextDouble() < dropRate) {
                    continue;
                }

                socket.send(new DatagramPacket(packet.getData(), packet.getLength(), destination));
            }
        }
    }
}
