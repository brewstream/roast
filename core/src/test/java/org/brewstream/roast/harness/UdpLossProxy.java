package org.brewstream.roast.harness;

import java.io.IOException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Single-client UDP relay that randomly drops packets in both directions, for
 * exercising SRT's ARQ path without OS-level netem/pfctl rules — DESIGN.md's
 * Phase 0 loss-injection harness.
 *
 * <p>Written as a {@code main}-only tool originally, which is why nothing ever
 * used it: a test can't drive a process that binds a fixed port and loops
 * forever. It now also works embedded — {@link #start} binds an ephemeral port,
 * relays on its own daemon thread, and {@link #close} stops it — while
 * {@link #main} keeps the standalone behaviour for manual runs.
 *
 * <p>{@link #droppedPackets()} matters more than it looks: a loss test that
 * happens to drop nothing proves nothing, so assert on it rather than trusting
 * the random draw.
 */
public final class UdpLossProxy implements AutoCloseable {

    private final DatagramSocket socket;
    private final SocketAddress target;
    private volatile double dropRate;
    private final AtomicInteger dropped = new AtomicInteger();
    private final AtomicInteger relayed = new AtomicInteger();
    private volatile boolean running = true;

    private UdpLossProxy(DatagramSocket socket, SocketAddress target, double dropRate) {
        this.socket = socket;
        this.target = target;
        this.dropRate = dropRate;
    }

    /** Binds an ephemeral local port and relays to {@code target}, dropping {@code dropRate} of packets each way. */
    public static UdpLossProxy start(SocketAddress target, double dropRate) throws IOException {
        UdpLossProxy proxy = new UdpLossProxy(new DatagramSocket(0), target, dropRate);
        Thread thread = new Thread(proxy::relayLoop, "udp-loss-proxy");
        thread.setDaemon(true);
        thread.start();
        return proxy;
    }

    /**
     * Changes the drop rate mid-flight. Useful to connect cleanly and only then
     * introduce loss: {@code SrtCaller} does not retry its handshake (see its
     * javadoc), so a dropped INDUCTION or CONCLUSION fails the connect outright
     * — a separate limitation from whether data survives loss.
     */
    public void setDropRate(double dropRate) {
        this.dropRate = dropRate;
    }

    /** The port a client should send to. */
    public int localPort() {
        return socket.getLocalPort();
    }

    public int droppedPackets() {
        return dropped.get();
    }

    public int relayedPackets() {
        return relayed.get();
    }

    @Override
    public void close() {
        running = false;
        socket.close();
    }

    private void relayLoop() {
        byte[] buf = new byte[2048];
        SocketAddress clientAddress = null;

        while (running) {
            try {
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
                    dropped.incrementAndGet();
                    continue;
                }

                socket.send(new DatagramPacket(packet.getData(), packet.getLength(), destination));
                relayed.incrementAndGet();
            } catch (IOException e) {
                if (running) {
                    continue; // a transient send failure shouldn't kill the relay
                }
                return; // closed
            }
        }
    }

    public static void main(String[] args) throws Exception {
        if (args.length != 4) {
            System.err.println("Usage: UdpLossProxy <listenPort> <targetHost> <targetPort> <dropRate>");
            System.exit(1);
        }
        int listenPort = Integer.parseInt(args[0]);
        SocketAddress target = new InetSocketAddress(args[1], Integer.parseInt(args[2]));
        double dropRate = Double.parseDouble(args[3]);

        try (DatagramSocket socket = new DatagramSocket(listenPort)) {
            UdpLossProxy proxy = new UdpLossProxy(socket, target, dropRate);
            proxy.relayLoop();
        }
    }
}
