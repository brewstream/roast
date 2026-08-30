package org.brewstream.roast.cli;

import org.brewstream.roast.socket.AcceptDecision;
import org.brewstream.roast.socket.SrtConnection;
import org.brewstream.roast.socket.SrtListener;

import java.net.InetSocketAddress;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Manual, hands-on smoke test for the publish/play relay pattern this library
 * describes as BrewStream's actual value proposition — not part of the test
 * suite and not held to this codebase's usual rigor (no design-note javadoc,
 * no tests): a throwaway tool to point real tools at and watch it work.
 *
 * <p>Binds one {@link SrtListener}. Any connection whose StreamID starts with
 * {@code "publish/"} becomes the source; every other connection is a player
 * and receives whatever the source sends, relayed as-is — one received DATA
 * packet becomes one sent DATA packet per player (no chunking/reassembly,
 * matching this codebase's existing "no message mode" scope).
 *
 * <p>Try it: {@code ./gradlew relayDemo} (default port 9000, override with
 * {@code -Pport=<n>}), then in separate terminals:
 * <pre>
 * ffmpeg -re -f lavfi -i testsrc=size=1280x720:rate=30 -f mpegts \
 *     "srt://127.0.0.1:9000?streamid=publish/test"
 *
 * vlc "srt://127.0.0.1:9000?streamid=play/test"
 * </pre>
 */
public final class RelayDemo {

    public static void main(String[] args) throws Exception {
        int port = args.length > 0 ? Integer.parseInt(args[0]) : 9000;

        AtomicReference<SrtConnection> publisher = new AtomicReference<>();
        List<SrtConnection> players = new CopyOnWriteArrayList<>();

        SrtListener listener = SrtListener.bind(new InetSocketAddress("0.0.0.0", port));
        listener.setAcceptHandler(request -> AcceptDecision.accept());
        listener.onConnection(connection -> {
            String streamId = connection.metadata().streamId();
            boolean isPublisher = streamId != null && streamId.startsWith("publish/");

            if (isPublisher) {
                System.out.println("[relay] publisher connected: " + streamId
                        + " from " + connection.metadata().peerAddress());
                publisher.set(connection);
                connection.onData(payload -> {
                    for (SrtConnection player : players) {
                        player.write(payload.retainedDuplicate());
                    }
                    payload.release();
                });
                connection.onClose(() -> {
                    System.out.println("[relay] publisher disconnected: " + streamId);
                    publisher.compareAndSet(connection, null);
                });
            } else {
                System.out.println("[relay] player connected: " + streamId
                        + " from " + connection.metadata().peerAddress());
                players.add(connection);
                connection.onClose(() -> {
                    System.out.println("[relay] player disconnected: " + streamId);
                    players.remove(connection);
                });
            }
        });

        System.out.println("[relay] listening on port " + port + " - Ctrl+C to stop");
        System.out.println("[relay]   publisher: srt://<host>:" + port + "?streamid=publish/<name>");
        System.out.println("[relay]   player:    srt://<host>:" + port + "?streamid=play/<name>");

        new CountDownLatch(1).await(); // block forever; Ctrl+C to exit
    }
}
