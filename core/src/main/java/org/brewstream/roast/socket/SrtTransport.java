package org.brewstream.roast.socket;

import io.netty.channel.EventLoopGroup;
import io.netty.channel.MultiThreadIoEventLoopGroup;
import io.netty.channel.nio.NioIoHandler;
import io.netty.channel.socket.DatagramChannel;
import io.netty.channel.socket.nio.NioDatagramChannel;

import java.util.Objects;

/**
 * The Netty resources a listener or caller runs on: an event loop group and a
 * datagram channel implementation.
 *
 * <p>Roast creates and owns these by default, which suits an application that
 * isn't otherwise using Netty. An application that <em>is</em> should hand over
 * its own, via {@link #shared}:
 *
 * <pre>{@code
 * SrtTransport transport = SrtTransport.shared(existingGroup, EpollDatagramChannel.class);
 * SrtListener.bind(address, SrtConfig.defaults(), transport);
 * }</pre>
 *
 * <p><b>Why this is not on {@link SrtConfig}.</b> An event loop group is a
 * resource with a lifecycle, not a setting — it is shared, it must be shut down,
 * and it is the embedding application's to own. {@code SrtConfig} is a value
 * object describing protocol preferences, safe to log, copy and hold; mixing a
 * thread pool into it would make it neither. Same reasoning that keeps the
 * passphrase on {@link AcceptDecision}.
 *
 * <p><b>Two things this buys an embedder</b>, beyond tidiness. Threads: every
 * caller connection otherwise starts its own group, and Netty starts one thread
 * per group that gets a channel — so fifty outbound pulls means fifty threads
 * that could have been a handful. And transport choice: the default is
 * {@link NioDatagramChannel}, while {@code EpollDatagramChannel} (or io_uring)
 * supports {@code SO_REUSEPORT}, which is how UDP receive scales across cores at
 * live packet rates. Neither was reachable before.
 *
 * <p>A shared transport is never shut down by Roast — {@link SrtListener#close}
 * and {@link SrtConnection#close} close their channels and leave the group
 * alone, since an application that lent it out is still using it. A default
 * transport is shut down with whatever created it.
 */
public final class SrtTransport {

    private final EventLoopGroup eventLoopGroup;
    private final Class<? extends DatagramChannel> channelType;
    private final boolean shutdownWithOwner;

    private SrtTransport(EventLoopGroup eventLoopGroup, Class<? extends DatagramChannel> channelType,
            boolean shutdownWithOwner) {
        this.eventLoopGroup = eventLoopGroup;
        this.channelType = channelType;
        this.shutdownWithOwner = shutdownWithOwner;
    }

    /**
     * Runs on an application's existing Netty resources. Roast will not shut the
     * group down, so its lifecycle stays entirely the caller's.
     *
     * @param eventLoopGroup the group to register channels on
     * @param channelType    the datagram channel implementation — must match the group's
     *                       transport ({@code NioDatagramChannel} with an NIO group,
     *                       {@code EpollDatagramChannel} with an epoll one), since Netty
     *                       fails the registration at runtime if they disagree
     */
    public static SrtTransport shared(EventLoopGroup eventLoopGroup,
            Class<? extends DatagramChannel> channelType) {
        Objects.requireNonNull(eventLoopGroup, "eventLoopGroup");
        Objects.requireNonNull(channelType, "channelType");
        return new SrtTransport(eventLoopGroup, channelType, false);
    }

    /** A fresh NIO group and channel type, owned by whatever it is handed to. */
    static SrtTransport owned() {
        return new SrtTransport(
                new MultiThreadIoEventLoopGroup(NioIoHandler.newFactory()), NioDatagramChannel.class, true);
    }

    EventLoopGroup eventLoopGroup() {
        return eventLoopGroup;
    }

    Class<? extends DatagramChannel> channelType() {
        return channelType;
    }

    /** Whether closing the listener or connection that holds this should also shut the group down. */
    boolean shutdownWithOwner() {
        return shutdownWithOwner;
    }
}
