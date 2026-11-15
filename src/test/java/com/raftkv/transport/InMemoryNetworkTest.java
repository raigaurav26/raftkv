package com.raftkv.transport;

import com.raftkv.core.NodeId;
import com.raftkv.rpc.AppendEntries;
import com.raftkv.rpc.RaftMessage;
import com.raftkv.rpc.RequestVote;
import com.raftkv.rpc.VoteResponse;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.*;

class InMemoryNetworkTest {

    private static final NodeId N1 = NodeId.of("n1");
    private static final NodeId N2 = NodeId.of("n2");
    private static final NodeId N3 = NodeId.of("n3");
    private static final NodeId N4 = NodeId.of("n4");
    private static final NodeId N5 = NodeId.of("n5");

    private InMemoryNetwork net;
    private Map<NodeId, List<String>> inbox;

    @BeforeEach
    void setUp() {
        net = new InMemoryNetwork();
        inbox = new HashMap<>();
        for (NodeId n : List.of(N1, N2, N3, N4, N5)) {
            inbox.put(n, new ArrayList<>());
            net.register(n, (from, msg) -> inbox.get(n).add(from + ":" + msg.term()));
        }
    }

    private static RaftMessage msg(long term) {
        return VoteResponse.grant(term);
    }

    @Test
    void nothingIsDeliveredUntilTheTestSaysSo() {
        net.send(N1, N2, msg(1));
        assertTrue(inbox.get(N2).isEmpty());
        assertEquals(1, net.inFlightCount());
        assertTrue(net.deliverNext());
        assertEquals(List.of("n1:1"), inbox.get(N2));
        assertFalse(net.deliverNext());
    }

    @Test
    void deliversInSendOrder() {
        net.send(N1, N2, msg(1));
        net.send(N3, N2, msg(2));
        net.send(N1, N2, msg(3));
        assertEquals(3, net.deliverAll());
        assertEquals(List.of("n1:1", "n3:2", "n1:3"), inbox.get(N2));
    }

    @Test
    void repliesSentDuringHandlingAreQueuedNotReentrant() {
        InMemoryNetwork net = new InMemoryNetwork();
        List<String> trace = new ArrayList<>();
        net.register(N1, (from, m) -> trace.add("n1 got reply"));
        net.register(N2, (from, m) -> {
            trace.add("n2 start");
            net.send(N2, from, VoteResponse.grant(m.term()));
            trace.add("n2 end");
        });
        net.send(N1, N2, new RequestVote(1, N1, 0, 0));
        assertEquals(2, net.deliverAll());
        assertEquals(List.of("n2 start", "n2 end", "n1 got reply"), trace);
    }

    @Test
    void messagesToUnknownNodesAreDropped() {
        net.unregister(N5);
        net.send(N1, N5, msg(1));
        net.deliverAll();
        assertTrue(inbox.get(N5).isEmpty());
        assertEquals(1, net.droppedCount());
    }

    @Test
    void partitionCutsLinksBetweenGroupsOnly() {
        net.partition(Set.of(N1, N2), Set.of(N3, N4, N5));
        net.send(N1, N2, msg(1));
        net.send(N1, N3, msg(1));
        net.send(N4, N1, msg(1));
        net.send(N3, N5, msg(1));
        net.deliverAll();
        assertEquals(List.of("n1:1"), inbox.get(N2));
        assertEquals(List.of("n3:1"), inbox.get(N5));
        assertTrue(inbox.get(N3).isEmpty());
        assertTrue(inbox.get(N1).isEmpty());
        assertEquals(2, net.droppedCount());
    }

    @Test
    void cutsAreCheckedAtDeliveryTime() {
        net.send(N1, N2, msg(1));
        net.isolate(N2);
        net.deliverAll();
        assertTrue(inbox.get(N2).isEmpty(), "a message in flight when the link breaks is lost");
    }

    @Test
    void isolateThenHeal() {
        net.isolate(N3);
        assertTrue(net.isCut(N3, N1));
        assertTrue(net.isCut(N5, N3));
        assertFalse(net.isCut(N1, N2));
        net.heal();
        net.send(N1, N3, msg(2));
        net.deliverAll();
        assertEquals(List.of("n1:2"), inbox.get(N3));
    }

    @Test
    void nodeLeftOutOfEveryGroupIsCutOff() {
        net.partition(Set.of(N1, N2, N3), Set.of(N4));
        assertTrue(net.isCut(N5, N1));
        assertTrue(net.isCut(N5, N4));
    }

    @Test
    void nodeInTwoGroupsIsRejected() {
        assertThrows(IllegalArgumentException.class, () -> net.partition(Set.of(N1, N2), Set.of(N2, N3)));
    }

    @Test
    void dropRuleTargetsSpecificMessages() {
        net.dropIf(e -> e.to().equals(N2) && e.message() instanceof AppendEntries);
        net.send(N1, N2, AppendEntries.heartbeat(1, N1, 0, 0, 0));
        net.send(N1, N2, msg(1));
        net.deliverAll();
        assertEquals(List.of("n1:1"), inbox.get(N2));
        assertEquals(1, net.droppedCount());
        assertEquals(1, net.deliveredCount());
    }

    @Test
    void dropAllInFlightLosesQueuedMessages() {
        net.send(N1, N2, msg(1));
        net.send(N1, N3, msg(1));
        assertEquals(2, net.dropAllInFlight());
        assertEquals(0, net.deliverAll());
    }

    @Test
    void deliverAllDetectsMessageLoops() {
        InMemoryNetwork net = new InMemoryNetwork();
        net.register(N1, (from, m) -> net.send(N1, from, m));
        net.register(N2, (from, m) -> net.send(N2, from, m));
        net.send(N1, N2, msg(1));
        assertThrows(IllegalStateException.class, () -> net.deliverAll(50));
    }

    @Test
    void oneHandlerPerNode() {
        assertThrows(IllegalStateException.class, () -> net.register(N1, (f, m) -> { }));
    }
}
