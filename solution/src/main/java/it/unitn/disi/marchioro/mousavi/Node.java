package it.unitn.disi.marchioro.mousavi;

import it.unitn.disi.marchioro.mousavi.SortedCircularDoublyLinkedList.*;

import akka.actor.AbstractActor;
import akka.actor.Actor;
import akka.actor.ActorRef;
import akka.actor.Props;

import java.io.Console;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;

public class Node extends AbstractActor {
    private int id, key;
    private HashMap<Integer, DataItem> storage;
    private SortedCircularDoublyLinkedList<ActorRef> group; // an array of group members

    public int getId() {
        return id;
    }

    public void setId(int id) {
        this.id = id;
    }

    public int getKey() {
        return key;
    }

    public void setKey(int key) {
        this.key = key;
    }

    // constructors
    public Node(int id, int key) {
        this(id, key, new HashMap<Integer, String>());
    }

    public Node(int id, int key, HashMap<Integer, String> storage) {
        this.id = id;
        this.key = key;
        this.storage = (HashMap<Integer, DataItem>) storage.clone();
    }

    static public Props props(int id, int value) {
        return Props.create(Node.class, () -> new Node(id, value));
    }

    static public class JoinNodeCoordinator {
        public final int nodeKey;
        public final ActorRef nodeRef;

        public JoinNodeCoordinator(int nodeKey, ActorRef nodeRef) {
            this.nodeKey = nodeKey;
            this.nodeRef = nodeRef;
        }
    }

    public static class LeaveNodeCoordinator {
        public final int nodeKey;

        public LeaveNodeCoordinator(int nodeKey) {
            this.nodeKey = nodeKey;
        }
    }

    static public class JoinNode {
        public final int nodeKey;
        public final ActorRef nodeRef;

        public JoinNode(int nodeKey, ActorRef nodeRef) {
            this.nodeKey = nodeKey;
            this.nodeRef = nodeRef;
        }
    }

    static public class LeaveNode {
        public final int nodeKey;

        public LeaveNode(int nodeKey) {
            this.nodeKey = nodeKey;
        }
    }

    @Override
    public Receive createReceive() {
        return receiveBuilder()
                .match(JoinNodeCoordinator.class, this::onJoinNodeCoordinator)
                .match(LeaveNodeCoordinator.class, this::onLeaveNodeCoordinator)
                .match(JoinNode.class, this::onJoinNode)
                .match(LeaveNode.class, this::onLeaveNode)
                .match(DataUpdateMessage.class, this::onDataUpdateMessage)
                .build();
    }

    public Receive crashed() {
        return receiveBuilder()
                .matchAny(msg -> {
                })// if crashed ignore all messages
                  // .matchAny(msg -> System.out.println(getSelf().path().name() + " ignoring " +
                  // msg.getClass().getSimpleName() + " (crashed)"))
                .build();
    }

    private void onJoinNodeCoordinator(JoinNodeCoordinator joinNodeCoordinator) {
        int nodeKey = joinNodeCoordinator.nodeKey;
        ActorRef nodeRef = joinNodeCoordinator.nodeRef;
        JoinNode joinNode = new JoinNode(nodeKey, nodeRef);
        for (Element<ActorRef> otherNode : this.group) {
            if (otherNode.key == this.key)
                continue;
            otherNode.value.tell(joinNode, getSelf());
        }
        this.group.add(nodeKey, nodeRef);
        System.out.println("Node " + this.id + " joined group");
        System.out.println(this.group);
    }

    private void onLeaveNodeCoordinator(LeaveNodeCoordinator leaveNodeCoordinator) {
        int nodeKey = leaveNodeCoordinator.nodeKey;
        LeaveNode leaveNode = new LeaveNode(nodeKey);
        for (Element<ActorRef> otherNode : this.group) {
            if (otherNode.key == this.key)
                continue;
            otherNode.value.tell(leaveNode, getSelf());
        }
        this.group.remove(nodeKey);
        System.out.println("Coordinator, My key" + this.key);
        System.out.println("Node " + this.id + " left group");
        System.out.println(this.group);
        System.out.println();
    }

    private void onJoinNode(JoinNode joinNode) {
        int nodeKey = joinNode.nodeKey;
        ActorRef nodeRef = joinNode.nodeRef;
        this.group.add(nodeKey, nodeRef);
        System.out.println("My key: " + this.key);
        System.out.println("Node " + this.id + " joined group");
        System.out.println(this.group);
        System.out.println();
    }

    private void onLeaveNode(LeaveNode leaveNode) {
        int nodeKey = leaveNode.nodeKey;
        this.group.remove(nodeKey);
        System.out.println("My key: " + this.key);
        System.out.println("Node " + this.id + " left group");
        System.out.println(this.group);
        System.out.println();
    }

    private void propagateUpdate(DataUpdateMessage msg) {
        int c = 0;
    }

    private void onDataUpdateMessage(DataUpdateMessage msg) {
        // TODO: modify method
        if (getSender().path().name().startsWith("node")) {// this means the event has been triggered by another Server
            // if request arrives from another server send back to it the information
            // regarding the data item
        } else {// this means the event has been triggered by a request from a Client
                // if received from a client, ask other servers, collect data and then send back
                // to client (check for timeout)
            if (storage.containsKey(msg.key)) {
                DataItem storedItem = storage.get(msg.key);
                storedItem.setVersion(storedItem.getVersion() + 1);
                storedItem.setValue(msg.value);
            } else {
                storage.put(msg.key, new DataItem(msg.key, msg.value, 0));
            }
            for (DataItem d : storage.values()) {
                System.out.println(this.id + ": " + d.toString());
            }
        }
    }

    // // Message used to communicate group updates: Nodes joining/leaving, also
    // used to define initial system configuration
    // public static class GroupUpdateMessage implements Serializable{
    // public final CircularLinkedList group; // an array of group members
    // public GroupUpdateMessage(CircularLinkedList group) {
    // this.group =group;
    // }
    // }

    public static class DataUpdateMessage implements Serializable {
        public int key;
        public String value;

        public DataUpdateMessage(int key, String value) {
            this.key = key;
            this.value = value;
        }
    }

}
