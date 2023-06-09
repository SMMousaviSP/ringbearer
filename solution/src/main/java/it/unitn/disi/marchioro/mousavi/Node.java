package it.unitn.disi.marchioro.mousavi;

import akka.actor.AbstractActor;
import akka.actor.ActorRef;
import akka.actor.Props;

import java.io.Serializable;
import java.util.HashMap;

public class Node extends AbstractActor {
    private int id, key;
    private HashMap<Integer, DataItem> storage;
    private SortedCircularDoublyLinkedList<ActorRef> group; // an array of group members
    private HashMap<Integer, Request> requests;

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
        this.group = new SortedCircularDoublyLinkedList<ActorRef>();
        this.group.add(key, getSelf());
        this.requests = new HashMap<>();
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

    // static public class JoinNode {
    // public final int nodeKey;
    // public final ActorRef nodeRef;

    // public JoinNode(int nodeKey, ActorRef nodeRef) {
    // this.nodeKey = nodeKey;
    // this.nodeRef = nodeRef;
    // }
    // }

    static public class LeaveNode {
        public final SortedCircularDoublyLinkedList<ActorRef> group;

        public LeaveNode(SortedCircularDoublyLinkedList<ActorRef> group) {
            this.group = group;
        }
    }

    static public class JoinNode {
        public final SortedCircularDoublyLinkedList<ActorRef> group;

        public JoinNode(SortedCircularDoublyLinkedList<ActorRef> group) {
            this.group = group;
        }
    }

    static public class ClientRequest implements Serializable {
        public final Request request;

        public ClientRequest(Request request) {
            this.request = request;
        }
    }

    static public class LockRequest implements Serializable {
        public final int key;
        public final Type type;

        public LockRequest(int key, Type type) {
            this.key = key;
            this.type = type;
        }
    }

    static public class LockResponse implements Serializable {
        public final DataItem dataItem;
        public final boolean requestState; // true means success

        public LockResponse(DataItem dataItem, boolean requestState) {
            this.dataItem = dataItem;
            this.requestState = requestState;
        }
    }

    static public class CommitRequest implements Serializable {
        public final DataItem dataItem;

        public CommitRequest(DataItem dataItem) {
            this.dataItem = dataItem;
        }
    }

    static public class UnlockRequest implements Serializable {
        public final DataItem dataItem;

        public UnlockRequest(DataItem dataItem) {
            this.dataItem = dataItem;
        }
    }

    @Override
    public Receive createReceive() {
        return receiveBuilder()
                .match(JoinNode.class, this::onJoinNode)
                .match(JoinNodeCoordinator.class, this::onJoinNodeCoordinator)
                .match(LeaveNodeCoordinator.class, this::onLeaveNodeCoordinator)
                .match(LeaveNode.class, this::onLeaveNode)
                .match(ClientRequest.class, this::onClientRequest)
                .match(LockRequest.class, this::onLockRequest)
                .match(LockResponse.class, this::onLockResponse)
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

    private void onJoinNode(JoinNode joinNode) {
        this.group = joinNode.group;
        System.out.println("My key: " + this.key);
        System.out.println("New node joined the group");
        System.out.println(this.group);
        System.out.println();
    }

    private void onJoinNodeCoordinator(JoinNodeCoordinator joinNodeCoordinator) {
        int nodeKey = joinNodeCoordinator.nodeKey;
        // Throw exception if nodeKey is the same as this.key
        if (nodeKey == this.key)
            throw new IllegalArgumentException("The coordinator cannot join itself");
        // Throw exception if nodeKey is already in the group
        if (this.group.getElement(nodeKey) != null)
            throw new IllegalArgumentException("The coordinator cannot join a node that is already in the group");
        ActorRef nodeRef = joinNodeCoordinator.nodeRef;
        // JoinNode joinNode = new JoinNode(nodeKey, nodeRef);
        this.group.add(nodeKey, nodeRef);
        JoinNode joinNode = new JoinNode(this.group);
        for (Element<ActorRef> otherNode : this.group) {
            if (otherNode.key == this.key)
                continue;
            otherNode.value.tell(joinNode, getSelf());
        }
        // for (Element<ActorRef> otherNode = this.group.getElement(this.key).next;
        // otherNode.key != this.key; otherNode = otherNode.next) {
        // otherNode.value.tell(joinNode, getSelf());
        // }

        // nodeRef.tell(newNode, getSelf());
        System.out.println("Coordinator, My key: " + this.key);
        System.out.println("Node " + nodeKey + " joined group");
        System.out.println(this.group);
        System.out.println();
    }

    private void onLeaveNodeCoordinator(LeaveNodeCoordinator leaveNodeCoordinator) {
        int nodeKey = leaveNodeCoordinator.nodeKey;
        // Throw exception if nodeKey is the same as this.key
        if (nodeKey == this.key)
            throw new IllegalArgumentException("The coordinator cannot leave itself");
        // Throw exception if nodeKey is not in the group
        if (this.group.getElement(nodeKey) == null)
            throw new IllegalArgumentException("The coordinator cannot leave a node that is not in the group");
        this.group.remove(nodeKey);
        LeaveNode leaveNode = new LeaveNode(this.group);
        for (Element<ActorRef> otherNode : this.group) {
            if (otherNode.key == this.key)
                continue;
            otherNode.value.tell(leaveNode, getSelf());
        }
        System.out.println("Coordinator, My key: " + this.key);
        System.out.println("Node " + nodeKey + " left group");
        System.out.println(this.group);
        System.out.println();
    }

    // private void onJoinNode(JoinNode joinNode) {
    // int nodeKey = joinNode.nodeKey;
    // ActorRef nodeRef = joinNode.nodeRef;
    // this.group.add(nodeKey, nodeRef);
    // System.out.println("My key: " + this.key);
    // System.out.println("Node " + nodeKey + " joined group");
    // System.out.println(this.group);
    // System.out.println();
    // }

    private void onLeaveNode(LeaveNode leaveNode) {
        this.group = leaveNode.group;
        System.out.println("My key: " + this.key);
        System.out.println("One node left the group");
        System.out.println(this.group);
        System.out.println();
    }

    private void onClientRequest(ClientRequest clientRequest) {

        if (requests.containsKey(clientRequest.request.getKey())) {
            throw new IllegalArgumentException("Coordinator is already handling an operation on the same dataitem");
        }
        requests.put(clientRequest.request.getKey(), clientRequest.request);
        if (clientRequest.request.getType() == Type.READ) {
            HashMap<Integer, Element<ActorRef>> handlers = this.group.getHandlers(clientRequest.request.getKey(),
                    Constants.N);

            System.out.println("client request to server " + getId() + " for data item "
                    + clientRequest.request.getKey() + ", asking ");
            requests.get(clientRequest.request.getKey()).setState(State.PENDING);
            for (Element<ActorRef> el : handlers.values()) {
                System.out.print(el.key + " ");
                el.value.tell(new LockRequest(clientRequest.request.getKey(), clientRequest.request.getType()),
                        getSelf());
            }
            System.out.println("");
        } else if (clientRequest.request.getType() == Type.UPDATE) {
            HashMap<Integer, Element<ActorRef>> handlers = this.group.getHandlers(clientRequest.request.getKey(),
                    Constants.N);

            System.out.println("client request to server " + getId() + " for data item "
                    + clientRequest.request.getKey() + ", asking ");
            requests.get(clientRequest.request.getKey()).setState(State.PENDING);
            for (Element<ActorRef> el : handlers.values()) {
                System.out.print(el.key + " ");
                el.value.tell(new LockRequest(clientRequest.request.getKey(), clientRequest.request.getType()),
                        getSelf());
            }
            System.out.println("");
        } else {
            throw new IllegalArgumentException("Request type not supported");
        }
    }

    private void onLockRequest(LockRequest lockRequest) {
        if (storage.containsKey(lockRequest.key)) {
            if (!storage.get(lockRequest.key).isLock()) {
                storage.get(lockRequest.key).setLock(true);
                getSender().tell(new LockResponse(storage.get(lockRequest.key), true), getSelf());
            } else {
                getSender().tell(new LockResponse(storage.get(lockRequest.key), false), getSelf());
            }
        } else if (lockRequest.type == Type.READ) {
            // this part of code is executed if for some reason we are trying
            // to read a data from a server that doesn't hold it, probably
            // we can simply say we don't provide the lock
            getSender().tell(new LockResponse(new DataItem(lockRequest.key, "", 0, false), false), getSelf());
        } else {
            // this is executed when we are trying to add a new data item to the storage of
            // the server
            getSender().tell(new LockResponse(new DataItem(lockRequest.key, "", 0, false), true), getSelf());
        }
    }

    public void onLockResponse(LockResponse lockResponse) {
        Request r = requests.get(lockResponse.dataItem.getKey());
        r.receivedResponse();
        System.out.println("received response ");
        if (lockResponse.requestState) {
            r.acquiredLock();
        }
        if (r.canCommit() && r.getState() == State.PENDING) {
            r.setState(State.COMMITTING);
            System.out.println("received enough locks to commit " + r.getKey());
            // start committing phase
        }
        if (!r.mayBePerformed()) {
            // abort
        }
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
