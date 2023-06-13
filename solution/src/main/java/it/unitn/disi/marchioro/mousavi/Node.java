package it.unitn.disi.marchioro.mousavi;

import akka.actor.AbstractActor;
import akka.actor.ActorRef;
import akka.actor.Props;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

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
        this(id, key, new HashMap<Integer, DataItem>());
    }

    public Node(int id, int key, HashMap<Integer, DataItem> storage) {
        this.id = id;
        this.key = key;
        this.storage = storage;
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
        public final int nodeKey;

        public JoinNode(SortedCircularDoublyLinkedList<ActorRef> group, int nodeKey) {
            this.group = group;
            this.nodeKey = nodeKey;
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
        public final int key;
        public final String value;

        public CommitRequest(int key, String value) {
            this.key = key;
            this.value = value;
        }
    }

    static public class UnlockRequest implements Serializable {
        public final int key;

        public UnlockRequest(int key) {
            this.key = key;
        }
    }

    static public class WriteAfterGroupChange implements Serializable {
        public final int key;
        public final String value;
        public final int version;

        public WriteAfterGroupChange(int key, String value, int version) {
            this.key = key;
            this.value = value;
            this.version = version;
        }
    }

    static public class RemoveAfterGroupChange implements Serializable {
        public final int key;

        public RemoveAfterGroupChange(int key) {
            this.key = key;
        }
    }

    static public class PrintStorage {

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
                .match(UnlockRequest.class, this::onUnlockRequest)
                .match(CommitRequest.class, this::onCommitRequest)
                .match(WriteAfterGroupChange.class, this::onWriteAfterGroupChange)
                .match(RemoveAfterGroupChange.class, this::onRemoveAfterGroupChange)
                .match(PrintStorage.class, this::onPrintStorage)
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
        int nodeKey = joinNode.nodeKey;

        // Check if the new node is the previous node before me
        if (this.group.getPrevElement(this.key).key == nodeKey) {
            for (Map.Entry<Integer, DataItem> entry : storage.entrySet()) {
                int dataKey = entry.getKey();
                // All of the data items that are in the node after the new joined node should be
                // added to the new node, except for the ones that considering their key, they
                // can be stored starting from the node after the new node.
                if (dataKey > nodeKey && dataKey <= this.key)
                    continue; // The next node after the new node is the starting node for this data key
                DataItem dataItem = entry.getValue();
                WriteAfterGroupChange writeAfterGroupChange = new WriteAfterGroupChange(dataKey, dataItem.getValue(),
                        dataItem.getVersion());
                this.group.getElement(nodeKey).value.tell(writeAfterGroupChange, getSender());
                ActorRef nodeRef = this.group.getNextN(dataKey, Constants.N).value;
                nodeRef.tell(new RemoveAfterGroupChange(dataKey), getSender());
            }
        }

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
        JoinNode joinNode = new JoinNode(this.group, nodeKey);
        for (Element<ActorRef> otherNode : this.group) {
            // It should also tell it self so it can handle data distribution if the coordinator is
            // the next or previous node of the new node.
            // if (otherNode.key == this.key)
            //     continue;
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
            // TODO: What if several servers are trying to add the same data item? We are not adding
            // a lock for the new data item and we are did not implement a function to remove the
            // item that was added for the first time if it can not be added to all of the nodes.
            getSender().tell(new LockResponse(new DataItem(lockRequest.key, "", 0, false), true), getSelf());
        }
    }

    public void onLockResponse(LockResponse lockResponse) {
        Request r = requests.get(lockResponse.dataItem.getKey());
        // if r is null it means that the request has already been handled
        if (r == null) {
            return;
        }
        r.receivedResponse();
        System.out.println("received response ");
        if (lockResponse.requestState) {
            r.acquiredLock();
        }
        if (r.canCommit() && r.getState() == State.PENDING) {
            r.setState(State.COMMITTING);
            System.out.println("received enough locks to commit " + r.getKey());
            // Get handlers
            HashMap<Integer, Element<ActorRef>> handlers = this.group.getHandlers(r.getKey(), Constants.N);
            // Send data commit request to handlers
            for (Element<ActorRef> el : handlers.values()) {
                el.value.tell(new CommitRequest(r.getKey(), r.getValue()), getSelf());
            }
            requests.remove(r.getKey());
        }
        if (!r.mayBePerformed()) {
            // Get handlers
            HashMap<Integer, Element<ActorRef>> handlers = this.group.getHandlers(r.getKey(), Constants.N);
            // Send Unlock request to handlers
            for (Element<ActorRef> el : handlers.values()) {
                el.value.tell(new UnlockRequest(r.getKey()), getSelf());
            }
            // remove r from te requests
            requests.remove(r.getKey());
        }
    }

    public void onUnlockRequest(UnlockRequest unlockRequest) {
        // TODO: check if the lock is for the node that is asking to unlock,
        // otherwise we can unlock a data item that we don't own.
        // With the current implementation it is not possible to do this.
        // We also need to store who locked the data item in DataItem class.
        if (storage.containsKey(unlockRequest.key)) {
            storage.get(unlockRequest.key).setLock(false);
        }
    }


    private void onCommitRequest(CommitRequest msg) {
        if (storage.containsKey(msg.key)) {
            DataItem storedItem = storage.get(msg.key);
            storedItem.setVersion(storedItem.getVersion() + 1);
            storedItem.setValue(msg.value);
            // Unlock data item
            storedItem.setLock(false);
        } else {
            storage.put(msg.key, new DataItem(msg.key, msg.value, 0));
        }
        for (DataItem d : storage.values()) {
            System.out.println(this.id + ": " + d.toString());
        }
    }

    private void onWriteAfterGroupChange(WriteAfterGroupChange msg) {
        storage.put(msg.key, new DataItem(msg.key, msg.value, msg.version));
    }

    private void onRemoveAfterGroupChange(RemoveAfterGroupChange msg) {
        storage.remove(msg.key);
    }

    private void onPrintStorage(PrintStorage msg) {
        String s = "My key is " + this.key + "\n";
        for (DataItem d : storage.values()) {
            s += d.toString() + "\n";
        }
        System.out.println(s);
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
