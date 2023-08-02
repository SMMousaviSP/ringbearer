package it.unitn.disi.marchioro.mousavi;

import akka.actor.AbstractActor;
import akka.actor.ActorRef;
import akka.actor.Props;
import scala.collection.immutable.Stream;
import scala.concurrent.duration.Duration;

import javax.xml.crypto.Data;
import java.io.Serializable;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

enum UpdateType {
    JOIN, LEAVE
}

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

    static public class GroupUpdateCoordinator {
        public final int nodeKey;
        public final ActorRef nodeRef;
        public final UpdateType updateType;

        public GroupUpdateCoordinator(int nodeKey, ActorRef nodeRef, UpdateType updateType) {
            this.nodeKey = nodeKey;
            this.nodeRef = nodeRef;
            this.updateType = updateType;
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

    static public class GroupUpdate {
        public final SortedCircularDoublyLinkedList<ActorRef> group;
        public final int nodeKey;
        public final ActorRef nodeRef;
        public final UpdateType updateType;

        public GroupUpdate(SortedCircularDoublyLinkedList<ActorRef> group, int nodeKey, ActorRef nodeRef, UpdateType updateType) {
            this.group = group.clone();
            this.nodeKey = nodeKey;
            this.nodeRef = nodeRef;
            this.updateType = updateType;

            if (this.updateType == UpdateType.JOIN) {
                this.group.add(this.nodeKey, this.nodeRef);
            } else if (this.updateType == UpdateType.LEAVE) {
                this.group.remove(this.nodeKey);
            }
        }
    }
    public static class Recovery implements Serializable {}

    static public class ClientRequest implements Serializable {
        public final Request request;

        public ClientRequest(Request request) {
            this.request = request;
        }
    }

    static public class ReadRequest implements Serializable {
        public final int key;
        public final int requesterID;

        public ReadRequest(int key,int requesterID) {
            this.key = key;
            this.requesterID=requesterID;
        }
    }

    static public class ReadResponse implements Serializable {
        public final DataItem dataItem;
        public final boolean requestState; // true means success

        public ReadResponse(DataItem dataItem, boolean requestState) {
            this.dataItem = dataItem;
            this.requestState = requestState;
        }
    }
    static public class LockRequest implements Serializable {
        public final int key;
        public final int requesterID;

        public LockRequest(int key,int requesterID) {
            this.key = key;
            this.requesterID=requesterID;
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
        public int version;
        public final Type type;

        public CommitRequest(int key, String value,int version, Type type) {
            this.key = key;
            this.value = value;
            this.version=version;
            this.type=type;
        }

    }

    static public class CommitResponse implements Serializable{
        public final DataItem dataItem;
        public final boolean requestState; // true means success

        public CommitResponse(DataItem dataItem, boolean requestState) {
            this.dataItem = dataItem;
            this.requestState = requestState;
        }
    }

    static public class StorageStateRequest implements Serializable {
        public final int key;
        public final int id;

        public StorageStateRequest(int key,int id) {
            this.key = key;
            this.id = id;
        }
    }

    static public class StorageStateResponse implements Serializable {
        public final int key;
        public final HashMap<Integer,DataItem> updates;

        public StorageStateResponse(int key,HashMap<Integer,DataItem> updates) {
            this.key = key;
            this.updates = (HashMap<Integer,DataItem>) updates.clone();
        }
    }
    static public class AbortOperation implements Serializable {
        public final int key;

        public AbortOperation(int key) {
            this.key = key;
        }
    }
    static public class UnlockRequest implements Serializable {
        public final int key;
        public final int requesterID;

        public UnlockRequest(int key,int requesterID) {
            this.key = key;
            this.requesterID=requesterID;
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

    static public class PrintStorage implements Serializable {}
    static public class Crash implements Serializable {}

    @Override
    public Receive createReceive() {
        return receiveBuilder()
                .match(GroupUpdate.class, this::onGroupUpdate)
                .match(GroupUpdateCoordinator.class, this::onGroupUpdateCoordinator)
                .match(ClientRequest.class, this::onClientRequest)
                .match(ReadRequest.class, this::onReadRequest)
                .match(ReadResponse.class, this::onReadResponse)
                .match(LockRequest.class, this::onLockRequest)
                .match(LockResponse.class, this::onLockResponse)
                .match(UnlockRequest.class, this::onUnlockRequest)
                .match(CommitRequest.class, this::onCommitRequest)
                .match(CommitResponse.class, this::onCommitResponse)
                .match(WriteAfterGroupChange.class, this::onWriteAfterGroupChange)
                .match(RemoveAfterGroupChange.class, this::onRemoveAfterGroupChange)
                .match(PrintStorage.class, this::onPrintStorage)
                .match(AbortOperation.class, this::onAbortOperation)
                .match(Crash.class,this::onCrash)
                .match(StorageStateRequest.class,this::onStorageStateRequest)
                .match(StorageStateResponse.class,this::onStorageStateResponse)
                .build();
    }

    private void onCrash(Crash crash) {
        getContext().become(crashed());
    }


    // if crashed ignore all messages
    public Receive crashed() {
        return receiveBuilder()
                .match(Recovery.class,this::onRecovery)
                .match(ClientRequest.class, this::onRequestToCrashed)
                .match(PrintStorage.class, this::onPrintStorage)
                .matchAny(msg -> {
                })
                .build();
    }

    //if the coordinator contacted by the client is crashed, send back error after some time, simulating a timeout in the connection
    private void onRequestToCrashed(ClientRequest request) {
        getContext().system().scheduler().scheduleOnce(
                Duration.create(2000, TimeUnit.MILLISECONDS),
                getSender(),
                new DataItem(request.request.getData().getKey(),"",-1), // message sent to myself
                getContext().system().dispatcher(), getSelf()
        );
    }


    private void onGroupUpdate(GroupUpdate groupUpdate) {
        for (Map.Entry<Integer, DataItem> entry : storage.entrySet()) {
            int dataKey = entry.getKey();
            // Only check the data items that were written first in this node.
            // To avoid checking the same data item more than once in different nodes.
            if (this.group.getFirstLargerEqualKey(dataKey).key == this.key) {
                DataItem dataItem = entry.getValue();
                HashMap<Integer, Element<ActorRef>> prevHandlers = this.group.getHandlers(dataKey, Constants.N);
                HashMap<Integer, Element<ActorRef>> newHandlers = groupUpdate.group.getHandlers(dataKey, Constants.N);

                // prevHandlers - newHandlers are all the handlers than should no longer have
                // the data
                HashMap<Integer, Element<ActorRef>> toRemove = new HashMap<>(prevHandlers);
                toRemove.keySet().removeAll(newHandlers.keySet());
                for (Element<ActorRef> handler : toRemove.values()) {
                    handler.value.tell(new RemoveAfterGroupChange(dataKey), getSelf());
                }

                // newHandlers - prevHandlers are all the handlers that should now have the data
                HashMap<Integer, Element<ActorRef>> toAdd = new HashMap<>(newHandlers);
                toAdd.keySet().removeAll(prevHandlers.keySet());
                for (Element<ActorRef> handler : toAdd.values()) {
                    handler.value.tell(new WriteAfterGroupChange(dataKey, dataItem.getValue(), dataItem.getVersion()),
                            getSelf());
                }
            }
        }

        this.group = groupUpdate.group;

        if(Constants.DEBUGGING){
            System.out.println("Group Update - My key: " + this.key);
            System.out.println(this.group);
            System.out.println();
        }
    }

    private void onGroupUpdateCoordinator(GroupUpdateCoordinator groupUpdateCoordinator) {
        int nodeKey = groupUpdateCoordinator.nodeKey;
        ActorRef nodeRef = groupUpdateCoordinator.nodeRef;
        UpdateType updateType = groupUpdateCoordinator.updateType;

        if (updateType == UpdateType.JOIN) {
            // Throw exception if nodeKey is the same as this.key
            if (nodeKey == this.key)
                throw new IllegalArgumentException("The coordinator cannot join itself");
            // Throw exception if nodeKey is already in the group
            if (this.group.getElement(nodeKey) != null)
                throw new IllegalArgumentException("The coordinator cannot join a node that is already in the group");
        } else if (updateType == UpdateType.LEAVE) {
            if (nodeKey == this.key)
                throw new IllegalArgumentException("The coordinator cannot leave itself");
            // Throw exception if nodeKey is not in the group
            if (this.group.getElement(nodeKey) == null)
                throw new IllegalArgumentException("The coordinator cannot leave a node that is not in the group");
        }

        if (updateType == UpdateType.JOIN) {
            GroupUpdate groupUpdate = new GroupUpdate(this.group, nodeKey, nodeRef, updateType);
            nodeRef.tell(groupUpdate, getSelf());
        }

        for (Element<ActorRef> otherNode : this.group) {
            // groupUpdate should be initialized inside the loop to avoid sending the same
            // object to all nodes.
            GroupUpdate groupUpdate = new GroupUpdate(this.group, nodeKey, nodeRef, updateType);
            otherNode.value.tell(groupUpdate, getSelf());
        }

        if(Constants.DEBUGGING) {
            System.out.println("Coordinator - Group Update, My key: " + this.key);
            System.out.println(this.group);
            System.out.println();
        }
    }

    private void onClientRequest(ClientRequest clientRequest) {

        if (clientRequest.request.getType()==Type.UPDATE && requests.containsKey(clientRequest.request.getData().getKey())) {
            getSender().tell(new DataItem(clientRequest.request.getData().getKey(),"",-1),getSelf());
            //throw new IllegalArgumentException("Coordinator is already handling an operation on the same dataitem");
        }
        requests.put(clientRequest.request.getData().getKey(), clientRequest.request);
        if (clientRequest.request.getType() == Type.READ) {
            HashMap<Integer, Element<ActorRef>> handlers = this.group.getHandlers(clientRequest.request.getData().getKey(),
                    Constants.N);

            if(Constants.DEBUGGING) {
                System.out.println("client request to server " + getId() + " for data item "
                        + clientRequest.request.getData().getKey() + ", asking ");
            }
            requests.get(clientRequest.request.getData().getKey()).setState(State.PENDING);
            for (Element<ActorRef> el : handlers.values()) {

                if(Constants.DEBUGGING) {
                    System.out.print(el.key + " ");
                }
                el.value.tell(new ReadRequest(clientRequest.request.getData().getKey(),getId()),getSelf());
                // don't request locks when reading, handle 3 cases:
                // highest version with lock: abort (an update is being performed)
                // else if highest version without lock: deliver highest version
                //el.value.tell(new LockRequest(clientRequest.request.getKey(), clientRequest.request.getType(),getId()),getSelf());
            }

            if(Constants.DEBUGGING) {
                System.out.println("");
            }
        } else if (clientRequest.request.getType() == Type.UPDATE) {
            HashMap<Integer, Element<ActorRef>> handlers = this.group.getHandlers(clientRequest.request.getData().getKey(),
                    Constants.N);

            if(Constants.DEBUGGING) {
                System.out.println("client request to server " + getId() + " for data item "
                        + clientRequest.request.getData().getKey() + ", asking ");
            }
            requests.get(clientRequest.request.getData().getKey()).setState(State.PENDING);
            for (Element<ActorRef> el : handlers.values()) {

                if(Constants.DEBUGGING) {
                    System.out.print(el.key + " ");
                }
                el.value.tell(new LockRequest(clientRequest.request.getData().getKey(),getId()),getSelf());
                getContext().system().scheduler().scheduleOnce(
                        Duration.create(4000, TimeUnit.MILLISECONDS),
                        getSelf(),
                        new AbortOperation(clientRequest.request.getData().getKey()),
                        getContext().system().dispatcher(), getSelf()
                );
            }

            if(Constants.DEBUGGING) {
                System.out.println("");
            }
        } else {
            throw new IllegalArgumentException("Request type not supported");
        }
    }

    private void onAbortOperation(AbortOperation abortOperation) {
        //check if the operation is still in the queue, otherwise it has been already dealt with
        if(requests.containsKey(abortOperation.key)){
            //immediately communicate error to client
            requests.get(abortOperation.key).getClient().tell(new DataItem(abortOperation.key, "", -1, -1),getSelf());
            //remove request from the list
            requests.remove(abortOperation.key);
            //release acquired locks
            HashMap<Integer, Element<ActorRef>> handlers = this.group.getHandlers(abortOperation.key,
                    Constants.N);
            for (Element<ActorRef> el : handlers.values()) {
                if(Constants.DEBUGGING){
                    System.out.print(el.key + " ");
                }
                el.value.tell(new UnlockRequest(abortOperation.key,getId()),getSelf());
            }
        }

    }

    private void onStorageStateRequest(StorageStateRequest storageStateRequest){
        HashMap<Integer,DataItem> updates= new HashMap<>();
        for (DataItem d: storage.values()) {
            if(d.getVersion()!=-1 && group.getHandlers(d.getKey(),Constants.N).containsKey(storageStateRequest.key)){
                updates.put(d.getKey(),d);
            }
        }
        getSender().tell(new StorageStateResponse(getId(),updates),getSelf());
    }

    private void onStorageStateResponse(StorageStateResponse storageStateResponse){
        Collection<DataItem> items= storageStateResponse.updates.values();
        for (DataItem d: items){
            if(!storage.containsKey(d.getKey()) ||
             d.getVersion()>storage.get(d.getKey()).getVersion()){
                storage.put(d.getKey(),new DataItem(d.getKey(),d.getValue(),d.getVersion()));
            }
        }
    }


    private void onRecovery(Recovery recovery) {
        Element<ActorRef> groupElement= group.getElement(getKey());
        // start asking for updates
        for (int i=0;i<Constants.R;i++){
            Element<ActorRef> server= groupElement.next;
            server.value.tell(new StorageStateRequest(getKey(),getId()),getSelf());
        }
        for (int i=0;i<Constants.R;i++){
            Element<ActorRef> server= groupElement.prev;
            server.value.tell(new StorageStateRequest(getKey(),getId()),getSelf());
        }

        getContext().become(createReceive());
    }

    private void onLockRequest(LockRequest lockRequest) {
        if (storage.containsKey(lockRequest.key)) {
            if (!storage.get(lockRequest.key).isLock()) {
                storage.get(lockRequest.key).setLock(lockRequest.requesterID);
                getSender().tell(new LockResponse(storage.get(lockRequest.key), true), getSelf());
            } else {
                getSender().tell(new LockResponse(storage.get(lockRequest.key), false), getSelf());
            }
        }  else {
            // this is executed when we are trying to add a new data item to the storage of
            // the server

            // done: What if several servers are trying to add the same data item? We are
            // not adding a lock for the new data item and we did not implement a
            // function to remove the item that was added for the first time if it can not
            // be added to all of the nodes.

            DataItem suppDataItem= new DataItem(lockRequest.key, "", -1, lockRequest.requesterID);
            storage.put(lockRequest.key, suppDataItem);
            getSender().tell(new LockResponse(suppDataItem, true), getSelf());
        }
    }

    public void onLockResponse(LockResponse lockResponse) {
        Request r = requests.get(lockResponse.dataItem.getKey());
        // if r is null it means that the request has already been handled
        if (r == null) {
            return;
        }
        r.receivedResponse();

        if(Constants.DEBUGGING) {
            System.out.println("received response ");
        }
        if (lockResponse.requestState) {
            r.acquiredLock();
        }
        if(r.getState() == State.PENDING){
            //get highest version
            if(requests.get(lockResponse.dataItem.getKey()).getData().getVersion()<lockResponse.dataItem.getVersion()){
                requests.get(lockResponse.dataItem.getKey()).getData().setVersion(lockResponse.dataItem.getVersion());
            }
            if (r.canCommit() ) {
                r.setState(State.COMMITTING);

                if(Constants.DEBUGGING) {
                    System.out.println("received enough locks to commit " + r.getData().getKey());
                }
                // Get handlers
                HashMap<Integer, Element<ActorRef>> handlers = this.group.getHandlers(r.getData().getKey(), Constants.N);
                // Send data commit request to handlers
                CommitRequest commitRequest= new CommitRequest(r.getData().getKey(),r.getData().getValue(),requests.get(lockResponse.dataItem.getKey()).getData().getVersion(),r.getType());
                if(r.getType()==Type.UPDATE){
                    commitRequest.version++;
                    requests.get(r.getData().getKey()).getData().setVersion(commitRequest.version);
                }
                for (Element<ActorRef> el : handlers.values()) {
                    el.value.tell(commitRequest, getSelf());
                }

                if(requests.containsKey(lockResponse.dataItem.getKey())){
                    requests.get(lockResponse.dataItem.getKey()).getClient().tell(requests.get(lockResponse.dataItem.getKey()).getData(),getSelf());
                    requests.remove(lockResponse.dataItem.getKey());
                }
            }
        }

        if (!r.mayBePerformed() && r.getState() == State.PENDING) {
            // Get handlers
            HashMap<Integer, Element<ActorRef>> handlers = this.group.getHandlers(r.getData().getKey(), Constants.N);
            // Send Unlock request to handlers
            for (Element<ActorRef> el : handlers.values()) {
                el.value.tell(new UnlockRequest(r.getData().getKey(),getId()), getSelf());
            }
            r.getClient().tell(new DataItem(r.getData().getKey(), "", -1, -1),getSelf());
            // remove r from te requests
            requests.remove(r.getData().getKey());

        }
    }
    private void onReadRequest(ReadRequest readRequest) {
        if (storage.containsKey(readRequest.key)) {
            getSender().tell(new ReadResponse(storage.get(readRequest.key), true), getSelf());
        }  else {
            // this is executed when we are trying to read a data item not present in the storage
            DataItem suppDataItem= new DataItem(readRequest.key, "", -1, -1);
            getSender().tell(new ReadResponse(suppDataItem, false), getSelf());
        }
    }

    public void onReadResponse(ReadResponse readResponse) {
        Request r = requests.get(readResponse.dataItem.getKey());
        // if r is null it means that the request has already been handled
        if (r == null) {
            return;
        }
        r.receivedResponse();

        if(Constants.DEBUGGING) {
            System.out.println("received response ");
        }
        if (readResponse.requestState) {
            r.acquiredLock();
            if(r.getData().getVersion()<readResponse.dataItem.getVersion() || (r.getData().getVersion()==readResponse.dataItem.getVersion() && readResponse.dataItem.isLock())){
                r.setData(readResponse.dataItem);
            }
        }
        if (r.canCommit() && r.getState() == State.PENDING) {
            r.setState(State.COMMITTING);
            if(r.getData().isLock()){
                //abort
                r.getClient().tell(new DataItem(r.getData().getKey(),"",-1),getSelf());
            }else{
                //success
                r.getClient().tell(r.getData(),getSelf());
            }
            requests.remove(r.getData().getKey());
        }
        if (!r.mayBePerformed() && r.getState() == State.PENDING) {
            //abort
            r.setState(State.ENDING);
            r.getClient().tell(new DataItem(r.getData().getKey(),"",-1),getSelf());
        }
    }
    public void onUnlockRequest(UnlockRequest unlockRequest) {
        // check if the lock is for the node that is asking to unlock,
        // otherwise we can unlock a data item that we don't own.

        //we only create UnlockRequests when we abort an update operation
        if (storage.containsKey(unlockRequest.key) && storage.get(unlockRequest.key).getLocker()==unlockRequest.requesterID) {
            //if version is -1 we simply delete the object from the storage since there has never been a successful update to the object
            if(storage.get(unlockRequest.key).getVersion()==-1){
                storage.remove(unlockRequest.key);
            }else{
                storage.get(unlockRequest.key).setLock(-1);
            }
        }
    }

    private void onCommitRequest(CommitRequest msg) {
        //this shouldn't be necessary. Only update requests can reach this branch
        if(msg.type==Type.UPDATE){
            //the storage contains key should always be true, a commit can be requested only after
            //a lockRequest that would have added the dataitem if not present
            if (storage.containsKey(msg.key) && msg.version>=storage.get(msg.key).getVersion()) {
                DataItem storedItem = storage.get(msg.key);
                storedItem.setVersion(msg.version);
                storedItem.setValue(msg.value);
                //getSender().tell(new CommitResponse(storedItem,true),getSelf());
                // Unlock data item
                storedItem.setLock(-1);
            }

        }
        if(Constants.DEBUGGING) {
            for (DataItem d : storage.values()) {
                System.out.println(this.id + ": " + d.toString());
            }
        }
    }
    private void onCommitResponse(CommitResponse commitResponse) {
        if(Constants.DEBUGGING)
            System.out.println("Request for data item "+commitResponse.dataItem.getKey()+ " completed "+ commitResponse.dataItem.toString());
        //if the key is not present the response's already being delivered to the client
        if(requests.containsKey(commitResponse.dataItem.getKey())){
            requests.get(commitResponse.dataItem.getKey()).getClient().tell(requests.get(commitResponse.dataItem.getKey()).getData(),getSelf());
            requests.remove(commitResponse.dataItem.getKey());
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

    public static class DataUpdateMessage implements Serializable {
        public int key;
        public String value;

        public DataUpdateMessage(int key, String value) {
            this.key = key;
            this.value = value;
        }
    }

}
