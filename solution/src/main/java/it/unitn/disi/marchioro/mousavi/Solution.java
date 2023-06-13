package it.unitn.disi.marchioro.mousavi;

import it.unitn.disi.marchioro.mousavi.Node.*;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;

import java.io.IOException;
import java.util.ArrayList;

public class Solution {

    public static void main(String[] args) {

        final int M = Constants.M;
        final int N = Constants.N;
        final int R = Constants.R;
        final int W = Constants.W;


        // Create the actor system
        final ActorSystem system = ActorSystem.create("akkasystem");

        SortedCircularDoublyLinkedList<ActorRef> actorList = new SortedCircularDoublyLinkedList<ActorRef>();

        // create M nodes
        int id, key;
        for (int i = 1; i <= M; i++) {
            try {
                Thread.sleep(300); // wait for 1 second
            } catch (InterruptedException e) {
                // handle the exception
            }
            
            id = i;
            key = i * 10;
            ActorRef actorRef = system.actorOf(Node.props(id, key), "node" + id);
            actorList.add(key, actorRef);
            
            // If this is the first node, there is no other node to tell it to join
            if (i == 1) {
                continue;
            }

            GroupUpdateCoordinator groupUpdateCoordinator = new GroupUpdateCoordinator(key, actorRef, UpdateType.JOIN);
            ActorRef headRef = actorList.getFirst().value;
            headRef.tell(groupUpdateCoordinator, ActorRef.noSender());
        }

        try {
            Thread.sleep(300); // wait for 1 second
        } catch (InterruptedException e) {
            // handle the exception
        }

        // LeaveNodeCoordinator leaveNodeCoordinator = new LeaveNodeCoordinator(40);
        ActorRef headRef = actorList.getFirst().value;
        // //headRef.tell(leaveNodeCoordinator, ActorRef.noSender());
        
        ClientRequest cr = new ClientRequest(new Request(35,"test",Type.UPDATE));
        ClientRequest cr2 = new ClientRequest(new Request(803,"test",Type.UPDATE));
        ClientRequest cr3 = new ClientRequest(new Request(35,"test2",Type.UPDATE));
        headRef.tell(cr ,ActorRef.noSender());
        headRef.tell(cr2 ,ActorRef.noSender());
        // sleep
        try {
            Thread.sleep(300); // wait for 1 second
        } catch (InterruptedException e) {
            // handle the exception
        }
        headRef.tell(cr3 ,ActorRef.noSender());

        // Joining a new node with key == 45
        ActorRef actorRef = system.actorOf(Node.props(6, 45), "node" + 6);

        // for (Element<ActorRef> el : actorList) {
        //     System.out.println("*** Printing storage of node " + el.key);
        // }

        actorList.add(45, actorRef);

        GroupUpdateCoordinator groupUpdateCoordinator = new GroupUpdateCoordinator(45, actorRef, UpdateType.JOIN);
        headRef.tell(groupUpdateCoordinator, ActorRef.noSender());

        // sleep
        try {
            Thread.sleep(3000); // wait for 1 second
        } catch (InterruptedException e) {
            // handle the exception
        }


        // Leave node with key == 45

        actorList.remove(45);

        GroupUpdateCoordinator groupUpdateCoordinator2 = new GroupUpdateCoordinator(45, actorRef, UpdateType.LEAVE);
        headRef.tell(groupUpdateCoordinator2, ActorRef.noSender());

        // sleep
        try {
            Thread.sleep(3000); // wait for 1 second
        } catch (InterruptedException e) {
            // handle the exception
        }

        for (Element<ActorRef> el : actorList) {
            System.out.println("*** Printing storage of node " + el.key);
            el.value.tell(new PrintStorage(), ActorRef.noSender());
            // sleep
            try {
                Thread.sleep(3000); // wait for 1 second
            } catch (InterruptedException e) {
                // handle the exception
            }
        }

        // Printing the storage of all nodes
        // for (Element<ActorRef> el : actorList) {
        //     System.out.println("Printing storage of node " + el.key);
        //     el.value.tell(new PrintStorage(), ActorRef.noSender());
        //     // wait
        //     try {
        //         Thread.sleep(3000); // wait for 1 second
        //     } catch (InterruptedException e) {
        //         // handle the exception
        //     }
        // }
        // for (Element<ActorRef> el : actorList) {
        //     System.out.println(el.value);
        // }

        // system shutdown
        try {
            System.out.println(">>> Press ENTER to exit <<<");
            System.in.read();
        }
        catch (IOException ignored) {}
        system.terminate();

    }


}
