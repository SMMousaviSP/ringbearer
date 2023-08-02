package it.unitn.disi.marchioro.mousavi;

import it.unitn.disi.marchioro.mousavi.Node.*;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Random;

public class Solution {
    public static ActorRef getRandomClient(ArrayList<ActorRef> clients) {
        int max = clients.size();
        Random r = new Random();
        return clients.get(r.nextInt(max));
    }

    public static void waitForInput(String message) {
        try {
            Thread.sleep(1000); // wait for 1 second
            System.out.println(message);
            System.in.read();
        } catch (IOException ignored) {
            System.out.println("Input stream error, closing the program ");
            System.exit(1);
        } catch (InterruptedException e) {
            System.out.println("Timeout error, closing the program ");
            System.exit(1);
        }
    }

    public static void main(String[] args) {

        // Create the actor system
        final ActorSystem system = ActorSystem.create("akkasystem");

        SortedCircularDoublyLinkedList<ActorRef> actorList = new SortedCircularDoublyLinkedList<ActorRef>();

        // create M nodes
        int id, key;
        for (int i = 1; i <= Constants.M; i++) {
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

        ArrayList<ActorRef> clients = new ArrayList<>();
        for (int i = 1; i < Constants.CLIENTS_NUMBER + 1; i++) {
            clients.add(system.actorOf((Client.props(i * 100)), "client" + i + "00"));
        }
        waitForInput(
                "Servers are ready to handle requests, press enter to generate 5 random requests to the same coordinator");
        ActorRef headRef = actorList.getFirst().value;
        Random r = new Random();
        for (int i = 0; i < 5; i++) {
            ActorRef client = getRandomClient(clients);
            Request req = new Request(r.nextInt(5) * 10, "value" + r.nextInt(1000),
                    r.nextInt(2) == 1 ? Type.READ : Type.UPDATE, client);
            System.out.println("Sending " + req.getType().name() + " request (" + req.getData().toString() + ") to "
                    + actorList.getFirst().key);
            ClientRequest cr = new ClientRequest(req);
            headRef.tell(cr, client);

            try {
                Thread.sleep(1); // wait for 1 second
            } catch (InterruptedException e) {
                // handle the exception
            }
        }

        waitForInput("Generate 10 random requests to a random coordinator");
        for (int i = 0; i < 10; i++) {
            ActorRef client = getRandomClient(clients);
            Request req = new Request(r.nextInt(5) * 10, "value" + r.nextInt(1000),
                    r.nextInt(2) == 1 ? Type.READ : Type.UPDATE, client);
            ClientRequest cr = new ClientRequest(req);
            Element<ActorRef> server = actorList.getFirst();
            int limit = r.nextInt(actorList.getSize());
            for (int j = 0; j < limit; j++) {
                server = server.next;
            }
            System.out.println(
                    "Sending " + req.getType().name() + " request (" + req.getData().toString() + ") to " + server.key);
            server.value.tell(cr, client);

            try {
                Thread.sleep(500); // wait for 1 second
            } catch (InterruptedException e) {
                // handle the exception
            }
        }

        try {
            Thread.sleep(5000); // wait for 5 second
        } catch (InterruptedException e) {
            // handle the exception
        }
        for (Element<ActorRef> el : actorList) {
            System.out.println("*** Printing storage of node " + el.key);
            el.value.tell(new PrintStorage(), ActorRef.noSender());
            // sleep
            try {
                Thread.sleep(1000); // wait for 1 second
            } catch (InterruptedException e) {
                // handle the exception
            }
        }
        // Joining a new node with key == 45
        waitForInput("Add server with id 45");
        ActorRef actorRef = system.actorOf(Node.props(6, 45), "node" + 6);

        actorList.add(45, actorRef);

        GroupUpdateCoordinator groupUpdateCoordinator = new GroupUpdateCoordinator(45, actorRef, UpdateType.JOIN);
        headRef.tell(groupUpdateCoordinator, ActorRef.noSender());

        try {
            Thread.sleep(5000); // wait for 5 second
        } catch (InterruptedException e) {
            // handle the exception
        }
        for (Element<ActorRef> el : actorList) {
            System.out.println("*** Printing storage of node " + el.key);
            el.value.tell(new PrintStorage(), ActorRef.noSender());
            // sleep
            try {
                Thread.sleep(1000); // wait for 1 second
            } catch (InterruptedException e) {
                // handle the exception
            }
        }

        waitForInput("Generate 10 random requests to a random coordinator");
        for (int i = 0; i < 10; i++) {
            ActorRef client = getRandomClient(clients);
            Request req = new Request(r.nextInt(5) * 10, "value" + r.nextInt(1000),
                    r.nextInt(2) == 1 ? Type.READ : Type.UPDATE, client);
            ClientRequest cr = new ClientRequest(req);
            Element<ActorRef> server = actorList.getFirst();
            int limit = r.nextInt(actorList.getSize());
            for (int j = 0; j < limit; j++) {
                server = server.next;
            }
            System.out.println(
                    "Sending " + req.getType().name() + " request (" + req.getData().toString() + ") to " + server.key);
            server.value.tell(cr, client);

            try {
                Thread.sleep(500); // wait for 1 second
            } catch (InterruptedException e) {
                // handle the exception
            }
        }
        try {
            Thread.sleep(5000); // wait for 5 second
        } catch (InterruptedException e) {
            // handle the exception
        }
        for (Element<ActorRef> el : actorList) {
            System.out.println("*** Printing storage of node " + el.key);
            el.value.tell(new PrintStorage(), ActorRef.noSender());
            // sleep
            try {
                Thread.sleep(1000); // wait for 1 second
            } catch (InterruptedException e) {
                // handle the exception
            }
        }

        // Leave node with key == 45

        waitForInput("Remove server with id 45");
        actorList.remove(45);
        // headRef.tell(cr4, client);

        GroupUpdateCoordinator groupUpdateCoordinator2 = new GroupUpdateCoordinator(45, actorRef, UpdateType.LEAVE);
        headRef.tell(groupUpdateCoordinator2, ActorRef.noSender());

        // sleep
        try {
            Thread.sleep(5000); // wait for 5 second
        } catch (InterruptedException e) {
            // handle the exception
        }
        for (Element<ActorRef> el : actorList) {
            System.out.println("*** Printing storage of node " + el.key);
            el.value.tell(new PrintStorage(), ActorRef.noSender());
            // sleep
            try {
                Thread.sleep(1000); // wait for 1 second
            } catch (InterruptedException e) {
                // handle the exception
            }
        }

        waitForInput("Crash server " + actorList.getFirst().key);
        headRef.tell(new Crash(), headRef);

        waitForInput("Generate 10 random requests (for items saved in 10) to a random coordinator");
        for (int i = 0; i < 10; i++) {
            ActorRef client = getRandomClient(clients);
            Request req = new Request(40 + r.nextInt(3) * 10, "value" + r.nextInt(1000),
                    r.nextInt(2) == 1 ? Type.READ : Type.UPDATE, client);
            ClientRequest cr = new ClientRequest(req);
            Element<ActorRef> server = actorList.getFirst();
            int limit = r.nextInt(actorList.getSize());
            for (int j = 0; j < limit; j++) {
                server = server.next;
            }
            System.out.println(
                    "Sending " + req.getType().name() + " request (" + req.getData().toString() + ") to " + server.key);
            server.value.tell(cr, client);

            try {
                Thread.sleep(500); // wait for 1 second
            } catch (InterruptedException e) {
                // handle the exception
            }
        }
        try {
            Thread.sleep(5000); // wait for 5 second
        } catch (InterruptedException e) {
            // handle the exception
        }
        for (Element<ActorRef> el : actorList) {
            System.out.println("*** Printing storage of node " + el.key);
            el.value.tell(new PrintStorage(), ActorRef.noSender());
            // sleep
            try {
                Thread.sleep(1000); // wait for 1 second
            } catch (InterruptedException e) {
                // handle the exception
            }
        }

        waitForInput("Crash server " + actorList.getFirst().next.key);
        actorList.getFirst().next.value.tell(new Crash(), headRef);

        waitForInput("Generate 10 random requests to a random coordinator");
        for (int i = 0; i < 10; i++) {
            ActorRef client = getRandomClient(clients);
            Request req = new Request(r.nextInt(5) * 10, "value" + r.nextInt(1000),
                    r.nextInt(2) == 1 ? Type.READ : Type.UPDATE, client);
            ClientRequest cr = new ClientRequest(req);
            Element<ActorRef> server = actorList.getFirst();
            int limit = r.nextInt(actorList.getSize());
            for (int j = 0; j < limit; j++) {
                server = server.next;
            }
            System.out.println(
                    "Sending " + req.getType().name() + " request (" + req.getData().toString() + ") to " + server.key);
            server.value.tell(cr, client);

            try {
                Thread.sleep(500); // wait for 1 second
            } catch (InterruptedException e) {
                // handle the exception
            }
        }
        try {
            Thread.sleep(5000); // wait for 5 second
        } catch (InterruptedException e) {
            // handle the exception
        }
        for (Element<ActorRef> el : actorList) {
            System.out.println("*** Printing storage of node " + el.key);
            el.value.tell(new PrintStorage(), ActorRef.noSender());
            // sleep
            try {
                Thread.sleep(1000); // wait for 1 second
            } catch (InterruptedException e) {
                // handle the exception
            }
        }

        waitForInput("Recover server " + actorList.getFirst().next.key);
        actorList.getFirst().next.value.tell(new Recovery(), headRef);
        try {
            Thread.sleep(5000); // wait for 5 second
        } catch (InterruptedException e) {
            // handle the exception
        }
        for (Element<ActorRef> el : actorList) {
            System.out.println("*** Printing storage of node " + el.key);
            el.value.tell(new PrintStorage(), ActorRef.noSender());
            // sleep
            try {
                Thread.sleep(1000); // wait for 1 second
            } catch (InterruptedException e) {
                // handle the exception
            }
        }
        waitForInput("Generate 15 random requests to a random coordinator");
        for (int i = 0; i < 15; i++) {
            ActorRef client = getRandomClient(clients);
            Request req = new Request(r.nextInt(5) * 10, "value" + r.nextInt(1000),
                    r.nextInt(2) == 1 ? Type.READ : Type.UPDATE, client);
            ClientRequest cr = new ClientRequest(req);
            Element<ActorRef> server = actorList.getFirst();
            int limit = r.nextInt(actorList.getSize());
            for (int j = 0; j < limit; j++) {
                server = server.next;
            }
            System.out.println(
                    "Sending " + req.getType().name() + " request (" + req.getData().toString() + ") to " + server.key);
            server.value.tell(cr, client);

            try {
                Thread.sleep(1000); // wait for 1 second
            } catch (InterruptedException e) {
                // handle the exception
            }
        }
        try {
            Thread.sleep(5000); // wait for 5 second
        } catch (InterruptedException e) {
            // handle the exception
        }
        for (Element<ActorRef> el : actorList) {
            System.out.println("*** Printing storage of node " + el.key);
            el.value.tell(new PrintStorage(), ActorRef.noSender());
            // sleep
            try {
                Thread.sleep(1000); // wait for 1 second
            } catch (InterruptedException e) {
                // handle the exception
            }
        }
        waitForInput("Recover server " + actorList.getFirst().key);
        actorList.getFirst().value.tell(new Recovery(), headRef);
        try {
            Thread.sleep(5000); // wait for 5 second
        } catch (InterruptedException e) {
            // handle the exception
        }
        for (Element<ActorRef> el : actorList) {
            System.out.println("*** Printing storage of node " + el.key);
            el.value.tell(new PrintStorage(), ActorRef.noSender());
            // sleep
            try {
                Thread.sleep(1000); // wait for 1 second
            } catch (InterruptedException e) {
                // handle the exception
            }
        }
        // system shutdown
        try {
            System.out.println(">>> Press ENTER to exit <<<");
            System.in.read();
        } catch (IOException ignored) {
        }
        system.terminate();

    }

}
