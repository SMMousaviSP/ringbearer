package it.unitn.disi.marchioro.mousavi;

import akka.actor.ActorRef;

public class CircularLinkedList {
    public ListNode head;
    public void addNode(int key, ActorRef actorRef){
        ListNode ln= new ListNode(key,actorRef);

        ListNode current= head;

        if(head==null){ //if the list is empty just add it and set head equal to item
           ln.next=ln;
           head=ln;
        }
        else if(current.getKey() > ln.getKey()){  //if not empty and key smaller than what's already in, cycle through all list and put new node at head of list
            while(current.next !=head){
                current=current.next;
            }
            current.next=ln;
            ln.next=head;
            head=ln;
        }else{
            while(current.next !=head && current.next.getKey()< ln.getKey()){ // else find the correct spot to insert
                current=current.next;
            }
            ln.next=current.next;
            current.next=ln;
        }
    }
    void printList()
    {
        if (head != null)
        {
            ListNode temp = head;
            do
            {
                System.out.print(temp.getKey() + " ");
                temp = temp.next;
            }  while (temp != head);
            System.out.println("");
        }
    }
    ListNode get(int i) throws Exception{ //get item in position
        //TODO: decide whether we want an Exception to be thrown or simply return null and check it afterwards
        int counter=0;
        if (head != null)
        {
            ListNode temp = head;
            do
            {
                if(counter==i){
                    return temp;
                }
                temp = temp.next;
                counter++;
            }  while (temp != head && counter<=i);
        }
        throw new Exception("Index out of bounds");
    }
    public class ListNode{
        private int key;
        private ActorRef actorRef;
        public ListNode next;

        public ListNode(int key, ActorRef actorRef) {
            this.key = key;
            this.actorRef = actorRef;
        }

        public int getKey() {
            return key;
        }

        public void setKey(int key) {
            this.key = key;
        }

        public ActorRef getActorRef() {
            return actorRef;
        }

        public void setActorRef(ActorRef actorRef) {
            this.actorRef = actorRef;
        }

    }
}
