package it.unitn.disi.marchioro.mousavi;

import java.util.*;

class Element<T> {
    int key;
    T value;
    Element<T> next;
    Element<T> prev;

    public Element(int key, T value) {
        this.key = key;
        this.value = value;
    }
}

public class SortedCircularDoublyLinkedList<T> implements Iterable<Element<T>> {
    private Element<T> head;

    public SortedCircularDoublyLinkedList() {
        this.head = null;
    }

    public void add(int key, T value) {
        Element<T> newNode = new Element<T>(key, value);
        if (head == null) {
            newNode.next = newNode;
            newNode.prev = newNode;
            head = newNode;
        } else {
            Element<T> current = head, previous = null;
            while (current.next != head && current.key < key) {
                previous = current;
                current = current.next;
            }
            if (previous == null) {
                // Insert at the beginning
                Element<T> last = head.prev;
                newNode.next = head;
                newNode.prev = last;
                head.prev = newNode;
                last.next = newNode;
                if (head.key > newNode.key) {
                    head = newNode;
                }
            } else if (current.next == head && current.key < key) {
                // Insert at the end
                newNode.next = head;
                newNode.prev = current;
                current.next = newNode;
                head.prev = newNode;
            } else {
                // Insert in the middle
                newNode.next = previous.next;
                newNode.prev = previous;
                previous.next = newNode;
                newNode.next.prev = newNode;
            }
        }
    }

    public void remove(int key) {
        if (head == null)
            return;
        Element<T> current = head;
        while (current.next != head && current.key != key) {
            current = current.next;
        }
        if (current.key == key) {
            if (current.next == current) { // Only one node in the list
                head = null;
            } else {
                current.next.prev = current.prev;
                current.prev.next = current.next;
                if (head == current) {
                    head = head.next;
                }
            }
        }
    }

    public Element<T> getNextElement(int key) {
        if (head == null) {
            return null;
        }
        Element<T> current = head;
        do {
            if (current.key == key) {
                return current.next;
            }
            current = current.next;
        } while (current != head);
        return null;
    }

    public Element<T> getPrevElement(int key) {
        if (head == null) {
            return null;
        }
        Element<T> current = head;
        do {
            if (current.key == key) {
                return current.prev;
            }
            current = current.next;
        } while (current != head);
        return null;
    }

    public Element<T> getElement(int key) {
        if (head == null) {
            return null;
        }
        Element<T> current = head;
        do {
            if (current.key == key) {
                return current;
            }
            current = current.next;
        } while (current != head);
        return null;
    }

    public Element<T> getFirst() {
        return head;
    }

    public List<Integer> getSortedKeys() {
        List<Integer> sortedKeys = new ArrayList<>();
        if (head == null)
            return sortedKeys;
        Element<T> current = head;
        do {
            sortedKeys.add(current.key);
            current = current.next;
        } while (current != head);
        Collections.sort(sortedKeys);
        return sortedKeys;
    }

    // given a dataitem key, return n handlers for that data item
    // n can also be set at compile time (N parameter), it shouldn't change during
    // execution
    public HashMap<Integer, Element<T>> getHandlers(int key, int n) {
        int mod = head.prev.key;
        key = key % mod;
        HashMap<Integer, Element<T>> handlers = new HashMap<>();
        if (head != null) {
            Element<T> temp = head;
            while (temp.key < key) {
                temp = temp.next;
                // Why?
                // if (temp == head) {
                //     return handlers;
                // }
            }
            for (int i = 0; i < n; i++) {
                handlers.put(temp.key, temp);
                temp = temp.next;
            }
        }
        return handlers;
    }

    public Element<T> getNextN(int key, int n) {
        if (head == null) {
            return null;
        }
        int mod = head.prev.key;
        key = key % mod;

        Element<T> current = head;
        while (current.key < key) {
            current = current.next;
        }
        for (int i = 0; i < n; i++) {
            current = current.next;
        }
        return current;
    }

    @Override
    public String toString() {
        if (head == null) {
            return "[]";
        }
        Element<T> current = head;
        StringBuilder output = new StringBuilder("[");
        do {
            output.append(current.key);
            current = current.next;
            if (current != head) {
                output.append(", ");
            }
        } while (current != head);
        output.append("]");
        return output.toString();
    }

    @Override
    public Iterator<Element<T>> iterator() {
        return new Iterator<Element<T>>() {
            private Element<T> current = head;
            private boolean completedCircle = false;

            @Override
            public boolean hasNext() {
                return head != null && !completedCircle;
            }

            @Override
            public Element<T> next() {
                if (current == null || completedCircle) {
                    throw new NoSuchElementException();
                }
                Element<T> element = current;
                current = current.next;
                if (current == head) {
                    completedCircle = true;
                }
                return element;
            }
        };
    }

}
