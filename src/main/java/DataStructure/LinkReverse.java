package DataStructure;

public class LinkReverse {
    public static void main(String[] args) {
        LinkNode<Integer> head = new LinkNode<Integer>(1);
        LinkNode<Integer> cur=head;
        for (int i = 2; i <=10 ; i++) {
            cur.next=new LinkNode<Integer>(i);
            cur = cur.next;
        }

        LinkNode<Integer> newhead = reverseLinked(head);

        while(newhead!=null){
            System.out.println(newhead.value);
            newhead = newhead.next;
        }

    }

    private static LinkNode<Integer> reverseLinked(LinkNode<Integer> head) {
        LinkNode<Integer> pre;
        LinkNode<Integer> cur;
        LinkNode<Integer> temp;
        pre=head;
        cur= head.next;
        pre.next=null;
        while(cur!=null){
            temp =cur.next;
            cur.next = pre;
            pre = cur;
            cur = temp;
        }
        return pre;
    }
}
class LinkNode<T>{
    T value=null;
    LinkNode<T> next;

    public LinkNode(T value) {
        this.value = value;
    }
}


