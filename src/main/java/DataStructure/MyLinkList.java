package DataStructure;

public class MyLinkList<E> {
    private static class Node<E>{
        Node<E> pre;
        Node<E> next;
        E item;
        public Node (Node<E> pre,E item,Node<E> next){
            this.item=item;
            this.pre=pre;
            this.next=next;
        }
    }
    private int size;
    private Node<E> head;
    private Node<E> last;
    public boolean add(E element){
        addLast(element);
        return true;
    }
    public void addLast(E element){
        final Node<E> l=last;
        final Node<E> ele=new Node<E>(l,element,null);
        last=ele;
        if(l==null){
            head=ele;
        }else{
            l.next=ele;
        }
        size++;
    }
    public void addFirst(E element){
        final Node<E> h=head;
        final Node<E> node=new Node<E>(null,element,h);
        head=node;
        if(h==null){
            last=node;
        }else{
            h.pre=node;
        }
        size++;
    }
    private void addBefore(E element,Node<E> succ){
        final Node<E> pre=succ.pre;
        final Node<E> newNode=new Node<E>(pre,element,succ);
        succ.pre=newNode;
        if(pre==null){
            head=newNode;
        }else{
            pre.next=newNode;
        }
        size++;
    }
    private E unlinkLast(Node<E> l){
        final E finalElement=l.item;
        final Node<E> newLast=l.pre;
        l.item=null;
        l.pre=null;
        last=newLast;
        if(newLast==null){
            head=null;
        }else{
            newLast.next=null;
        }
        size--;
        return finalElement;
    }
    private E unlinkHead(Node<E> f){
        final E finalElement=f.item;
        final Node<E> newFirst=f.next;
        f.item=null;
        f.next=null;
        head=newFirst;
        if(newFirst==null){
            last=null;
        }else{
            newFirst.pre=null;
        }
        size--;
        return finalElement;
    }
    public E unlink(Node<E> node){
        final Node<E> pre=node.pre;
        final Node<E> next=node.next;
        final E element=node.item;
        if(pre==null){
            head=next;
        }else{
            pre.next=next;
            node.pre=null;
        }
        if(next==null){
            last=pre;
        }else{
            next.pre=pre;
            node.next=null;
        }
        node.item=null;
        size--;
        return element;
    }


    public int size(){
        return size;
    }
    public E get(int index){
        Node<E> getNode=node(index);
        return getNode.item;
    }
    public void checkRangeForIndex(int index){
        if(index>size||index<0){
            throw new IndexOutOfBoundsException("index越界异常");
        }
    }
    public Node<E> node(int index){
        if(index<(size>>1)){
            Node<E> start=head;
            for(int i=0;i<index;i++){
                start=start.next;
            }
            return start;
        }else{
            Node<E> end=last;
            for(int i=size-1;i>index;i--){
                end=end.pre;
            }
            return end;
        }
    }
    public void add(int index,E elment){
        checkRangeForIndex(index);
        if(index==size){
            addLast(elment);
        }else{
            Node<E> inNode=node(index);
            addBefore(elment,inNode);
        }
    }
    public int indexOf(Object o){
        int index=0;
        if(o==null){
            for(Node<E> x=head;x!=null;x.next=x){
                if(x.item==null){
                    return index++;
                }
                return index;
            }
        }else{
            for(Node<E> x=head;x!=null;x.next=x){
                if(x.item==null){
                    return index++;
                }
                return index;
            }

        }
        return -1;
    }
    public boolean remove(Object o){
        if(o==null){
            for(Node<E> x=head;x!=null;x=x.next){
                if(x.item==null){
                    unlink(x);
                    return true;
                }
            }
        }else{
            for(Node<E> x=head;x!=null;x=x.next){
                if(x.item.equals(o)){
                    unlink(x);
                    return true;
                }
            }
        }
        return false;
    }
    public E remove(int index){
        Node<E> deleteE=node(index);
        E element=unlink(deleteE);
        return element;
    }
}
