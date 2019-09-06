package DataStructure;

public class PalindromeList {
    public static void main(String[] args) {
        Palindrome<String> head=new Palindrome<String>("a");
        head.add("b").add("c").add("b").add("a");
        System.out.println(isPalindrome(head));
    }
    public static boolean isPalindrome(Palindrome test ){
        Palindrome slow=test;
        Palindrome fast=test;
        Palindrome prev=null;

        while(fast!=null&&fast.next!=null){
            fast=fast.next.next;
            Palindrome next=slow.next;
            slow.next=prev;
            prev=slow;
            slow=next;
        }
        if(fast!=null){
            slow=slow.next;
        }
        while(slow!=null){
            if(!slow.value.equals(prev.value)){
                return false;
            }
            slow=slow.next;
            prev=prev.next;
        }
        return true;
    }

}
class Palindrome<E>{
    E value;
    Palindrome<E> next;
    public Palindrome<E> add(E element){
        Palindrome<E> next=new Palindrome<E>(element);
        this.next=next;
        return this;
    }
    public Palindrome(E element){
        this.value=element;
    }

}
