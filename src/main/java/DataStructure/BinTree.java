package DataStructure;

import java.util.ArrayList;
import java.util.List;
import java.util.Stack;

public class BinTree {
    private static List<Node> listNode=null;
    private static class Node{
        Node Left;
        Node Right;
        int data;
        Node(Node left,Node right,int data){
            this.Left=left;
            this.Right=right;
            this.data=data;
        }
        Node(int data){
            this.Left=null;
            this.Right=null;
            this.data=data;
        }
    }

    public void createBinTree(int[] arr) {
        listNode = new ArrayList<Node>();
        for (int i = 0; i <arr.length ; i++) {
            listNode.add(new Node(arr[i]));
        }
        for (int parent = 0; parent <arr.length/2-1 ; parent++) {
            listNode.get(parent).Left=listNode.get(parent*2+1);
            listNode.get(parent).Right=listNode.get(parent*2+2);
        }
        int lastIndex=arr.length/2-1;
        listNode.get(lastIndex).Left=listNode.get(lastIndex*2+1);
        if(arr.length%2==1){
            listNode.get(lastIndex).Right=listNode.get(lastIndex*2+2);
        }
    }

    public static void preOrderTraverse(Node node){
        if(node==null){
            return;
        }
        System.out.println(node.data);
        preOrderTraverse(node.Left);
        preOrderTraverse(node.Right);
    }
    public static void inOrderTraverse(Node node){
        if(node==null){
            return;
        }
        inOrderTraverse(node.Left);
        System.out.println(node.data);
        inOrderTraverse(node.Right);
    }
    public static void postOrderTraverse(Node node){
        if(node==null){
            return;
        }
        postOrderTraverse(node.Left);
        postOrderTraverse(node.Right);
        System.out.println(node.data);
    }

    public static void preOrderNoRe(Node node){
        Stack<Node> stack= new Stack<Node>();
        while(node!=null||!stack.isEmpty()){
            if(node!=null){
                System.out.println(node.data);
                stack.push(node);
                node=node.Left;
            }else{
                node=stack.pop();
                node=node.Right;
            }

        }
    }

    public static void inOrderNoRe(Node node){
        Stack<Node> stack=new Stack();
        while(node!=null||! stack.isEmpty()){
            if(node!=null){
                stack.push(node);
                node=node.Left;
            }else{
                node=stack.pop();
                System.out.println(node.data);
                node=node.Right;
            }
        }

    }

    public static void postOrderNoRe(Node node){
        Stack<Node> stack=new Stack<Node>();
        Stack<Node> output=new Stack<Node>();
        while(node!=null||!stack.isEmpty()){
            if(node!=null){
                stack.push(node);
                output.push(node);
                node=node.Right;
            }else{
                node=stack.pop();
                node=node.Left;
            }
        }
        while(!output.isEmpty()){
            System.out.println(output.pop().data);
        }

    }

    public static int getLeafNum(Node node){
        if(node!=null){
            int l=getLeafNum(node.Left);
            int r=getLeafNum(node.Right);
            if(l==0&&r==0){
                return 1;
            }else{
                return l+r;
            }
        }else{
            return 0;
        }
    }

    public static int nodeCount(Node node){
        if(node!=null){
            int l=nodeCount(node.Left);
            int r=nodeCount(node.Right);
            if(l==0&&r==0){
                return 1;
            }else{
                return l+r+1;
            }
        }else{
            return 0;
        }

    }

    public static int getDepth(Node node){
        if(node!=null){
            int l=getDepth(node.Left);
            int r=getDepth(node.Right);
            if(l>r){
                return l+1;
            }else{
                return r+1;
            }
        }else{
            return 0;
        }

    }

    public static int lastWordLength(String str){
        char [] carr = str.toCharArray();
        int i=0;
        int j=carr.length-1;
        while(carr[j]==' '){
            j--;
        }
        int index=j;
        while(carr[index]!=' '){
            index--;
        }
        return j-index;
    }


    public static void main(String[] args) {
        int [] arr={1,2,3,4,5,6,7,8,9,10};
        new BinTree().createBinTree(arr);
//        preOrderTraverse(DataStructure.BinTree.listNode.get(0));
//        inOrderTraverse(DataStructure.BinTree.listNode.get(0));
//        postOrderTraverse(DataStructure.BinTree.listNode.get(0));
//        preOrderNoRe(DataStructure.BinTree.listNode.get(0));
//        inOrderNoRe(DataStructure.BinTree.listNode.get(0));
//        postOrderNoRe(DataStructure.BinTree.listNode.get(0));
        System.out.println(nodeCount(BinTree.listNode.get(0)));
//        System.out.println(getDepth(listNode.get(0)));

        String str="hello word";
        System.out.println(lastWordLength(str));
    }
}
