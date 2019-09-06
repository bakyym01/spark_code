package DataStructure;

public class Test {
    public static void main(String[] args) {

        Changer c=new Changer();
        c.c(args);
        System.out.println(args[0]+" "+args[1]);
    }
    static class Changer{
        void c(String [] s){
            String temp =s[0];
            s[1]=s[0];
            s[1]=temp;
        }
    }
}
