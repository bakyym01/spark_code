package DataStructure;

public class LongestPrefix {
    public static void main(String[] args) {
        String first="abcdeabcdesa";
        String second="description";
        String result=prefix(first,second);
        System.out.println(result);
    }

    private static String prefix(String first, String second) {
        if(first==null||second==null)
            return "";
        char [] cf=first.toCharArray();
        char [] sf=second.toCharArray();
        int i=0,j=0;
        int cl=cf.length,sl=sf.length;
        int max=0;
        while(i<cl&&j<sl){
            if(cf[i]==sf[j]){
                j++;
            }else{
                j=0;
            }
            i++;
            max=(max>j)?max:j;
        }
        return second.substring(0,max);
    }
}
