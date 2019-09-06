package DataStructure;

import java.util.LinkedList;

public class Split_str {
    private static LinkedList<String> result=new LinkedList<String>();
    public static void main(String[] args) {
        String test = "abc---def::ghi::jkl:mno-";
        String [] list = {"--","ghi",":","-","rst"};
        String[] ans= split(test,list);
        for(String i:ans){
            System.out.println(i);
        }
    }
    public static String [] split(String str,String [] listTokens){
        split(str,listTokens,0);
        return result.toArray(new String[0]);
    }
    public static void split(String str,String [] listTokens,int start){
        if(start>=listTokens.length)return;
        if(result.size()>0)result.remove(result.size()-1);
        String regex = listTokens[start];
        if(str.contains(regex)){
            String [] list =str.split(regex,-1);
            for(String i:list){
                result.add(i);
                split(i,listTokens,start+1);
            }
        }else result.add(str);
    }
}
