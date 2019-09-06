package DataStructure;

import java.util.Arrays;

public class ChainMiddle {
    public static void main(String[] args) {
        int [] nums ={3,5,11,19,2};
        System.out.println(splitArraySameAverage(nums));
    }

    public static boolean splitArraySameAverage(int[] A) {
        if(A==null||A.length==0) return false;
        int len = A.length;
        int sum = 0;
        for(int i:A){
            sum+=i;
        }
        Arrays.sort(A);
        for(int i=1;i<=len/2;i++){
            if(sum*i%len ==0 && dfs(A,0,i,sum*i/len)) return true;
        }
        return false;
    }

    public static boolean dfs(int [] nums,int begin,int n,int target){
        if(n==0) return target==0;
        if(nums[begin]>target/n) return false;
        for(int i=begin;i<=nums.length-n;i++){
            if(i>begin && nums[i]==nums[i-1]) continue;
            if(dfs(nums,i+1,n-1,target-nums[i])) return true;
        }
        return false;
    }
}
