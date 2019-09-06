package kafka;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class Sum4 {
    public static void main(String[] args) {
        int [] input = {-1,-5,-5,-3,2,5,0,4};
        int tagret = -7;
        System.out.println(fourSum(input,tagret));
    }
    public static List<List<Integer>> fourSum(int[] nums, int target) {
        List<List<Integer>> res = new ArrayList();
        if(nums==null||nums.length==0) return res;
        int len=nums.length;
        Arrays.sort(nums);
        for(int i=0;i<len-3;i++){
            if(i>1&&nums[i]==nums[i-1]) continue;
            if(nums[i]+nums[len-1]+nums[len-2]+nums[len-3]<target) continue;
            if(nums[i]+nums[i+1]+nums[i+2]+nums[i+3]>target) break;
            for(int j=i+1;j<len-2;j++){
                if(j-i>1&&nums[j]==nums[j-1]) continue;
                if(nums[i]+nums[j]+nums[len-2]+nums[len-1]<target)continue;
                if(nums[i]+nums[j]+nums[j+1]+nums[j+2]>target) break;
                int left = j+1;
                int right = len-1;
                while(right>left){
                    if(nums[i]+nums[j]+nums[left]+nums[right]==target){
                        List<Integer> tmp = new ArrayList(Arrays.asList(nums[i],nums[j],nums[left],nums[right]));
                        res.add(tmp);
                        while(left<right&&nums[left]==nums[left+1]) left++;
                        while(left<right&&nums[right]==nums[right-1]) right--;
                        left++;
                        right--;
                    }else if(nums[i]+nums[j]+nums[left]+nums[right]>target){
                        right--;
                    }else{
                        left++;
                    }
                }
            }
        }
        return res;

    }
}
