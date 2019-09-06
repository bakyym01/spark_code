package DataStructure;

public class SentenceReserve {
    public static void main(String[] args) {
        int[] nums ={1,2,3,2,4};
        System.out.println(findDuplicate(nums));

    }

    public static boolean findDuplicate(int [] nums){
        if(nums==null||nums.length==0) return false;
        int temp=0;
        for(int i=0;i<nums.length;i++){
            if(nums[i]>nums.length-1||nums[i]<0) return false;
        }
        for (int i=0;i<nums.length;){
            if(nums[i]!=i){
                if(nums[i]==nums[nums[i]]){
                    return true;
                }
                temp = nums[i];
                nums[i]=nums[temp];
                nums[temp] = temp;
            }else{
                i++;
            }
        }
        return false;
    }
}
