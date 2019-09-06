package DataStructure;

public class Test3 {
    public static void main(String[] args) {
        int [] nums={1,2,3,4,5,6};
        sort(nums);
        for(int i:nums){
            System.out.println(i);
        }
    }

    public static void sort(int [] nums){
        int start = 0;
        int end = nums.length-1;
        int temp=0;
        while(start<end){
            while(start<end&&nums[start]%2!=1){
                start++;
            }
            while(start<end&&nums[end]%2!=0){
                end--;
            }
            temp =nums[start];
            nums[start] = nums[end];
            nums[end] = temp;
        }
    }
}
