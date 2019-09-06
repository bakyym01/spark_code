package DataStructure;

public class MergeSort {
    public static void main(String[] args) {
        int [] test={2,335,556,21,456,768,98,342,34,56,87};
        int [] result = mergeSort(test,0,test.length-1);
        for (int i = 0; i <result.length ; i++) {
            System.out.print(result[i]+" ");
        }

    }
    public static int[] mergeSort(int[]a,int low,int high){
        int mid = (low+high)>>1;
        if(low<high){
            mergeSort(a,low,mid);
            mergeSort(a,mid+1,high);
            mergePass(a,low,mid,high);
        }

        return a;
    }
    public static void mergePass(int[]a,int low, int mid, int high){
        int[] temp=new int[high-low+1];
        int i = low;
        int j = mid+1;
        int k=0;
        while(i<=mid && j<=high){
            if( a[i]<a[j]){
                temp[k++] = a[i++];
            }else{
                temp[k++] = a[j++];
            }
        }
        while(i<=mid){
            temp[k++] = a[i++];
        }
        while(j<=high){
            temp[k++] = a[j++];
        }
        for (int l = 0; l <temp.length ; l++) {
            a[low+l] = temp[l];
        }

    }
}
