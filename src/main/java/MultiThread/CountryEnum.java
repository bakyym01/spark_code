package MultiThread;

public enum CountryEnum {
    ONE(1,"齐国"),TWO(2,"楚国"),THREE(3,"燕国"),FOUR(4,"韩国"),
    FIVE(5,"赵国"),SIX(6,"魏国");
     private Integer id;
     private String country;
     CountryEnum(Integer id,String country){
        this.id=id;
        this.country = country;
    }

    public static String foreach_Country(int i){
         CountryEnum[] array =CountryEnum.values();
        for (CountryEnum count:array
             ) {
            if(count.id==i) return count.country;
        }
        return null;

    }


}
