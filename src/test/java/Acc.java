import java.util.HashMap;
import java.util.Map;

public class Acc {
    public static void main(String[] args) {
        Map<String, Integer> hp =  new HashMap<>();
        String str = "asddfgsg";
        for(int i=0;i<str.length();i++){
            String v=String.valueOf(str.charAt(i));
            if(hp.containsKey(v)){
                hp.put(v,(int)(hp.get(v)+1));
            }else{
               hp.put(v,1);
            }
        }
        System.out.println(hp);
    }
}
