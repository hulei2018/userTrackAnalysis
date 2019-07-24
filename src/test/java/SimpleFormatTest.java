import java.text.SimpleDateFormat;
import java.util.Date;

public class SimpleFormatTest {
    public static final String DATE_FORMAT="yyyy-MM-dd";
    public static void main(String[] args) {
        SimpleDateFormat sft = new SimpleDateFormat(DATE_FORMAT);
        String currentDate = sft.format(new Date());
        System.out.println(currentDate);

    }
}
