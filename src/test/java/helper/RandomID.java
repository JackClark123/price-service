package helper;

import java.util.Random;

public class RandomID {

    public static String generate() {
        Random r = new Random();
        int low = 1;
        int high = 1000000000;
        int result = r.nextInt(high-low) + low;
        return String.valueOf(result);
    }

}
