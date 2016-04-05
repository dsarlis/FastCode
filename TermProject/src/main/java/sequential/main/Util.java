package sequential.main;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class Util {

    public static <K, V> List<V> getDefaultList(Map<K, List<V>> map, K key) {
        List<V> realValue = map.get(key);

        if (realValue == null) {
            realValue = new ArrayList<V>();
            map.put(key, realValue);
        }
        return realValue;
    }

}
