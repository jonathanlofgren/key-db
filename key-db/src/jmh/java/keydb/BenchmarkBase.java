package keydb;

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

public class BenchmarkBase {

    public HashMap<String, String> generateData(final int num) {
        final HashMap<String, String> data = new HashMap<>();
        for (int i = 0; i < num; i++) {
            data.put(UUID.randomUUID().toString().substring(0, 14), UUID.randomUUID().toString());
        }

        return data;
    }

    public void populateDatabase(final KeyDB db, final HashMap<String, String> data) {
        for (final Map.Entry<String, String> entry : data.entrySet()) {
            db.put(entry.getKey(), entry.getValue());
        }
    }
}
