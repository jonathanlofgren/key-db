package keydb.types;

import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.Optional;
import java.util.ServiceLoader;

public interface ValueIO<T> {

    void write(final T value, final DataOutput output) throws IOException;

    T read(final DataInput input) throws IOException;

    boolean canHandle(Class<?> stringClass);

    @SuppressWarnings("unchecked")
    static <P> ValueIO<P> getProvider(final Class<P> clazz) {
        final ServiceLoader<ValueIO> loaded = ServiceLoader.load(ValueIO.class);

        final Optional<ValueIO> first = loaded.stream()
                .filter(valueIOProvider -> valueIOProvider.get().canHandle(clazz))
                .map(ServiceLoader.Provider::get).findFirst();

        return first.orElseThrow(() -> new RuntimeException("No ValueIO implementation found"));
    }
}
