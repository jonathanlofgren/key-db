package keydb.types;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.Optional;
import java.util.ServiceLoader;

public interface ValueIO<T> {

    long write(final T value, final DataOutputStream output) throws IOException;

    T read(final DataInputStream input) throws IOException;

    boolean canHandle(Class<?> stringClass);


    static <P> ValueIO<P> getProvider(final Class<P> clazz) {
        final ServiceLoader<ValueIO> loaded = ServiceLoader.load(ValueIO.class);

        final Optional<ValueIO> first = loaded.stream()
                .filter(valueIOProvider -> valueIOProvider.get().canHandle(clazz))
                .map(ServiceLoader.Provider::get).findFirst();

        return first.orElseThrow(() -> new RuntimeException("No ValueIO implementation found"));
    }
}
