package keydb.file;

import java.io.Closeable;

public abstract class FileManager<T extends Closeable> implements AutoCloseable {

    protected final T resource;

    protected FileManager(final T resource) {
        this.resource = resource;
    }

    public void close() throws Exception {
        resource.close();
    }
}
