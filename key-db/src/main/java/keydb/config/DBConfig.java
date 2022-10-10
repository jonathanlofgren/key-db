package keydb.config;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.NoArgsConstructor;
import lombok.Value;

import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;

@Value
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class DBConfig {

    // The size at which the MemTable will be persisted to disk
    // to form a new segment
    // Default: 2 MB
    @Builder.Default
    long memTableFlushSizeBytes = 2_000_000;

    // Approximate number of bytes between the indices that are created
    // when a MemTable is persisted to form a new segment
    // Default: 4 KB
    @Builder.Default
    long bytesPerIndex = 4_096;

    // Charset used when serializing the String key/value pairs
    // Default: UTF-8
    @Builder.Default
    Charset charset = StandardCharsets.UTF_8;
}
