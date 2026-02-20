use crc32fast::Hasher;

/// Calculate a CRC32 checksum of the given content, line by line.
///
/// This matches Flyway's checksum behavior: each line is read without its
/// line ending (matching Java's BufferedReader.readLine()), and the bytes
/// are fed into the CRC32 hasher.
pub fn calculate_checksum(content: &str) -> i32 {
    let mut hasher = Hasher::new();
    for line in content.lines() {
        hasher.update(line.as_bytes());
    }
    hasher.finalize() as i32
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_checksum_basic() {
        let checksum = calculate_checksum("SELECT 1;");
        assert_ne!(checksum, 0);
    }

    #[test]
    fn test_checksum_deterministic() {
        let content = "CREATE TABLE users (id SERIAL PRIMARY KEY);\n";
        assert_eq!(calculate_checksum(content), calculate_checksum(content));
    }

    #[test]
    fn test_checksum_line_ending_normalization() {
        // \r\n and \n should produce the same checksum since str::lines()
        // strips line endings
        let unix = "line1\nline2\nline3";
        let windows = "line1\r\nline2\r\nline3";
        assert_eq!(calculate_checksum(unix), calculate_checksum(windows));
    }

    #[test]
    fn test_checksum_different_content() {
        let a = "SELECT 1;";
        let b = "SELECT 2;";
        assert_ne!(calculate_checksum(a), calculate_checksum(b));
    }

    #[test]
    fn test_checksum_empty() {
        let checksum = calculate_checksum("");
        // Empty content should produce the CRC32 initial value
        assert_eq!(checksum, 0);
    }

    #[test]
    fn test_checksum_flyway_compatible() {
        // Verify our algorithm matches Flyway's: CRC32 over line bytes (no newlines),
        // UTF-8 encoded, cast to i32.
        //
        // For "SELECT 1;\n", Flyway reads one line "SELECT 1;" and feeds its UTF-8
        // bytes into CRC32. We can verify by computing CRC32 of "SELECT 1;" directly.
        let content = "SELECT 1;\n";
        let checksum = calculate_checksum(content);

        // Compute expected: CRC32 of just "SELECT 1;" bytes (no newline)
        let mut expected = Hasher::new();
        expected.update(b"SELECT 1;");
        let expected = expected.finalize() as i32;

        assert_eq!(checksum, expected);
    }

    #[test]
    fn test_checksum_multiline_flyway_compatible() {
        // Flyway feeds each line separately (without newlines) into the same CRC32 hasher.
        // "CREATE TABLE t (\n  id INT\n);\n" becomes three updates:
        //   hasher.update("CREATE TABLE t (")
        //   hasher.update("  id INT")
        //   hasher.update(");")
        let content = "CREATE TABLE t (\n  id INT\n);\n";
        let checksum = calculate_checksum(content);

        let mut expected = Hasher::new();
        expected.update(b"CREATE TABLE t (");
        expected.update(b"  id INT");
        expected.update(b");");
        let expected = expected.finalize() as i32;

        assert_eq!(checksum, expected);
    }
}
