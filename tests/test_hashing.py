"""Tests for repro/hashing helpers (Slice 4 Task 4.2)."""

from __future__ import annotations


class TestSha256Bytes:
    def test_known_value(self):
        from portiere.repro.hashing import sha256_bytes

        # sha256("hello") = 2cf24dba5fb0a30e26e83b2ac5b9e29e1b161e5c1fa7425e73043362938b9824
        assert (
            sha256_bytes(b"hello")
            == "2cf24dba5fb0a30e26e83b2ac5b9e29e1b161e5c1fa7425e73043362938b9824"
        )

    def test_empty(self):
        from portiere.repro.hashing import sha256_bytes

        # sha256("") = e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855
        assert (
            sha256_bytes(b"") == "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
        )

    def test_deterministic(self):
        from portiere.repro.hashing import sha256_bytes

        assert sha256_bytes(b"x" * 1000) == sha256_bytes(b"x" * 1000)


class TestSha256Text:
    def test_unicode_safe(self):
        from portiere.repro.hashing import sha256_text

        # Two valid UTF-8 strings; different content → different hash.
        a = sha256_text("café")
        b = sha256_text("cafe")
        assert a != b

    def test_matches_explicit_utf8(self):
        from portiere.repro.hashing import sha256_bytes, sha256_text

        s = "test string"
        assert sha256_text(s) == sha256_bytes(s.encode("utf-8"))


class TestSha256File:
    def test_small_file(self, tmp_path):
        from portiere.repro.hashing import sha256_bytes, sha256_file

        p = tmp_path / "x.bin"
        p.write_bytes(b"some content")
        assert sha256_file(p) == sha256_bytes(b"some content")

    def test_large_chunked_file(self, tmp_path):
        from portiere.repro.hashing import sha256_bytes, sha256_file

        # 5 MB — exceeds default 1 MB chunk size; verifies streaming
        data = b"a" * (5 * 1024 * 1024)
        p = tmp_path / "large.bin"
        p.write_bytes(data)
        assert sha256_file(p) == sha256_bytes(data)

    def test_chunk_size_does_not_affect_result(self, tmp_path):
        from portiere.repro.hashing import sha256_file

        p = tmp_path / "x.bin"
        p.write_bytes(b"abcdefghijklmnop" * 100_000)
        assert sha256_file(p, chunk_size=1024) == sha256_file(p, chunk_size=1 << 20)


class TestSha256FileOrMetadata:
    def test_small_file_uses_content_hash(self, tmp_path):
        from portiere.repro.hashing import sha256_file, sha256_file_or_metadata

        p = tmp_path / "x.bin"
        p.write_bytes(b"hello")
        result = sha256_file_or_metadata(p)
        assert not result.startswith("meta:")
        assert result == sha256_file(p)

    def test_large_file_uses_metadata(self, tmp_path, monkeypatch):
        # Force metadata path by lowering the threshold to 1 byte
        import portiere.repro.hashing as hashing
        from portiere.repro.hashing import sha256_file_or_metadata

        monkeypatch.setattr(hashing, "LARGE_FILE_THRESHOLD", 1)

        p = tmp_path / "huge.bin"
        p.write_bytes(b"any content")
        result = sha256_file_or_metadata(p)
        assert result.startswith("meta:")

    def test_metadata_changes_with_content(self, tmp_path, monkeypatch):
        import os
        import time

        import portiere.repro.hashing as hashing
        from portiere.repro.hashing import sha256_file_or_metadata

        monkeypatch.setattr(hashing, "LARGE_FILE_THRESHOLD", 1)

        p = tmp_path / "f.bin"
        p.write_bytes(b"a")
        first = sha256_file_or_metadata(p)

        # Change content and bump mtime explicitly so the metadata hash differs
        time.sleep(0.01)
        p.write_bytes(b"abc")
        os.utime(p, ns=(time.time_ns(), time.time_ns()))
        second = sha256_file_or_metadata(p)
        assert first != second
