"""File chunker that splits log files into byte-range chunks at line boundaries."""

import os


def split_file(
    file_path: str, chunk_size: int = 67_108_864
) -> list[tuple[str, int, int]]:
    """Split a file into chunks at line boundaries.

    Returns list of (file_path, start_byte, end_byte) tuples.
    Each chunk boundary falls at a newline character so no line is split.
    """
    file_size = os.path.getsize(file_path)

    if file_size == 0:
        return []

    if file_size <= chunk_size:
        return [(file_path, 0, file_size)]

    chunks: list[tuple[str, int, int]] = []
    start = 0

    with open(file_path, "rb") as f:
        while start < file_size:
            end = min(start + chunk_size, file_size)

            if end < file_size:
                # Seek to the tentative end and find the next newline
                f.seek(end)
                remainder = f.read(8192)
                while remainder:
                    nl_pos = remainder.find(b"\n")
                    if nl_pos != -1:
                        end = f.tell() - len(remainder) + nl_pos + 1
                        break
                    end = f.tell()
                    remainder = f.read(8192)
                else:
                    # Reached EOF without finding newline
                    end = file_size

            chunks.append((file_path, start, end))
            start = end

    return chunks


def read_chunk(file_path: str, start: int, end: int) -> list[str]:
    """Read lines from a byte range of a file.

    Opens the file, seeks to start, reads (end - start) bytes,
    splits into lines. Returns list of non-empty line strings.
    """
    with open(file_path, "rb") as f:
        f.seek(start)
        data = f.read(end - start)

    text = data.decode("utf-8", errors="replace")
    return [line for line in text.splitlines() if line.strip()]
