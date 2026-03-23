"""Core MapReduce engine with parallel map and reduce phases."""

import logging
import time
from collections import defaultdict
from multiprocessing import Pool
from typing import Any, Callable

from src.chunker import read_chunk, split_file
from src.parsers import detect_format, parse_line

import src.analyzers  # noqa: F401  — trigger decorator registration
from src.analyzers.registry import get_map_fn, get_postprocess_fn, get_reduce_fn

logger = logging.getLogger(__name__)

DEFAULT_TIMEOUT = 300  # seconds


# --- Worker functions (must be top-level for pickling) ---


def _map_worker(args: tuple) -> tuple[list[tuple[str, Any]], int, int]:
    """Map worker: reads a chunk, parses lines, applies map function.

    Uses a combiner step to pre-aggregate map output within each chunk,
    dramatically reducing the volume of data going through shuffle.

    Args: (file_path, start, end, format, map_fn_name)
    Returns: (list of (key, value) pairs, records_processed, records_skipped)
    """
    file_path, start, end, fmt, map_fn_name = args
    map_fn = get_map_fn(map_fn_name)

    lines = read_chunk(file_path, start, end)
    local_counts: dict[str, Any] = {}  # combiner
    processed = 0
    skipped = 0

    for line in lines:
        record = parse_line(line, fmt)
        if record is None:
            skipped += 1
            continue
        try:
            pairs = map_fn(record)
            for key, value in pairs:
                if key in local_counts:
                    local_counts[key] += value
                else:
                    local_counts[key] = value
            processed += 1
        except Exception:
            skipped += 1

    # Return pre-aggregated pairs
    return list(local_counts.items()), processed, skipped


def _reduce_worker(args: tuple) -> dict[str, Any]:
    """Reduce worker: applies reduce function to a subset of key groups.

    Args: (key_value_groups: list[(key, values_list)], reduce_fn_name)
    Returns: dict of {key: reduced_value}
    """
    key_value_groups, reduce_fn_name = args
    reduce_fn = get_reduce_fn(reduce_fn_name)

    results = {}
    for key, values in key_value_groups:
        try:
            results[key] = reduce_fn(key, values)
        except Exception:
            pass  # skip failed reductions

    return results


class MapReduceEngine:
    """Orchestrates the MapReduce pipeline: split -> map -> shuffle -> reduce."""

    def __init__(
        self,
        num_workers: int = 4,
        chunk_size: int = 67_108_864,
        timeout: int = DEFAULT_TIMEOUT,
    ):
        self.num_workers = num_workers
        self.chunk_size = chunk_size
        self.timeout = timeout

    def run(
        self,
        input_files: list[str],
        map_fn_name: str,
        reduce_fn_name: str,
        progress_callback: Callable | None = None,
    ) -> dict:
        """Run the full MapReduce pipeline.

        Args:
            input_files: list of log file paths
            map_fn_name: name of map function in registry
            reduce_fn_name: name of reduce function in registry
            progress_callback: optional callback(phase, progress_float, info_dict)

        Returns:
            dict of {key: value} results
        """
        start_time = time.time()
        total_processed = 0
        total_skipped = 0

        # 1. Split all input files into chunks
        all_chunks = []
        for file_path in input_files:
            fmt = detect_format(file_path)
            chunks = split_file(file_path, self.chunk_size)
            for chunk in chunks:
                all_chunks.append((*chunk, fmt, map_fn_name))

        total_chunks = len(all_chunks)
        if progress_callback:
            progress_callback("mapping", 0.0, {"total_chunks": total_chunks})

        # 2. Map phase - parallel
        logger.info(
            f"Map phase: processing {total_chunks} chunks with {self.num_workers} workers"
        )
        map_start = time.time()
        all_map_results = []

        if total_chunks == 0:
            if progress_callback:
                progress_callback("mapping", 1.0, {
                    "completed_chunks": 0,
                    "total_chunks": 0,
                    "records_processed": 0,
                })
        else:
            try:
                with Pool(processes=self.num_workers) as pool:
                    for i, result in enumerate(
                        pool.imap_unordered(_map_worker, all_chunks)
                    ):
                        pairs, processed, skipped = result
                        all_map_results.extend(pairs)
                        total_processed += processed
                        total_skipped += skipped
                        if progress_callback:
                            progress_callback(
                                "mapping",
                                (i + 1) / total_chunks,
                                {
                                    "completed_chunks": i + 1,
                                    "total_chunks": total_chunks,
                                    "records_processed": total_processed,
                                },
                            )
            except Exception as e:
                logger.error(f"Map phase failed: {e}")
                raise

        map_time = time.time() - map_start
        logger.info(
            f"Map phase complete in {map_time:.2f}s: {len(all_map_results)} combined pairs "
            f"from {total_processed} records ({total_skipped} skipped)"
        )

        # 3. Shuffle phase
        shuffle_start = time.time()
        if progress_callback:
            progress_callback("shuffling", 0.0, {})

        grouped = defaultdict(list)
        for key, value in all_map_results:
            grouped[key].append(value)

        shuffle_time = time.time() - shuffle_start
        logger.info(f"Shuffle phase complete in {shuffle_time:.2f}s: {len(grouped)} unique keys")
        if progress_callback:
            progress_callback("shuffling", 1.0, {"unique_keys": len(grouped)})

        # 4. Reduce phase - parallel
        if progress_callback:
            progress_callback("reducing", 0.0, {})

        # Partition keys across workers
        items = list(grouped.items())
        num_reducers = min(self.num_workers, len(items))
        if num_reducers == 0:
            elapsed = time.time() - start_time
            if progress_callback:
                progress_callback(
                    "completed",
                    1.0,
                    {
                        "execution_time": elapsed,
                        "records_processed": total_processed,
                        "records_skipped": total_skipped,
                        "unique_keys": 0,
                    },
                )
            return {}

        partitions = [[] for _ in range(num_reducers)]
        for i, item in enumerate(items):
            partitions[i % num_reducers].append(item)

        reduce_args = [
            (partition, reduce_fn_name) for partition in partitions if partition
        ]

        reduce_start = time.time()
        final_results = {}
        try:
            with Pool(processes=num_reducers) as pool:
                for i, partial in enumerate(
                    pool.imap_unordered(_reduce_worker, reduce_args)
                ):
                    final_results.update(partial)
                    if progress_callback:
                        progress_callback("reducing", (i + 1) / len(reduce_args), {})
        except Exception as e:
            logger.error(f"Reduce phase failed: {e}")
            raise

        reduce_time = time.time() - reduce_start
        logger.info(f"Reduce phase complete in {reduce_time:.2f}s")

        # 5. Postprocess phase (optional)
        postprocess_fn = get_postprocess_fn(map_fn_name)
        if postprocess_fn:
            final_results = postprocess_fn(final_results)

        elapsed = time.time() - start_time
        logger.info(
            f"Pipeline complete in {elapsed:.2f}s | {total_processed} records | "
            f"{len(final_results)} result keys"
        )

        if progress_callback:
            progress_callback(
                "completed",
                1.0,
                {
                    "execution_time": elapsed,
                    "records_processed": total_processed,
                    "records_skipped": total_skipped,
                    "unique_keys": len(final_results),
                },
            )

        return final_results
