class QuorumMetrics:
    def __init__(self):
        self.total_reads: int = 0
        self.total_writes: int = 0
        self.failed_reads: int = 0
        self.failed_writes: int = 0
        self.consistency_violations: int = 0

    def record_read(self, success: bool):
        self.total_reads += 1
        if not success:
            self.failed_reads += 1

    def record_write(self, success: bool):
        self.total_writes += 1
        if not success:
            self.failed_writes += 1

    def record_violation(self):
        self.consistency_violations += 1

    def to_dict(self) -> dict:
        return {
            "total_reads": self.total_reads,
            "total_writes": self.total_writes,
            "failed_reads": self.failed_reads,
            "failed_writes": self.failed_writes,
            "consistency_violations": self.consistency_violations,
        }
