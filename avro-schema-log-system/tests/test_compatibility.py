"""Tests for CompatibilityChecker."""


class TestCompatibilityChecker:
    """Tests for check_compatibility() and build_compatibility_matrix()."""

    def test_compatibility_matrix_9_of_9(self, checker):
        """Build the full matrix and assert all 9 pairs (3x3) are True."""
        matrix = checker.build_compatibility_matrix()
        true_count = sum(
            1
            for writer in matrix
            for reader in matrix[writer]
            if matrix[writer][reader]
        )
        assert true_count == 9, f"Expected 9/9 compatible pairs, got {true_count}/9"

    def test_check_single_pair(self, checker):
        """Check that v1 -> v2 is compatible."""
        assert checker.check_compatibility("v1", "v2") is True

    def test_all_backward_compatible(self, checker):
        """Newer readers can read older writer data: v1->v2, v1->v3, v2->v3."""
        assert checker.check_compatibility("v1", "v2") is True
        assert checker.check_compatibility("v1", "v3") is True
        assert checker.check_compatibility("v2", "v3") is True

    def test_all_forward_compatible(self, checker):
        """Older readers can read newer writer data: v2->v1, v3->v1, v3->v2."""
        assert checker.check_compatibility("v2", "v1") is True
        assert checker.check_compatibility("v3", "v1") is True
        assert checker.check_compatibility("v3", "v2") is True

    def test_self_compatible(self, checker):
        """Each version is compatible with itself."""
        for v in ["v1", "v2", "v3"]:
            assert checker.check_compatibility(v, v) is True
