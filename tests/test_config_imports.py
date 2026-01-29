"""
Test that all modules import KAZ_ERA_START from src/config.py

This test ensures we maintain a single source of truth for the Kaz-era definition.
If any module hardcodes KAZ_ERA_START, this test will fail.
"""

import pytest
import ast
import os
from pathlib import Path
from datetime import datetime


class TestConfigSingleSourceOfTruth:
    """Verify src/config.py is the single source of truth for KAZ_ERA_START."""

    def test_config_defines_kaz_era_start(self):
        """Verify config.py defines KAZ_ERA_START."""
        from src.config import KAZ_ERA_START
        assert KAZ_ERA_START is not None
        assert isinstance(KAZ_ERA_START, datetime)
        # Canonical date: September 10, 2025
        assert KAZ_ERA_START == datetime(2025, 9, 10)

    def test_config_exports_helper_functions(self):
        """Verify config.py exports helper functions."""
        from src.config import is_kaz_era, get_cohort, get_signal_status
        assert callable(is_kaz_era)
        assert callable(get_cohort)
        assert callable(get_signal_status)

    def test_is_kaz_era_function(self):
        """Test the is_kaz_era helper function."""
        from src.config import is_kaz_era

        # Should return True for dates on/after Sep 10, 2025
        assert is_kaz_era(datetime(2025, 9, 10)) == True
        assert is_kaz_era(datetime(2025, 9, 11)) == True
        assert is_kaz_era(datetime(2026, 1, 1)) == True

        # Should return False for dates before Sep 10, 2025
        assert is_kaz_era(datetime(2025, 9, 9)) == False
        assert is_kaz_era(datetime(2025, 1, 1)) == False
        assert is_kaz_era(datetime(2023, 10, 1)) == False  # Old incorrect date

        # Should handle edge cases
        assert is_kaz_era(None) == False
        assert is_kaz_era("2025-09-10") == True
        assert is_kaz_era("2025-09-09") == False

    def test_cohort_thresholds_defined(self):
        """Verify cohort thresholds are defined in config."""
        from src.config import (
            COHORT_NEW_MAX_DAYS,
            COHORT_MID_MAX_DAYS,
            COHORT_OLD_MAX_DAYS,
        )
        assert COHORT_NEW_MAX_DAYS == 90
        assert COHORT_MID_MAX_DAYS == 180
        assert COHORT_OLD_MAX_DAYS == 365

    def test_get_cohort_function(self):
        """Test the get_cohort helper function."""
        from src.config import get_cohort

        assert get_cohort(30) == 'new'
        assert get_cohort(89) == 'new'
        assert get_cohort(90) == 'mid'
        assert get_cohort(179) == 'mid'
        assert get_cohort(180) == 'old'
        assert get_cohort(364) == 'old'
        assert get_cohort(365) == 'toxic'
        assert get_cohort(500) == 'toxic'
        assert get_cohort(None) == 'unknown'


class TestNoHardcodedKazEraStart:
    """Verify no module hardcodes KAZ_ERA_START outside of config.py."""

    @pytest.fixture
    def project_root(self):
        """Get project root directory."""
        return Path(__file__).parent.parent

    def _find_python_files(self, root: Path):
        """Find all Python files in the project."""
        files = []
        for pattern in ['src/**/*.py', 'scripts/*.py']:
            files.extend(root.glob(pattern))
        return files

    def _check_file_for_hardcoded_date(self, filepath: Path) -> list:
        """Check a file for hardcoded KAZ_ERA_START definitions.

        Returns list of violations found.
        """
        violations = []

        # Skip config.py itself - it's the source of truth
        if filepath.name == 'config.py' and 'src' in str(filepath):
            return []

        try:
            with open(filepath, 'r') as f:
                content = f.read()

            # Parse the AST to find assignments
            try:
                tree = ast.parse(content)
            except SyntaxError:
                return []  # Skip files that can't be parsed

            for node in ast.walk(tree):
                # Check for direct assignment: KAZ_ERA_START = datetime(...)
                if isinstance(node, ast.Assign):
                    for target in node.targets:
                        if isinstance(target, ast.Name) and target.id == 'KAZ_ERA_START':
                            # Check if it's a datetime call (hardcoded)
                            if isinstance(node.value, ast.Call):
                                # Get the function name
                                if isinstance(node.value.func, ast.Name):
                                    if node.value.func.id == 'datetime':
                                        violations.append(
                                            f"{filepath}: Line {node.lineno} - "
                                            f"Hardcoded KAZ_ERA_START = datetime(...)"
                                        )
                                elif isinstance(node.value.func, ast.Attribute):
                                    if node.value.func.attr == 'datetime':
                                        violations.append(
                                            f"{filepath}: Line {node.lineno} - "
                                            f"Hardcoded KAZ_ERA_START"
                                        )

        except Exception as e:
            pass  # Skip files that can't be read

        return violations

    def test_no_hardcoded_kaz_era_in_src(self, project_root):
        """Verify src/ modules don't hardcode KAZ_ERA_START."""
        all_violations = []

        for filepath in project_root.glob('src/**/*.py'):
            violations = self._check_file_for_hardcoded_date(filepath)
            all_violations.extend(violations)

        if all_violations:
            pytest.fail(
                "Found hardcoded KAZ_ERA_START definitions:\n" +
                "\n".join(all_violations) +
                "\n\nAll modules should import from src.config"
            )

    def test_no_hardcoded_kaz_era_in_scripts(self, project_root):
        """Verify scripts/ don't hardcode KAZ_ERA_START."""
        all_violations = []

        for filepath in project_root.glob('scripts/*.py'):
            violations = self._check_file_for_hardcoded_date(filepath)
            all_violations.extend(violations)

        if all_violations:
            pytest.fail(
                "Found hardcoded KAZ_ERA_START definitions:\n" +
                "\n".join(all_violations) +
                "\n\nAll scripts should import from src.config"
            )

    def test_modules_import_from_config(self, project_root):
        """Verify key modules import KAZ_ERA_START from config."""
        modules_that_should_import = [
            'src/metrics/kaz_era.py',
            'src/metrics/pending_tracker.py',
            'src/metrics/v3_metrics.py',
            'scripts/scrape_pending.py',
            'scripts/scrape_opendoor.py',
            'scripts/backfill_history.py',
        ]

        for module_path in modules_that_should_import:
            filepath = project_root / module_path
            if not filepath.exists():
                continue

            with open(filepath, 'r') as f:
                content = f.read()

            # Check for import from config
            has_import = (
                'from src.config import' in content and
                'KAZ_ERA_START' in content
            )

            assert has_import, (
                f"{module_path} should import KAZ_ERA_START from src.config"
            )


class TestSignalThresholds:
    """Test signal threshold definitions."""

    def test_pace_thresholds(self):
        """Test guidance pace thresholds."""
        from src.config import PACE_GREEN_MIN, PACE_YELLOW_MIN
        assert PACE_GREEN_MIN == 95.0
        assert PACE_YELLOW_MIN == 80.0

    def test_win_rate_thresholds(self):
        """Test win rate thresholds."""
        from src.config import WIN_RATE_GREEN_MIN, WIN_RATE_YELLOW_MIN
        assert WIN_RATE_GREEN_MIN == 85.0
        assert WIN_RATE_YELLOW_MIN == 70.0

    def test_get_signal_status(self):
        """Test get_signal_status function."""
        from src.config import get_signal_status

        # Pace (higher is better)
        assert get_signal_status('pace', 100) == 'green'
        assert get_signal_status('pace', 90) == 'yellow'
        assert get_signal_status('pace', 70) == 'red'

        # Toxic pct (lower is better)
        assert get_signal_status('toxic_pct', 3) == 'green'
        assert get_signal_status('toxic_pct', 8) == 'yellow'
        assert get_signal_status('toxic_pct', 15) == 'red'
