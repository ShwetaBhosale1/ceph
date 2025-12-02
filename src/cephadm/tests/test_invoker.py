# Tests for invoker.py - secure wrapper for executing cephadm commands
#
import hashlib
import os
import sys
import tempfile
from pathlib import Path
from unittest import mock

import pytest


# Import the invoker module directly
sys.path.insert(0, str(Path(__file__).parent.parent))
import invoker


class TestVerifyCephadmHash:
    """Tests for verify_cephadm_hash function."""

    def test_matching_hash(self, tmp_path):
        """Test verification with matching hash."""
        content = b'#!/usr/bin/env python3\nprint("hello")\n'
        hash_value = hashlib.sha256(content).hexdigest()
        test_file = tmp_path / f'cephadm.{hash_value}'
        test_file.write_bytes(content)

        is_valid, expected, actual = invoker.verify_cephadm_hash(str(test_file))
        assert is_valid is True
        assert expected == hash_value
        assert actual == hash_value

    def test_mismatched_hash(self, tmp_path):
        """Test verification with mismatched hash."""
        content = b'#!/usr/bin/env python3\nprint("hello")\n'
        wrong_hash = 'wronghash123'
        test_file = tmp_path / f'cephadm.{wrong_hash}'
        test_file.write_bytes(content)

        is_valid, expected, actual = invoker.verify_cephadm_hash(str(test_file))
        assert is_valid is False
        assert expected == wrong_hash
        assert actual == hashlib.sha256(content).hexdigest()

    def test_no_hash_in_path(self, tmp_path):
        """Test verification when path has no hash (should be valid)."""
        content = b'#!/usr/bin/env python3\nprint("hello")\n'
        test_file = tmp_path / 'cephadm'
        test_file.write_bytes(content)

        is_valid, expected, actual = invoker.verify_cephadm_hash(str(test_file))
        assert is_valid is True
        assert expected is None
        assert actual == hashlib.sha256(content).hexdigest()

    def test_nonexistent_file(self, tmp_path):
        """Test verification of nonexistent file."""
        test_file = tmp_path / 'nonexistent'
        is_valid, expected, actual = invoker.verify_cephadm_hash(str(test_file))
        assert is_valid is False
        assert expected is None
        assert actual is None


class TestExecuteCephadm:
    """Tests for execute_cephadm function."""

    def test_successful_execution(self):
        """Test successful cephadm execution."""
        with mock.patch('subprocess.run') as mock_run:
            mock_run.return_value = mock.Mock(returncode=0)
            result = invoker.execute_cephadm('python3', '/tmp/cephadm.test', ['--help'])
            assert result == 0
            mock_run.assert_called_once()
            args = mock_run.call_args[0][0]
            assert args == ['python3', '/tmp/cephadm.test', '--help']

    def test_failed_execution(self):
        """Test failed cephadm execution."""
        with mock.patch('subprocess.run') as mock_run:
            mock_run.return_value = mock.Mock(returncode=1)
            result = invoker.execute_cephadm('python3', '/tmp/cephadm.test', ['invalid'])
            assert result == 1

    def test_execution_with_exception(self):
        """Test execution with exception."""
        with mock.patch('subprocess.run') as mock_run:
            mock_run.side_effect = OSError('Test error')
            result = invoker.execute_cephadm('python3', '/tmp/cephadm.test', ['--help'])
            assert result == 1


class TestExecuteDirectCommand:
    """Tests for execute_direct_command function."""

    def test_empty_args(self):
        """Test execution with empty args."""
        result = invoker.execute_direct_command([])
        assert result == 1

    def test_deployment_command_success(self):
        """Test successful deployment command execution."""
        with mock.patch('subprocess.run') as mock_run:
            mock_run.return_value = mock.Mock(returncode=0)
            result = invoker.execute_direct_command(['python3', '--version'])
            assert result == 0
            mock_run.assert_called_once()

    def test_deployment_command_failure(self):
        """Test failed deployment command execution."""
        with mock.patch('subprocess.run') as mock_run:
            mock_run.return_value = mock.Mock(returncode=1)
            result = invoker.execute_direct_command(['python3', 'nonexistent.py'])
            assert result == 1

    def test_non_deployment_command(self):
        """Test non-deployment command (should fail)."""
        result = invoker.execute_direct_command(['bash', '-c', 'echo test'])
        assert result == 1

    def test_deployment_command_with_exception(self):
        """Test deployment command with exception."""
        with mock.patch('subprocess.run') as mock_run:
            mock_run.side_effect = OSError('Test error')
            result = invoker.execute_direct_command(['python3', '--version'])
            assert result == 1


class TestMain:
    """Tests for main function."""

    def test_no_arguments(self, monkeypatch):
        """Test main with no arguments."""
        monkeypatch.setattr('sys.argv', ['invoker.py'])
        result = invoker.main()
        assert result == 1

    def test_insufficient_arguments(self, monkeypatch):
        """Test main with insufficient arguments."""
        monkeypatch.setattr('sys.argv', ['invoker.py', 'python3'])
        result = invoker.main()
        assert result == 1

    def test_exec_mode_no_command(self, monkeypatch):
        """Test --exec mode without command."""
        monkeypatch.setattr('sys.argv', ['invoker.py', '--exec'])
        result = invoker.main()
        assert result == 1

    def test_exec_mode_with_command(self, monkeypatch):
        """Test --exec mode with command."""
        with mock.patch('subprocess.run') as mock_run:
            mock_run.return_value = mock.Mock(returncode=0)
            monkeypatch.setattr('sys.argv', ['invoker.py', '--exec', 'python3', '--version'])
            result = invoker.main()
            assert result == 0

    def test_nonexistent_binary(self, monkeypatch, tmp_path):
        """Test with nonexistent cephadm binary."""
        nonexistent = tmp_path / 'nonexistent'
        monkeypatch.setattr('sys.argv', ['invoker.py', 'python3', str(nonexistent), 'ls'])
        result = invoker.main()
        assert result == 2

    def test_valid_hash_execution(self, monkeypatch, tmp_path):
        """Test execution with valid hash."""
        content = b'#!/usr/bin/env python3\nprint("test")\n'
        hash_value = hashlib.sha256(content).hexdigest()
        test_file = tmp_path / f'cephadm.{hash_value}'
        test_file.write_bytes(content)

        with mock.patch('subprocess.run') as mock_run:
            mock_run.return_value = mock.Mock(returncode=0)
            monkeypatch.setattr('sys.argv', ['invoker.py', 'python3', str(test_file), 'ls'])
            result = invoker.main()
            assert result == 0
            mock_run.assert_called_once()

    def test_invalid_hash_execution(self, monkeypatch, tmp_path):
        """Test execution with invalid hash."""
        content = b'#!/usr/bin/env python3\nprint("test")\n'
        wrong_hash = 'wronghash123'
        test_file = tmp_path / f'cephadm.{wrong_hash}'
        test_file.write_bytes(content)

        monkeypatch.setattr('sys.argv', ['invoker.py', 'python3', str(test_file), 'ls'])
        result = invoker.main()
        assert result == 2
        # Verify the file was deleted
        assert not test_file.exists()

    def test_no_hash_in_path_execution(self, monkeypatch, tmp_path):
        """Test execution with path that has no hash."""
        content = b'#!/usr/bin/env python3\nprint("test")\n'
        test_file = tmp_path / 'cephadm'
        test_file.write_bytes(content)

        with mock.patch('subprocess.run') as mock_run:
            mock_run.return_value = mock.Mock(returncode=0)
            monkeypatch.setattr('sys.argv', ['invoker.py', 'python3', str(test_file), 'ls'])
            result = invoker.main()
            assert result == 0

    def test_non_python_command_warning(self, monkeypatch, tmp_path):
        """Test warning when first argument is not python."""
        content = b'#!/usr/bin/env python3\nprint("test")\n'
        hash_value = hashlib.sha256(content).hexdigest()
        test_file = tmp_path / f'cephadm.{hash_value}'
        test_file.write_bytes(content)

        with mock.patch('subprocess.run') as mock_run:
            mock_run.return_value = mock.Mock(returncode=0)
            monkeypatch.setattr('sys.argv', ['invoker.py', 'bash', str(test_file), 'ls'])
            result = invoker.main()
            # Should still execute despite warning
            assert result == 0
