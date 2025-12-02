#!/usr/bin/env python3
"""
Cephadm Invoker - A secure wrapper for executing cephadm commands

This script validates the cephadm binary hash before execution. If the hash
doesn't match, it restricts command execution:
  - Bootstrap commands (for creating/deploying cephadm binary) execute directly
  - All other commands use 'cephadm exec'

The invoker is designed to be called by the cephadm mgr module with the
cephadm binary path as the first argument:
    invoker.py <cephadm_binary_path> <cephadm_args...>
    invoker.py --exec <command> [args...]  (restricted mode)
"""

import hashlib
import os
import sys
import subprocess
from typing import List, Optional


# Allowed commands when cephadm binary hash doesn't match
# These commands are ONLY for deploying/creating the cephadm binary
# All other commands will use 'cephadm exec'
BOOTSTRAP_COMMANDS = {
    'python3',
    'python',
    'curl',
    'wget',
    'mkdir',
    'chmod',
    'chown',
    'mv',
    'cp',
    'touch',
    'tee',
    'which'
}


def calculate_hash(content: bytes) -> str:
    """Calculate SHA256 hash of binary content."""
    m = hashlib.sha256()
    m.update(content)
    return m.hexdigest()


def validate_cephadm_path(binary_path: str) -> bool:
    """
    Validate that the provided cephadm binary path exists and is readable.
    Returns True if valid, False otherwise.
    """
    try:
        return os.path.isfile(binary_path) and os.access(binary_path, os.R_OK)
    except (IOError, OSError):
        return False


def extract_hash_from_path(path: str) -> Optional[str]:
    """Extract the hash from cephadm binary path."""
    # Path format: /var/lib/ceph/{fsid}/cephadm.{hash}
    basename = os.path.basename(path)
    if basename.startswith('cephadm.') and len(basename) > 8:
        return basename[8:]  # Remove 'cephadm.' prefix
    return None


def verify_cephadm_hash(binary_path: str) -> bool:
    """
    Verify that the cephadm binary hash matches the hash in its path.
    Returns True if hash matches or if path doesn't contain hash.
    """
    try:
        # Read binary content
        with open(binary_path, 'rb') as f:
            content = f.read()
        
        # Calculate actual hash
        actual_hash = calculate_hash(content)
        
        # Extract expected hash from path
        expected_hash = extract_hash_from_path(binary_path)
        
        # If no hash in path, assume it's valid (e.g., /usr/sbin/cephadm)
        if expected_hash is None:
            return True
        
        # Compare hashes
        return actual_hash == expected_hash
    
    except (IOError, OSError) as e:
        print(f"Error reading cephadm binary: {e}", file=sys.stderr)
        return False


def is_bootstrap_command(command: List[str]) -> bool:
    """
    Check if command is a bootstrap command (for creating/deploying cephadm binary).
    Only these commands execute directly when hash verification fails.
    All other commands use 'cephadm exec'.
    """
    if not command:
        return False
    
    cmd_base = os.path.basename(command[0])
    return cmd_base in BOOTSTRAP_COMMANDS


def execute_cephadm(python_cmd: str, binary_path: str, args: List[str]) -> int:
    """Execute the cephadm binary with provided arguments."""
    try:
        # Execute as: python3 cephadm.{hash} <args>
        cmd = [python_cmd, binary_path] + args
        result = subprocess.run(cmd, env=os.environ.copy())
        return result.returncode
    except Exception as e:
        print(f"Error executing cephadm: {e}", file=sys.stderr)
        return 1


def execute_via_cephadm_exec(cephadm_path: str, args: List[str]) -> int:
    """
    Execute command via cephadm exec (when hash verification fails).
    This uses the cephadm binary's exec command to run the command.
    """
    try:
        # Build command: cephadm exec <command> [args...]
        cmd = [cephadm_path, 'exec'] + args
        result = subprocess.run(cmd, env=os.environ.copy())
        return result.returncode
    except Exception as e:
        print(f"Error executing via cephadm exec: {e}", file=sys.stderr)
        return 1


def execute_direct_command(args: List[str], cephadm_path: Optional[str] = None) -> int:
    """
    Execute command in restricted mode (when cephadm hash doesn't match).
    
    Logic:
    - If command is a bootstrap command (curl, wget, mkdir, etc.) → execute directly
    - Otherwise → use 'cephadm exec' (if cephadm binary is available)
    - If cephadm not available and not bootstrap → reject command
    
    This ensures only bootstrap commands execute directly; all other bash commands
    go through 'cephadm exec' for security.
    """
    if not args:
        print("Error: No command provided", file=sys.stderr)
        return 1
    
    # Check if this is a bootstrap command
    if is_bootstrap_command(args):
        # Bootstrap command - execute directly
        print(f"Note: Executing bootstrap command directly: {args[0]}", file=sys.stderr)
        try:
            result = subprocess.run(args, env=os.environ.copy())
            return result.returncode
        except Exception as e:
            print(f"Error executing bootstrap command: {e}", file=sys.stderr)
            return 1
    
    # Not a bootstrap command - try to use cephadm exec
    if cephadm_path and os.path.exists(cephadm_path):
        print(f"Note: Using 'cephadm exec' for command: {args[0]}", file=sys.stderr)
        return execute_via_cephadm_exec(cephadm_path, args)
    
    # No cephadm available and not a bootstrap command
    print(f"Error: Command '{args[0]}' is not a bootstrap command", file=sys.stderr)
    print("Bootstrap commands (execute directly): curl, wget, mkdir, chmod, chown, mv, cp, touch, tee, python3", file=sys.stderr)
    print("All other commands require 'cephadm exec' (binary not available)", file=sys.stderr)
    return 1


def main() -> int:
    """Main entry point for the invoker."""
    if len(sys.argv) < 2:
        print("Usage: invoker.py python3 <cephadm_binary_path> <cephadm_args...>", file=sys.stderr)
        print("   or: invoker.py --exec <command> [args...]  (restricted mode)", file=sys.stderr)
        print("", file=sys.stderr)
        print("Example: invoker.py python3 /var/lib/ceph/fsid/cephadm.hash ls", file=sys.stderr)
        return 1
    
    # Check if this is direct execution mode (restricted)
    if sys.argv[1] == '--exec':
        if len(sys.argv) < 3:
            print("Error: --exec requires a command", file=sys.stderr)
            return 1
        return execute_direct_command(sys.argv[2:])
    
    # First argument should be python3 (or python)
    # Second argument should be the cephadm binary path
    # Format: invoker.py python3 /var/lib/ceph/fsid/cephadm.hash <args>
    if len(sys.argv) < 3:
        print("Error: Expected format: invoker.py python3 <cephadm_binary_path> <args...>", file=sys.stderr)
        return 1
    
    python_cmd = sys.argv[1]
    cephadm_path = sys.argv[2]
    cephadm_args = sys.argv[3:] if len(sys.argv) > 3 else []
    
    # Validate python command
    if 'python' not in python_cmd.lower():
        print(f"Warning: Expected python3 as first argument, got: {python_cmd}", file=sys.stderr)
        print("Continuing anyway...", file=sys.stderr)
    
    # Validate the binary path exists
    if not validate_cephadm_path(cephadm_path):
        print(f"Error: cephadm binary not found or not readable at {cephadm_path}", file=sys.stderr)
        # Return exit code 2 to signal that cephadm binary doesn't exist
        return 2
    
    # Verify cephadm hash
    if not verify_cephadm_hash(cephadm_path):
        print(f"Warning: cephadm binary hash mismatch at {cephadm_path}", file=sys.stderr)
        print("Expected hash (from path): {0}".format(extract_hash_from_path(cephadm_path) or 'N/A'), file=sys.stderr)

        # Calculate and show actual hash for debugging
        try:
            with open(cephadm_path, 'rb') as f:
                actual = calculate_hash(f.read())
            print("Actual hash (calculated): {0}".format(actual), file=sys.stderr)
        except Exception:
            pass

        # Delete the corrupted/mismatched binary
        try:
            os.remove(cephadm_path)
            print(f"Deleted mismatched binary: {cephadm_path}", file=sys.stderr)
        except OSError as e:
            print(f"Warning: Could not delete mismatched binary: {e}", file=sys.stderr)

        # Return exit code 2 to signal manager to redeploy the binary
        print("Returning exit code 2 to trigger binary redeployment", file=sys.stderr)
        return 2
    
    # Hash matches - execute cephadm normally with python3
    return execute_cephadm(python_cmd, cephadm_path, cephadm_args)


if __name__ == '__main__':
    sys.exit(main())
