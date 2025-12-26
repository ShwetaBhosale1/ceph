# user_utils.py - user management utility functions

import logging
import os
import pwd
from typing import Tuple, Optional

from .call_wrappers import call, CallVerbosity
from .context import CephadmContext
from .exceptions import Error
from .exe_utils import find_program
from .ssh import authorize_ssh_key

logger = logging.getLogger()


def validate_user_exists(username: str) -> Tuple[int, int, str]:
    """Validate that a user exists and return their uid, gid, and home directory.
    Args:
        username: The username to validate
    Returns:
        Tuple of (uid, gid, home_directory)
    Raises:
        Error: If the user does not exist
    """
    try:
        pwd_entry = pwd.getpwnam(username)
        return pwd_entry.pw_uid, pwd_entry.pw_gid, pwd_entry.pw_dir
    except KeyError:
        raise Error(
            f'User {username} does not exist on this host. '
            f'Please create the user first: useradd -m -s /bin/bash {username}'
        )


def setup_sudoers(ctx: CephadmContext, username: str, sudoers_content: Optional[str] = None) -> None:
    """Setup sudoers for a user with custom or default permissions.
    """
    sudoers_file = f'/etc/sudoers.d/{username}'
    if sudoers_content is None:
        sudoers_content = f'{username} ALL=(ALL) NOPASSWD: ALL\n'

    # Ensure content ends with newline
    if not sudoers_content.endswith('\n'):
        sudoers_content += '\n'

    logger.info(f'Setting up sudoers for {username}...')
    try:
        # Write sudoers file with proper permissions
        with open(sudoers_file, 'w') as f:
            f.write(sudoers_content)
        os.chmod(sudoers_file, 0o440)
        os.chown(sudoers_file, 0, 0)

        # Validate sudoers syntax
        visudo_cmd = find_program('visudo')
        _out, _err, code = call(
            ctx,
            [visudo_cmd, '-c', '-f', sudoers_file],
            verbosity=CallVerbosity.DEBUG
        )
        if code != 0:
            # Clean up invalid file
            try:
                os.remove(sudoers_file)
            except OSError:
                pass
            raise Error(f'Invalid sudoers syntax: {_err}')
        logger.info(f'Successfully set up sudoers for {username}')
    except Error:
        raise
    except Exception as e:
        logger.exception(f'Failed to setup sudoers for {username}')
        raise Error(f'Failed to setup sudoers for {username}: {e}')


def setup_sudoers_restricted(ctx: CephadmContext, username: str, allowed_command: str) -> None:
    """Setup sudoers with restricted permissions for a specific command.
    Args:
        ctx: CephadmContext
        username: Username to configure sudoers for
        allowed_command: Full path to the command that user can run with sudo
    """
    sudoers_content = f'{username} ALL=(ALL) NOPASSWD: {allowed_command}'
    setup_sudoers(ctx, username, sudoers_content)


def setup_ssh_user(ctx: CephadmContext, username: str, ssh_pub_key: str) -> None:
    """Setup SSH user with passwordless sudo and SSH key authorization.
    It performs the following:
    1. Validates that the user exists
    2. Sets up passwordless sudo for the user (skipped for root)
    3. Authorizes the SSH public key for the user
    """
    if not ssh_pub_key or ssh_pub_key.isspace():
        raise Error('SSH public key is required and cannot be empty')

    logger.info(f'Setting up SSH user {username} on this host...')

    # Validate user exists (will raise Error if not)
    validate_user_exists(username)

    # Setup sudoers (skip for root user)
    if username != 'root':
        setup_sudoers(ctx, username)
    else:
        logger.info('Skipping sudoers setup for root user')

    # Setup SSH key using existing function from ssh.py
    try:
        authorize_ssh_key(ssh_pub_key, username)
        logger.info(f'Successfully copied SSH key to {username}')
    except Exception as e:
        logger.exception(f'Failed to copy SSH key for {username}')
        raise Error(f'Failed to copy SSH key for {username}: {e}')

    logger.info(f'Successfully set up SSH user {username} on this host')


def install_or_upgrade_cephadm(ctx: CephadmContext, requested_version: Optional[str] = None) -> Tuple[bool, str]:
    """Install or upgrade cephadm package to match a specific version.
    """
    import subprocess
    from .packagers import create_packager

    try:
        current_version = None
        needs_install = False

        # Check if cephadm is installed and get its version
        try:
            result = subprocess.run(['cephadm', 'version'],
                                    capture_output=True, text=True, timeout=5)
            if result.returncode == 0:
                current_version = result.stdout.strip()
                logger.info('Found installed cephadm: %s', current_version)

                # Check if we need to upgrade
                if requested_version:
                    if requested_version not in current_version:
                        logger.info('Version mismatch: installed=%s, requested=%s',
                                    current_version, requested_version)
                        needs_install = True
                    else:
                        logger.info(f'cephadm version {requested_version} already installed')
                        return True, f'cephadm {requested_version} already installed'
                else:
                    logger.info('cephadm installed, no specific version requested')
                    return True, f'cephadm already installed: {current_version}'
            else:
                logger.info('cephadm command failed, will install')
                needs_install = True
        except (subprocess.TimeoutExpired, FileNotFoundError) as e:
            logger.info(f'cephadm not found: {e}, will install')
            needs_install = True

        # Install or upgrade if needed
        if needs_install:
            logger.info('Installing/upgrading cephadm package...')
            pkg = create_packager(ctx, version=requested_version)
            pkg.install(['cephadm'])

            # Verify installation
            result = subprocess.run(['cephadm', 'version'],
                                    capture_output=True, text=True, timeout=5)
            if result.returncode == 0:
                new_version = result.stdout.strip()
                logger.info(f'Successfully installed cephadm: {new_version}')
                # Check if we got the requested version
                if requested_version and requested_version not in new_version:
                    logger.warning(f'Installed version {new_version} does not match requested {requested_version}')
                    return True, f'cephadm installed/upgraded: {new_version} (requested {requested_version} not available)'
                return True, f'cephadm installed/upgraded: {new_version}'
            else:
                logger.warning('cephadm package installed but version command failed')
                return True, 'cephadm package installed (version check unavailable)'

        # Should never reach here, but return success as package was installed
        return True, 'cephadm installed/upgraded successfully'

    except Exception as e:
        error_msg = f'Failed to install/upgrade cephadm: {e}'
        logger.exception(error_msg)
        return False, error_msg
