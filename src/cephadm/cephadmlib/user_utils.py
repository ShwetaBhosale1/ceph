# user_utils.py - user management utility functions

import logging
import os
import pwd
from typing import Tuple

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


def setup_sudoers(ctx: CephadmContext, username: str) -> None:
    """Setup passwordless sudo for a user.
    """
    sudoers_file = f'/etc/sudoers.d/{username}'
    sudoers_content = f'{username} ALL=(ALL) NOPASSWD: ALL\n'

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


def setup_ssh_user(ctx: CephadmContext, username: str, ssh_pub_key: str) -> None:
    """Setup SSH user with passwordless sudo and SSH key authorization.
    This function must be run as root. It performs the following:
    1. Validates that the user exists
    2. Sets up passwordless sudo for the user (skipped for root)
    3. Authorizes the SSH public key for the user
    """
    # Verify we're running as root
    if os.geteuid() != 0:
        raise Error('This operation must be run as root')

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
