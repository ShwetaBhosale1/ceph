# -*- coding: utf-8 -*-
"""
Storage Provision API Module

This module provides APIs for provisioning storage services including
NFS exports over CephFS. Uses mgr.remote() calls for reliable operation.
Supports parallel execution of multiple configurations.
"""

import json
import logging
import time
import uuid
import concurrent.futures
from functools import partial
from threading import Lock
from typing import Any, Dict, List, Optional

import yaml  # type: ignore

from .. import mgr
from ..exceptions import DashboardException
from ..security import Scope
from ..services.cephfs import CephFS as CephFSService
from ..services.exception import serialize_dashboard_exception
from ..services.orchestrator import OrchClient
from . import (
    APIDoc,
    APIRouter,
    CreatePermission,
    Endpoint,
    EndpointDoc,
    ReadPermission,
    RESTController,
    Task,
)
from ._version import APIVersion

logger = logging.getLogger(__name__)


# Schema documentation for API docs
PROVISION_NFS_CEPHFS_SCHEMA = {
    'nfs_service': ({
        'service_id': (str, 'NFS service identifier'),
        'placement': ({
            'hosts': ([str], 'List of hosts'),
            'count': (int, 'Number of daemons', True),
            'labels': ([str], 'Host labels', True),
        }, 'Placement specification'),
        'port': (int, 'NFS port (default: 2049)', True),
    }, 'NFS service specification', True),
    'fs_volume': ({
        'name': (str, 'CephFS volume name'),
    }, 'CephFS volume specification', True),
    'fs_subvolume_groups': ([{
        'vol_name': (str, 'Parent volume name'),
        'group_name': (str, 'Subvolume group name'),
    }], 'List of subvolume groups to create', True),
    'fs_subvolumes': ([{
        'vol_name': (str, 'Parent volume name'),
        'sub_name': (str, 'Subvolume name'),
    }], 'List of subvolumes to create', True),
    'nfs_exports': ([{
        'cluster_id': (str, 'NFS cluster identifier'),
        'pseudo_path': (str, 'NFS v4 pseudo path'),
        'fsname': (str, 'CephFS volume name'),
    }], 'List of NFS exports to create', True),
}

PROVISION_RESULT_SCHEMA = {
    'success': (bool, 'Overall success status'),
    'results': ([{
        'operation': (str, 'Operation name'),
        'status': (str, 'success, failed, or skipped'),
        'message': (str, 'Result message'),
    }], 'List of operation results'),
}

BATCH_PROVISION_SCHEMA = {
    'configs': ([PROVISION_NFS_CEPHFS_SCHEMA], 'List of configurations'),
    'parallel': (bool, 'Execute configurations in parallel', True),
    'max_workers': (int, 'Max parallel workers (default: 4)', True),
}

BATCH_PROVISION_RESULT_SCHEMA = {
    'success': (bool, 'Overall success status'),
    'total': (int, 'Total number of configurations'),
    'succeeded': (int, 'Number of successful configurations'),
    'failed': (int, 'Number of failed configurations'),
    'config_results': ([{
        'config_id': (str, 'Configuration identifier'),
        'config_name': (str, 'Config name (from service_id or vol)'),
        'success': (bool, 'Success status for this configuration'),
        'results': ([{
            'operation': (str, 'Operation name'),
            'status': (str, 'success, failed, or skipped'),
            'message': (str, 'Result message'),
        }], 'List of operation results'),
    }], 'Results per configuration'),
}


def provision_task(name, metadata, wait_for=10.0):
    """Task decorator for provision operations."""
    return Task(
        "storage_provision/{}".format(name),
        metadata,
        wait_for,
        partial(serialize_dashboard_exception, include_http_status=True)
    )


class ProvisionResult:
    """Helper class to track provision operation results."""

    def __init__(
        self,
        config_id: Optional[str] = None,
        config_name: Optional[str] = None
    ):
        self.config_id = config_id or str(uuid.uuid4())[:8]
        self.config_name = config_name or 'unnamed'
        self.results: List[Dict[str, Any]] = []
        self._has_error = False
        self._lock = Lock()

    def add_success(
        self,
        operation: str,
        message: str,
        details: Optional[Dict] = None
    ):
        with self._lock:
            self.results.append({
                'operation': operation,
                'status': 'success',
                'message': message,
                'details': details or {}
            })

    def add_failure(
        self,
        operation: str,
        message: str,
        details: Optional[Dict] = None
    ):
        with self._lock:
            self._has_error = True
            self.results.append({
                'operation': operation,
                'status': 'failed',
                'message': message,
                'details': details or {}
            })

    def add_skipped(self, operation: str, message: str):
        with self._lock:
            self.results.append({
                'operation': operation,
                'status': 'skipped',
                'message': message,
                'details': {}
            })

    @property
    def has_error(self) -> bool:
        return self._has_error

    def to_dict(self) -> Dict[str, Any]:
        return {
            'success': not self._has_error,
            'results': self.results
        }

    def to_batch_dict(self) -> Dict[str, Any]:
        """Return dict format suitable for batch results."""
        return {
            'config_id': self.config_id,
            'config_name': self.config_name,
            'success': not self._has_error,
            'results': self.results
        }


@APIRouter('/storage-provision', Scope.CEPHFS)
@APIDoc("Storage Provision API", "StorageProvision")
class StorageProvision(RESTController):
    """
    Storage Provision API for provisioning storage services.

    This controller provides endpoints for provisioning complete storage
    configurations including NFS services, CephFS volumes, subvolumes,
    and NFS exports in a single operation.

    Uses mgr.remote() calls directly to avoid REST framework issues.
    """

    @Endpoint('POST', path='/nfs-cephfs')
    @CreatePermission
    @EndpointDoc(
        "Provision NFS export over CephFS (synchronous)",
        parameters={'config': (dict, 'Provision configuration')},
        responses={200: PROVISION_RESULT_SCHEMA}
    )
    @RESTController.MethodMap(version=APIVersion(1, 0))  # type: ignore
    def provision_nfs_cephfs(self, config: Dict[str, Any]) -> Dict[str, Any]:
        """
        Provision NFS export over CephFS (synchronous).

        Operations executed in sequence (skipping any not specified):
        1. Apply NFS service specification
        2. Create CephFS volume
        3. Create subvolume groups
        4. Create subvolumes
        5. Wait for NFS service (if created and exports specified)
        6. Create NFS exports

        :param config: Configuration dictionary
        :return: Dictionary with success status and operation results
        """
        result = self._execute_single_config(config)
        return result.to_dict()

    @Endpoint('POST', path='/nfs-cephfs/async')
    @CreatePermission
    @EndpointDoc(
        "Provision NFS export over CephFS (async/background task)",
        parameters={'config': (dict, 'Provision configuration')},
        responses={
            200: PROVISION_RESULT_SCHEMA,
            202: {'name': (str, 'Task name'), 'metadata': (dict, 'Task info')}
        }
    )
    @RESTController.MethodMap(version=APIVersion(1, 0))  # type: ignore
    @provision_task('nfs_cephfs', {}, wait_for=5.0)
    def provision_nfs_cephfs_async(
        self,
        config: Dict[str, Any]
    ) -> Dict[str, Any]:
        """
        Provision NFS export over CephFS as a background task.

        Returns immediately with task info if operation takes > 5 seconds.
        Check task status via GET /api/task?name=storage_provision/nfs_cephfs

        :param config: Configuration dictionary
        :return: Dictionary with success status and operation results
        """
        result = self._execute_single_config(config)
        return result.to_dict()

    @Endpoint('POST', path='/nfs-cephfs/batch')
    @CreatePermission
    @EndpointDoc(
        "Provision multiple NFS/CephFS configurations (synchronous)",
        parameters={
            'configs': ([dict], 'List of provision configurations'),
            'parallel': (bool, 'Execute in parallel (default: true)'),
            'max_workers': (int, 'Max parallel workers (default: 4)')
        },
        responses={200: BATCH_PROVISION_RESULT_SCHEMA}
    )
    @RESTController.MethodMap(version=APIVersion(1, 0))  # type: ignore
    def provision_nfs_cephfs_batch(
        self,
        configs: List[Dict[str, Any]],
        parallel: bool = True,
        max_workers: int = 4
    ) -> Dict[str, Any]:
        """
        Provision multiple NFS/CephFS configurations (synchronous).

        Each configuration is executed independently. With parallel=True,
        all configurations are processed concurrently using a thread pool.

        :param configs: List of provision configurations
        :param parallel: Execute configurations in parallel (default: True)
        :param max_workers: Maximum number of parallel workers (default: 4)
        :return: Dictionary with batch results
        """
        return self._execute_batch(configs, parallel, max_workers)

    @Endpoint('POST', path='/nfs-cephfs/batch/async')
    @CreatePermission
    @EndpointDoc(
        "Provision multiple NFS/CephFS configurations (async/background)",
        parameters={
            'configs': ([dict], 'List of provision configurations'),
            'parallel': (bool, 'Execute in parallel (default: true)'),
            'max_workers': (int, 'Max parallel workers (default: 4)')
        },
        responses={
            200: BATCH_PROVISION_RESULT_SCHEMA,
            202: {'name': (str, 'Task name'), 'metadata': (dict, 'Task info')}
        }
    )
    @RESTController.MethodMap(version=APIVersion(1, 0))  # type: ignore
    @provision_task('nfs_cephfs_batch', {}, wait_for=5.0)
    def provision_nfs_cephfs_batch_async(
        self,
        configs: List[Dict[str, Any]],
        parallel: bool = True,
        max_workers: int = 4
    ) -> Dict[str, Any]:
        """
        Provision multiple NFS/CephFS configurations as background task.

        Returns immediately with task info if operation takes > 5 seconds.
        Check status: GET /api/task?name=storage_provision/nfs_cephfs_batch

        :param configs: List of provision configurations
        :param parallel: Execute configurations in parallel (default: True)
        :param max_workers: Maximum number of parallel workers (default: 4)
        :return: Dictionary with batch results
        """
        return self._execute_batch(configs, parallel, max_workers)

    def _execute_batch(
        self,
        configs: List[Dict[str, Any]],
        parallel: bool = True,
        max_workers: int = 4
    ) -> Dict[str, Any]:
        """
        Internal method to execute batch configurations.

        :param configs: List of provision configurations
        :param parallel: Execute in parallel
        :param max_workers: Max parallel workers
        :return: Batch result dictionary
        """
        if not configs:
            return {
                'success': True,
                'total': 0,
                'succeeded': 0,
                'failed': 0,
                'config_results': []
            }

        config_results: List[Dict[str, Any]] = []

        if parallel and len(configs) > 1:
            # Execute configurations in parallel
            executor = concurrent.futures.ThreadPoolExecutor(
                max_workers=max_workers
            )
            with executor:
                # Submit all configurations
                future_to_idx = {
                    executor.submit(
                        self._execute_single_config, cfg, idx
                    ): idx
                    for idx, cfg in enumerate(configs)
                }

                # Collect results as they complete
                results_by_idx: Dict[int, ProvisionResult] = {}
                for future in concurrent.futures.as_completed(future_to_idx):
                    idx = future_to_idx[future]
                    try:
                        result = future.result()
                        results_by_idx[idx] = result
                    except Exception as e:  # pylint: disable=broad-except
                        # Create error result for failed execution
                        err_result = ProvisionResult(
                            config_id=str(idx),
                            config_name='config_{}'.format(idx)
                        )
                        err_result.add_failure(
                            'execution',
                            'Configuration execution failed: {}'.format(
                                str(e)
                            ),
                            {'error': str(e)}
                        )
                        results_by_idx[idx] = err_result
                        logger.exception(
                            'Configuration %d execution failed', idx
                        )

                # Sort results by original index
                for idx in sorted(results_by_idx.keys()):
                    config_results.append(results_by_idx[idx].to_batch_dict())
        else:
            # Execute configurations sequentially
            for idx, cfg in enumerate(configs):
                try:
                    result = self._execute_single_config(cfg, idx)
                    config_results.append(result.to_batch_dict())
                except Exception as e:  # pylint: disable=broad-except
                    err_result = ProvisionResult(
                        config_id=str(idx),
                        config_name='config_{}'.format(idx)
                    )
                    err_result.add_failure(
                        'execution',
                        'Configuration execution failed: {}'.format(str(e)),
                        {'error': str(e)}
                    )
                    config_results.append(err_result.to_batch_dict())
                    logger.exception('Configuration %d execution failed', idx)

        # Calculate summary
        succeeded = sum(1 for r in config_results if r['success'])
        failed = len(config_results) - succeeded

        return {
            'success': failed == 0,
            'total': len(configs),
            'succeeded': succeeded,
            'failed': failed,
            'config_results': config_results
        }

    def _execute_single_config(
        self,
        config: Dict[str, Any],
        index: int = 0
    ) -> ProvisionResult:
        """
        Execute a single provision configuration.

        :param config: Configuration dictionary
        :param index: Configuration index (for identification)
        :return: ProvisionResult with operation results
        """
        # Extract configuration from the wrapper if present
        if 'configure_nfs_cephfs_export' in config:
            config = config['configure_nfs_cephfs_export']

        # Determine config name from service_id or volume name
        config_name = 'config_{}'.format(index)
        nfs_service_config = config.get('nfs_service')
        if nfs_service_config and nfs_service_config.get('service_id'):
            config_name = 'nfs_{}'.format(nfs_service_config['service_id'])
        elif config.get('fs_volume', {}).get('name'):
            config_name = 'fs_{}'.format(config['fs_volume']['name'])

        result = ProvisionResult(
            config_id=str(index),
            config_name=config_name
        )

        # Step 1: Apply NFS Service Specification
        self._apply_nfs_service(nfs_service_config, result)

        # Step 2: Create CephFS Volume
        self._create_fs_volume(config.get('fs_volume'), result)

        # Step 3: Create Subvolume Groups
        subvol_groups = config.get('fs_subvolume_groups')
        self._create_subvolume_groups(subvol_groups, result)

        # Step 4: Create Subvolumes
        self._create_subvolumes(config.get('fs_subvolumes'), result)

        # Step 5: Wait for NFS service if we created one and have exports
        nfs_exports = config.get('nfs_exports')
        if nfs_service_config and nfs_exports:
            service_id = nfs_service_config.get('service_id')
            if service_id:
                self._wait_for_nfs_service(service_id, result)

        # Step 6: Create NFS Exports
        self._create_nfs_exports(nfs_exports, result)

        return result

    @Endpoint('POST', path='/nfs-cephfs/yaml')
    @CreatePermission
    @EndpointDoc(
        "Provision NFS export over CephFS from YAML",
        parameters={'yaml_config': (str, 'YAML configuration string')},
        responses={200: PROVISION_RESULT_SCHEMA}
    )
    @RESTController.MethodMap(version=APIVersion(1, 0))  # type: ignore
    def provision_nfs_cephfs_yaml(self, yaml_config: str) -> Dict[str, Any]:
        """
        Provision NFS export over CephFS from YAML configuration.

        Supports both single configuration and batch (list) configurations.
        If a list is provided, executes all configurations in parallel.

        :param yaml_config: YAML configuration string
        :return: Dictionary with success status and operation results
        """
        try:
            if not yaml_config:
                raise DashboardException(
                    msg='yaml_config parameter is required',
                    component='storage_provision'
                )

            logger.info('Received YAML config of length %d', len(yaml_config))

            try:
                config = yaml.safe_load(yaml_config)
            except yaml.YAMLError as e:
                raise DashboardException(
                    msg='Invalid YAML configuration: {}'.format(str(e)),
                    component='storage_provision'
                ) from e

            if config is None:
                raise DashboardException(
                    msg='YAML configuration is empty or invalid',
                    component='storage_provision'
                )

            logger.info('Parsed YAML config type: %s', type(config).__name__)

            # Check if it's a list of configurations
            if isinstance(config, list):
                # Execute batch directly to avoid endpoint parameter issues
                return self._execute_batch(
                    configs=config,
                    parallel=True,
                    max_workers=4
                )

            # Check if 'configs' key is present (batch format)
            if isinstance(config, dict) and 'configs' in config:
                return self._execute_batch(
                    configs=config['configs'],
                    parallel=config.get('parallel', True),
                    max_workers=config.get('max_workers', 4)
                )

            # Single configuration - call internal method directly
            result = self._execute_single_config(config)
            return result.to_dict()

        except DashboardException:
            raise
        except Exception as e:
            logger.exception('Unexpected error in provision_nfs_cephfs_yaml')
            raise DashboardException(
                msg='Unexpected error: {}'.format(str(e)),
                component='storage_provision'
            ) from e

    @Endpoint('POST', path='/nfs-cephfs/yaml/async')
    @CreatePermission
    @EndpointDoc(
        "Provision NFS/CephFS from YAML (async/background task)",
        parameters={'yaml_config': (str, 'YAML configuration string')},
        responses={
            200: PROVISION_RESULT_SCHEMA,
            202: {'name': (str, 'Task name'), 'metadata': (dict, 'Task info')}
        }
    )
    @RESTController.MethodMap(version=APIVersion(1, 0))  # type: ignore
    @provision_task('nfs_cephfs_yaml', {}, wait_for=5.0)
    def provision_nfs_cephfs_yaml_async(
        self,
        yaml_config: str
    ) -> Dict[str, Any]:
        """
        Provision NFS/CephFS from YAML as a background task.

        Returns immediately with task info if operation takes > 5 seconds.
        Check status: GET /api/task?name=storage_provision/nfs_cephfs_yaml

        :param yaml_config: YAML configuration string
        :return: Dictionary with success status and operation results
        """
        # Parse YAML
        if not yaml_config:
            raise DashboardException(
                msg='yaml_config parameter is required',
                component='storage_provision'
            )

        try:
            config = yaml.safe_load(yaml_config)
        except yaml.YAMLError as e:
            raise DashboardException(
                msg='Invalid YAML configuration: {}'.format(str(e)),
                component='storage_provision'
            ) from e

        if config is None:
            raise DashboardException(
                msg='YAML configuration is empty or invalid',
                component='storage_provision'
            )

        # Check if it's a list of configurations
        if isinstance(config, list):
            return self._execute_batch(
                configs=config, parallel=True, max_workers=4
            )

        # Check if 'configs' key is present (batch format)
        if isinstance(config, dict) and 'configs' in config:
            return self._execute_batch(
                configs=config['configs'],
                parallel=config.get('parallel', True),
                max_workers=config.get('max_workers', 4)
            )

        # Single configuration
        result = self._execute_single_config(config)
        return result.to_dict()

    @Endpoint('POST', path='/validate')
    @ReadPermission
    @EndpointDoc(
        "Validate provision configuration",
        parameters={'config': (dict, 'Provision configuration')},
        responses={200: {'valid': (bool, ''), 'errors': ([str], '')}}
    )
    def validate_config(self, config: Dict[str, Any]) -> Dict[str, Any]:
        """
        Validate provision configuration without executing.

        :param config: Configuration dictionary to validate
        :return: Dictionary with validation result and any errors
        """
        errors = []

        # Extract configuration from the wrapper if present
        if 'configure_nfs_cephfs_export' in config:
            config = config['configure_nfs_cephfs_export']

        # Validate NFS service config
        nfs_service = config.get('nfs_service')
        if nfs_service:
            if not nfs_service.get('service_id'):
                errors.append('nfs_service.service_id is required')

        # Validate fs_volume config
        fs_volume = config.get('fs_volume')
        if fs_volume:
            if not fs_volume.get('name'):
                errors.append('fs_volume.name is required')

        # Validate subvolume groups
        subvol_groups = config.get('fs_subvolume_groups', [])
        for i, group in enumerate(subvol_groups):
            if not group.get('vol_name'):
                errors.append(
                    'fs_subvolume_groups[{}].vol_name is required'.format(i)
                )
            if not group.get('group_name'):
                errors.append(
                    'fs_subvolume_groups[{}].group_name is required'.format(i)
                )

        # Validate subvolumes
        subvols = config.get('fs_subvolumes', [])
        for i, subvol in enumerate(subvols):
            if not subvol.get('vol_name'):
                errors.append(
                    'fs_subvolumes[{}].vol_name is required'.format(i)
                )
            if not subvol.get('sub_name'):
                errors.append(
                    'fs_subvolumes[{}].sub_name is required'.format(i)
                )

        # Validate NFS exports
        exports = config.get('nfs_exports', [])
        for i, export in enumerate(exports):
            if not export.get('cluster_id'):
                errors.append(
                    'nfs_exports[{}].cluster_id is required'.format(i)
                )
            if not export.get('pseudo_path'):
                errors.append(
                    'nfs_exports[{}].pseudo_path is required'.format(i)
                )
            if not export.get('fsname'):
                errors.append(
                    'nfs_exports[{}].fsname is required'.format(i)
                )

        return {
            'valid': len(errors) == 0,
            'errors': errors
        }

    def _apply_nfs_service(
        self,
        nfs_service: Optional[Dict],
        result: ProvisionResult
    ):
        """Create NFS service using orchestrator Service API."""
        if not nfs_service:
            result.add_skipped(
                'apply_nfs_service',
                'nfs_service not specified in config'
            )
            return

        try:
            service_id = nfs_service.get('service_id')
            if not service_id:
                result.add_failure(
                    'apply_nfs_service',
                    'service_id is required for NFS service'
                )
                return

            orch = OrchClient.instance()

            # Validate placement hosts exist in cluster
            placement = nfs_service.get('placement')
            if placement and isinstance(placement, dict):
                hosts = placement.get('hosts', [])
                if hosts:
                    # Get list of actual hosts in cluster
                    all_hosts = orch.hosts.list()
                    cluster_hosts = [h.hostname for h in all_hosts]
                    invalid = [h for h in hosts if h not in cluster_hosts]
                    if invalid:
                        msg = 'Invalid hosts: {}. Available: {}'.format(
                            invalid, cluster_hosts
                        )
                        result.add_failure(
                            'apply_nfs_service', msg,
                            {
                                'invalid_hosts': invalid,
                                'available_hosts': cluster_hosts
                            }
                        )
                        return

            # Build service spec for orchestrator
            # This matches NFSServiceSpec structure
            service_spec: Dict[str, Any] = {
                'service_type': 'nfs',
                'service_id': service_id,
                'unmanaged': False,
            }

            # Add placement if specified
            if placement:
                service_spec['placement'] = placement
            else:
                # Default: place on 1 host
                service_spec['placement'] = {'count': 1}

            # Add port at top level (NFSServiceSpec expects it there)
            port = nfs_service.get('port')
            if port is not None:
                service_spec['port'] = port

            # Add other optional fields
            for field in ['networks', 'virtual_ip']:
                if field in nfs_service and nfs_service[field]:
                    service_spec[field] = nfs_service[field]

            # Apply service via OrchClient
            apply_result = orch.services.apply(
                service_spec, no_overwrite=False
            )
            logger.info('NFS service apply result: %s', apply_result)

            svc_name = 'nfs.{}'.format(service_id)
            note_msg = (
                'NFS daemon may take a few minutes to start. '
                'NFS exports can only be created after daemon is running.'
            )
            result.add_success(
                'apply_nfs_service',
                'NFS service {} scheduled for deployment'.format(svc_name),
                {'service_id': service_id, 'note': note_msg}
            )
            logger.info('Scheduled NFS service: %s', svc_name)

        except Exception as e:  # pylint: disable=broad-except
            result.add_failure(
                'apply_nfs_service',
                'Failed to create NFS service: {}'.format(str(e)),
                {'error': str(e)}
            )
            logger.exception('Failed to create NFS service')

    def _wait_for_nfs_service(
        self,
        service_id: str,
        result: ProvisionResult,
        timeout: int = 300,
        poll_interval: int = 5
    ):
        """
        Wait for NFS service to come online.

        :param service_id: NFS service ID (cluster ID)
        :param result: ProvisionResult to record status
        :param timeout: Maximum time to wait in seconds (default: 300)
        :param poll_interval: Time between checks in seconds (default: 5)
        """
        service_name = 'nfs.{}'.format(service_id)
        logger.info(
            'Waiting for NFS service %s to come online...', service_name
        )

        start_time = time.time()
        last_status = 'unknown'

        while True:
            elapsed = time.time() - start_time
            if elapsed >= timeout:
                result.add_failure(
                    'wait_nfs_service',
                    'Timeout waiting for NFS service {}'.format(service_name),
                    {
                        'service_id': service_id,
                        'timeout': timeout,
                        'last_status': last_status
                    }
                )
                logger.error(
                    'Timeout waiting for NFS service %s after %ds',
                    service_name, timeout
                )
                return

            try:
                orch = OrchClient.instance()
                services, _ = orch.services.list(
                    service_type='nfs',
                    service_name=service_name
                )

                if services:
                    svc = services[0]
                    running = svc.get('status', {}).get('running', 0)
                    size = svc.get('status', {}).get('size', 0)
                    last_status = 'running={}/{}'.format(running, size)

                    if running > 0:
                        # Also verify NFS cluster is accessible
                        try:
                            clusters = mgr.remote('nfs', 'cluster_ls')
                            if service_id in clusters:
                                result.add_success(
                                    'wait_nfs_service',
                                    'NFS service {} is online'.format(
                                        service_name
                                    ),
                                    {
                                        'service_id': service_id,
                                        'running': running,
                                        'wait_time': int(elapsed)
                                    }
                                )
                                logger.info(
                                    'NFS service %s is online after %ds',
                                    service_name, int(elapsed)
                                )
                                return
                        except Exception:  # pylint: disable=broad-except
                            pass  # NFS module not ready yet

                logger.debug(
                    'NFS service %s not ready yet (%s), waiting...',
                    service_name, last_status
                )

            except Exception as ex:  # pylint: disable=broad-except
                logger.debug(
                    'Error checking NFS service status: %s', str(ex)
                )
                last_status = 'error: {}'.format(str(ex))

            time.sleep(poll_interval)

    def _create_fs_volume(
        self,
        fs_volume: Optional[Dict],
        result: ProvisionResult
    ):
        """Create CephFS volume using CephFS controller."""
        if not fs_volume:
            result.add_skipped(
                'create_fs_volume',
                'fs_volume not specified in config'
            )
            return

        try:
            name = fs_volume.get('name')
            if not name:
                result.add_failure(
                    'create_fs_volume',
                    'name is required for fs_volume'
                )
                return

            # Build placement string for MDS
            placement_str = '1'  # Default MDS count
            placement = fs_volume.get('placement')
            if placement:
                if isinstance(placement, dict):
                    if 'hosts' in placement:
                        hosts = placement['hosts']
                        placement_str = '1 ' + ' '.join(hosts)
                    elif 'labels' in placement:
                        labels = placement['labels']
                        label_str = ','.join(
                            ['label:{}'.format(lbl) for lbl in labels]
                        )
                        placement_str = '1 {}'.format(label_str)

            # Use mgr.remote() to call volumes module directly
            params: Dict[str, Any] = {
                'name': name,
                'placement': placement_str,
            }
            if fs_volume.get('data_pool'):
                params['data_pool'] = fs_volume['data_pool']
            if fs_volume.get('metadata_pool'):
                params['meta_pool'] = fs_volume['metadata_pool']

            error_code, _, err = mgr.remote(
                'volumes',
                '_cmd_fs_volume_create',
                None,
                params
            )

            if error_code != 0:
                result.add_failure(
                    'create_fs_volume',
                    'Failed to create volume {}: {}'.format(name, err),
                    {'error_code': error_code, 'error': err}
                )
            else:
                result.add_success(
                    'create_fs_volume',
                    'CephFS volume {} created successfully'.format(name),
                    {'volume_name': name}
                )
                logger.info('Created CephFS volume: %s', name)

        except Exception as e:  # pylint: disable=broad-except
            result.add_failure(
                'create_fs_volume',
                'Failed to create CephFS volume: {}'.format(str(e)),
                {'error': str(e)}
            )
            logger.exception('Failed to create CephFS volume')

    def _set_quota(
        self,
        vol_name: str,
        path: str,
        quota_config: Dict,
        result: ProvisionResult,
        operation_name: str
    ):
        """Set quota on a path using CephFS service."""
        try:
            max_bytes = quota_config.get('max_bytes')
            max_files = quota_config.get('max_files')

            if max_bytes is None and max_files is None:
                return  # No quota to set

            # Use CephFS service to set quotas
            cfs = CephFSService(vol_name)
            cfs.set_quotas(path, max_bytes, max_files)

            result.add_success(
                '{}_quota'.format(operation_name),
                'Quota set on {}'.format(path),
                {
                    'path': path,
                    'max_bytes': max_bytes,
                    'max_files': max_files
                }
            )
            logger.info('Set quota on %s: max_bytes=%s, max_files=%s',
                        path, max_bytes, max_files)

        except Exception as e:  # pylint: disable=broad-except
            result.add_failure(
                '{}_quota'.format(operation_name),
                'Failed to set quota: {}'.format(str(e)),
                {'error': str(e)}
            )
            logger.exception('Failed to set quota on %s', path)

    def _create_subvolume_groups(
        self,
        subvolume_groups: Optional[List[Dict]],
        result: ProvisionResult
    ):
        """Create CephFS subvolume groups using mgr.remote()."""
        if not subvolume_groups:
            result.add_skipped(
                'create_subvolume_groups',
                'fs_subvolume_groups not specified in config'
            )
            return

        for i, group_config in enumerate(subvolume_groups):
            try:
                vol_name = group_config.get('vol_name')
                group_name = group_config.get('group_name')

                if not vol_name or not group_name:
                    result.add_failure(
                        'create_subvolume_group[{}]'.format(i),
                        'vol_name and group_name are required',
                        {'config': group_config}
                    )
                    continue

                # Build params for mgr.remote() call
                params: Dict[str, Any] = {
                    'vol_name': vol_name,
                    'group_name': group_name,
                }

                # Add optional parameters
                opt_fields = ['pool_layout', 'uid', 'gid', 'mode']
                for field in opt_fields:
                    val = group_config.get(field)
                    if val is not None:
                        params[field] = val

                # Handle size from quota.max_bytes for creation
                quota_config = group_config.get('quota', {})
                if 'size' in group_config:
                    params['size'] = group_config['size']
                elif isinstance(quota_config, dict):
                    max_bytes = quota_config.get('max_bytes')
                    if max_bytes is not None:
                        params['size'] = max_bytes

                # Use mgr.remote() to create subvolume group
                error_code, _, err = mgr.remote(
                    'volumes',
                    '_cmd_fs_subvolumegroup_create',
                    None,
                    params
                )

                if error_code != 0:
                    result.add_failure(
                        'create_subvolume_group[{}]'.format(i),
                        'Failed to create subvolume group {}: {}'.format(
                            group_name, err
                        ),
                        {'error': err, 'config': group_config}
                    )
                    continue

                result.add_success(
                    'create_subvolume_group[{}]'.format(i),
                    'Subvolume group {} created in {}'.format(
                        group_name, vol_name
                    ),
                    {'vol_name': vol_name, 'group_name': group_name}
                )
                logger.info(
                    'Created subvolume group: %s/%s', vol_name, group_name
                )

                # Set quota if max_files is specified
                if isinstance(quota_config, dict):
                    max_files = quota_config.get('max_files')
                    max_bytes = quota_config.get('max_bytes')
                    if max_files is not None or max_bytes is not None:
                        # Get path from subvolume group
                        err_code, out, err = mgr.remote(
                            'volumes',
                            '_cmd_fs_subvolumegroup_getpath',
                            None,
                            {'vol_name': vol_name, 'group_name': group_name}
                        )
                        if err_code == 0 and out:
                            group_path = out.strip()
                            quota_cfg = {
                                'max_files': max_files,
                                'max_bytes': max_bytes
                            }
                            self._set_quota(
                                vol_name=vol_name,
                                path=group_path,
                                quota_config=quota_cfg,
                                result=result,
                                operation_name='subvolume_group[{}]'.format(i)
                            )

            except Exception as e:  # pylint: disable=broad-except
                result.add_failure(
                    'create_subvolume_group[{}]'.format(i),
                    'Failed to create subvolume group: {}'.format(str(e)),
                    {'error': str(e), 'config': group_config}
                )
                logger.exception('Failed to create subvolume group %d', i)

    def _create_subvolumes(
        self,
        subvolumes: Optional[List[Dict]],
        result: ProvisionResult
    ):
        """Create CephFS subvolumes using mgr.remote()."""
        if not subvolumes:
            result.add_skipped(
                'create_subvolumes',
                'fs_subvolumes not specified in config'
            )
            return

        for i, subvol_config in enumerate(subvolumes):
            try:
                vol_name = subvol_config.get('vol_name')
                sub_name = subvol_config.get('sub_name')

                if not vol_name or not sub_name:
                    result.add_failure(
                        'create_subvolume[{}]'.format(i),
                        'vol_name and sub_name are required',
                        {'config': subvol_config}
                    )
                    continue

                # Build params for mgr.remote() call
                params: Dict[str, Any] = {
                    'vol_name': vol_name,
                    'sub_name': sub_name,
                }

                # Add optional parameters
                optional_fields = [
                    'group_name', 'pool_layout', 'uid', 'gid', 'mode',
                    'namespace_isolated', 'earmark'
                ]
                for field in optional_fields:
                    val = subvol_config.get(field)
                    if val is not None:
                        params[field] = val

                # Handle size from quota.max_bytes for creation
                quota_config = subvol_config.get('quota', {})
                if 'size' in subvol_config:
                    params['size'] = subvol_config['size']
                elif isinstance(quota_config, dict):
                    max_bytes = quota_config.get('max_bytes')
                    if max_bytes is not None:
                        params['size'] = max_bytes

                # Use mgr.remote() to create subvolume
                error_code, _, err = mgr.remote(
                    'volumes',
                    '_cmd_fs_subvolume_create',
                    None,
                    params
                )

                if error_code != 0:
                    result.add_failure(
                        'create_subvolume[{}]'.format(i),
                        'Failed to create subvolume {}: {}'.format(
                            sub_name, err
                        ),
                        {'error': err, 'config': subvol_config}
                    )
                    continue

                result.add_success(
                    'create_subvolume[{}]'.format(i),
                    'Subvolume {} created in {}'.format(sub_name, vol_name),
                    {
                        'vol_name': vol_name,
                        'sub_name': sub_name,
                        'group_name': subvol_config.get('group_name', '')
                    }
                )
                logger.info('Created subvolume: %s/%s', vol_name, sub_name)

                # Set quota if max_files is specified
                if isinstance(quota_config, dict):
                    max_files = quota_config.get('max_files')
                    max_bytes = quota_config.get('max_bytes')
                    if max_files is not None or max_bytes is not None:
                        # Get path from subvolume info
                        group_name = subvol_config.get('group_name', '')
                        info_params: Dict[str, Any] = {
                            'vol_name': vol_name,
                            'sub_name': sub_name
                        }
                        if group_name:
                            info_params['group_name'] = group_name

                        err_code, out, _ = mgr.remote(
                            'volumes',
                            '_cmd_fs_subvolume_info',
                            None,
                            info_params
                        )
                        if err_code == 0 and out:
                            subvol_info = json.loads(out)
                            subvol_path = subvol_info.get('path', '').strip()
                            if subvol_path:
                                quota_cfg = {
                                    'max_files': max_files,
                                    'max_bytes': max_bytes
                                }
                                self._set_quota(
                                    vol_name=vol_name,
                                    path=subvol_path,
                                    quota_config=quota_cfg,
                                    result=result,
                                    operation_name='subvolume[{}]'.format(i)
                                )

            except Exception as e:  # pylint: disable=broad-except
                result.add_failure(
                    'create_subvolume[{}]'.format(i),
                    'Failed to create subvolume: {}'.format(str(e)),
                    {'error': str(e), 'config': subvol_config}
                )
                logger.exception('Failed to create subvolume %d', i)

    def _create_nfs_exports(
        self,
        nfs_exports: Optional[List[Dict]],
        result: ProvisionResult
    ):
        """Create NFS exports using mgr.remote()."""
        if not nfs_exports:
            result.add_skipped(
                'create_nfs_exports',
                'nfs_exports not specified in config'
            )
            return

        for i, export_config in enumerate(nfs_exports):
            try:
                cluster_id = export_config.get('cluster_id')
                pseudo_path = export_config.get('pseudo_path')
                fsname = export_config.get('fsname')

                if not cluster_id or not pseudo_path or not fsname:
                    result.add_failure(
                        'create_nfs_export[{}]'.format(i),
                        'cluster_id, pseudo_path, and fsname are required',
                        {'config': export_config}
                    )
                    continue

                # Build export parameters
                path = export_config.get('path', '/')
                readonly = export_config.get('readonly', False)
                squash = export_config.get('squash', 'no_root_squash')
                client_addr = export_config.get('client_addr', [])
                access_type = 'RO' if readonly else 'RW'

                # Get the export manager from nfs module
                export_mgr = mgr.remote('nfs', 'fetch_nfs_export_obj')

                # Check if export already exists
                existing = export_mgr.get_export_by_pseudo(
                    cluster_id, pseudo_path
                )
                if existing:
                    result.add_success(
                        'create_nfs_export[{}]'.format(i),
                        'NFS export {} already exists on cluster {}'.format(
                            pseudo_path, cluster_id
                        ),
                        {
                            'cluster_id': cluster_id,
                            'pseudo_path': pseudo_path,
                            'fsname': fsname,
                            'existing': True
                        }
                    )
                    continue

                # Build clients list
                clients: List[Dict[str, Any]] = []
                if client_addr:
                    addr_list = client_addr
                    if not isinstance(client_addr, list):
                        addr_list = [client_addr]
                    clients = [{
                        'addresses': addr_list,
                        'access_type': access_type,
                        'squash': squash
                    }]

                # Build the raw export configuration
                raw_export: Dict[str, Any] = {
                    'path': path,
                    'pseudo': pseudo_path,
                    'cluster_id': cluster_id,
                    'access_type': access_type,
                    'squash': squash,
                    'security_label': False,
                    'protocols': [4],  # NFSv4
                    'transports': ['TCP'],
                    'fsal': {
                        'name': 'CEPH',
                        'fs_name': fsname,
                    },
                    'clients': clients
                }

                # Apply the export
                applied_exports = export_mgr.apply_export(
                    cluster_id,
                    json.dumps(raw_export)
                )

                if applied_exports.has_error:
                    err_msg = 'Unknown error'
                    if applied_exports.changes:
                        # changes is a list of dicts with 'msg' key
                        first_change = applied_exports.changes[0]
                        err_msg = first_change.get('msg', err_msg)
                    result.add_failure(
                        'create_nfs_export[{}]'.format(i),
                        'Failed to create NFS export {}: {}'.format(
                            pseudo_path, err_msg
                        ),
                        {
                            'cluster_id': cluster_id,
                            'pseudo_path': pseudo_path,
                            'error': err_msg
                        }
                    )
                else:
                    # Get the created export info
                    export_info = export_mgr.get_export_by_pseudo(
                        cluster_id, pseudo_path
                    )
                    export_id = None
                    if export_info:
                        export_id = export_info.get('export_id')
                    result.add_success(
                        'create_nfs_export[{}]'.format(i),
                        'NFS export {} created on cluster {}'.format(
                            pseudo_path, cluster_id
                        ),
                        {
                            'cluster_id': cluster_id,
                            'pseudo_path': pseudo_path,
                            'fsname': fsname,
                            'export_id': export_id
                        }
                    )
                    logger.info(
                        'Created NFS export: %s:%s', cluster_id, pseudo_path
                    )

            except Exception as e:  # pylint: disable=broad-except
                result.add_failure(
                    'create_nfs_export[{}]'.format(i),
                    'Failed to create NFS export: {}'.format(str(e)),
                    {'error': str(e), 'config': export_config}
                )
                logger.exception('Failed to create NFS export %d', i)

    @Endpoint('GET', path='/status')
    @ReadPermission
    @EndpointDoc("Get storage provision service status")
    def status(self) -> Dict[str, Any]:
        """
        Get status of storage provision service and its dependencies.

        :return: Dictionary with service status information
        """
        status: Dict[str, Any] = {
            'available': True,
            'orchestrator': {'available': False, 'message': ''},
            'cephfs': {'available': False, 'message': ''},
            'nfs': {'available': False, 'message': ''},
        }

        # Check orchestrator
        try:
            orch = OrchClient.instance()
            orch_status = orch.status()
            status['orchestrator'] = {
                'available': orch_status.get('available', False),
                'message': orch_status.get('message', '')
            }
        except Exception as e:  # pylint: disable=broad-except
            status['orchestrator'] = {
                'available': False,
                'message': str(e)
            }

        # Check CephFS via mgr.remote()
        try:
            filesystems = CephFSService.list_filesystems()
            status['cephfs'] = {
                'available': True,
                'message': 'CephFS is available',
                'filesystems_count': len(filesystems) if filesystems else 0
            }
        except Exception as e:  # pylint: disable=broad-except
            status['cephfs'] = {
                'available': False,
                'message': str(e)
            }

        # Check NFS via mgr.remote()
        try:
            mgr.remote('nfs', 'cluster_ls')
            status['nfs'] = {
                'available': True,
                'message': 'NFS-Ganesha is available'
            }
        except Exception as e:  # pylint: disable=broad-except
            status['nfs'] = {
                'available': False,
                'message': str(e)
            }

        # Overall availability
        status['available'] = all([
            status['orchestrator']['available'],
            status['cephfs']['available'],
            status['nfs']['available']
        ])

        return status
