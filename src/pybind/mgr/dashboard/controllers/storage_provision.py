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
from datetime import datetime, timezone
from functools import partial, wraps

import cherrypy
from threading import Lock
from queue import Queue
from typing import Any, Dict, List, Optional

from .. import mgr
from ..security import Scope
from ..services.cephfs import CephFS as CephFSService
from ..services.exception import serialize_dashboard_exception
from ..services.orchestrator import OrchClient
from ..tools import TaskManager
from . import (
    APIDoc,
    APIRouter,
    CreatePermission,
    Endpoint,
    EndpointDoc,
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
    # If metadata is callable, call it to get dynamic metadata
    if callable(metadata):
        metadata = metadata()

    return Task(
        "storage_provision/{}".format(name),
        metadata,
        wait_for,
        partial(serialize_dashboard_exception, include_http_status=True)
    )


def provision_task_async(name, metadata, wait_for=0):
    """Task decorator for async provision operations."""
    # If metadata is callable, call it to get dynamic metadata
    if callable(metadata):
        metadata = metadata()

    class AsyncTask(Task):
        def __call__(self, func):
            @wraps(func)
            def wrapper(*args, **kwargs):
                arg_map = self._gen_arg_map(func, args, kwargs)
                metadata = self._get_metadata(arg_map)

                # Generate unique task ID
                unique_id = str(uuid.uuid4())
                task_name = f"storage_provision/{name}/{unique_id}"

                TaskManager.run(task_name, metadata, func, args, kwargs,
                                exception_handler=self.exception_handler)
                # Always return immediately with task info
                cherrypy.response.status = 202
                return {
                    'task_id': task_name,
                    'unique_id': unique_id,
                    'message': 'Configuration started successfully'
                }
            return wrapper

    return AsyncTask(
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
        "Provision NFS export over CephFS (batch async/background)",
        parameters={
            'configs': ([dict], 'List of provision configurations'),
            'parallel': (bool, 'Execute in parallel (default: true)'),
            'max_workers': (int, 'Max parallel workers (default: 4)')
        },
        responses={
            202: {
                'task_id': (str, 'Unique task identifier for querying status'),
                'unique_id': (str, 'Unique ID component'),
                'message': (str, 'Success message')
            }
        }
    )
    @RESTController.MethodMap(version=APIVersion(1, 0))  # type: ignore
    @provision_task_async('nfs_cephfs', {'subtasks': []})
    def provision_nfs_cephfs_batch_async(
        self,
        configs: List[Dict[str, Any]],
        parallel: bool = True,
        max_workers: int = 4
    ) -> Dict[str, Any]:
        """
        Provision multiple NFS/CephFS configurations as background task.

        Returns immediately with unique task ID and success message.
        Check status: GET /api/task?name={task_id}

        :param configs: List of provision configurations
        :param parallel: Execute configurations in parallel (default: True)
        :param max_workers: Maximum number of parallel workers (default: 4)
        :return: Dictionary with task_id and message
        """
        return self._execute_batch(configs, parallel, max_workers)

    @Endpoint('POST', path='/rgw')
    @CreatePermission
    @EndpointDoc(
        "Provision RGW (Object Gateway) service - PLACEHOLDER",
        parameters={
            'configs': ([dict], 'List of RGW provision configurations'),
        },
        responses={
            501: {'error': (str, 'Not implemented')}
        }
    )
    @RESTController.MethodMap(version=APIVersion(1, 0))  # type: ignore
    def provision_rgw(self, configs: List[Dict[str, Any]]) -> Dict[str, Any]:
        """
        Provision RGW (Object Gateway) service configurations.

        PLACEHOLDER - Not implemented yet.

        :param configs: List of RGW provision configurations
        :return: Placeholder response
        """
        raise NotImplementedError("RGW provisioning not implemented yet")

    @Endpoint('POST', path='/smb')
    @CreatePermission
    @EndpointDoc(
        "Provision SMB/CIFS service - PLACEHOLDER",
        parameters={
            'configs': ([dict], 'List of SMB provision configurations'),
        },
        responses={
            501: {'error': (str, 'Not implemented')}
        }
    )
    @RESTController.MethodMap(version=APIVersion(1, 0))  # type: ignore
    def provision_smb(self, configs: List[Dict[str, Any]]) -> Dict[str, Any]:
        """
        Provision SMB/CIFS service configurations.

        PLACEHOLDER - Not implemented yet.

        :param configs: List of SMB provision configurations
        :return: Placeholder response
        """
        raise NotImplementedError("SMB provisioning not implemented yet")

    @Endpoint('POST', path='/rbd')
    @CreatePermission
    @EndpointDoc(
        "Provision RBD (Block Device) service - PLACEHOLDER",
        parameters={
            'configs': ([dict], 'List of RBD provision configurations'),
        },
        responses={
            501: {'error': (str, 'Not implemented')}
        }
    )
    @RESTController.MethodMap(version=APIVersion(1, 0))  # type: ignore
    def provision_rbd(self, configs: List[Dict[str, Any]]) -> Dict[str, Any]:
        """
        Provision RBD (Block Device) service configurations.

        PLACEHOLDER - Not implemented yet.

        :param configs: List of RBD provision configurations
        :return: Placeholder response
        """
        raise NotImplementedError("RBD provisioning not implemented yet")

    @Endpoint('POST', path='/nfs-rgw')
    @CreatePermission
    @EndpointDoc(
        "Provision NFS-RGW service - PLACEHOLDER",
        parameters={
            'configs': ([dict], 'List of NFS-RGW provision configurations'),
        },
        responses={
            501: {'error': (str, 'Not implemented')}
        }
    )
    @RESTController.MethodMap(version=APIVersion(1, 0))  # type: ignore
    def provision_nfs_rgw(
        self,
        configs: List[Dict[str, Any]]
    ) -> Dict[str, Any]:
        """
        Provision NFS-RGW service configurations.

        PLACEHOLDER - Not implemented yet.

        :param configs: List of NFS-RGW provision configurations
        :return: Placeholder response
        """
        raise NotImplementedError("NFS-RGW provisioning not implemented yet")

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

        # Initialize detailed subtasks for each operation type per config
        task_begin_time = datetime.now(timezone.utc).isoformat()
        subtasks = []
        for idx, cfg in enumerate(configs):
            config_name = self._get_config_name(cfg, idx)

            # Add subtasks for each operation type in this config
            if cfg.get('nfs_service'):
                subtasks.append({
                    'name': f'config_{idx}_nfs_service',
                    'description': f'NFS service for {config_name}',
                    'status': 'pending',
                    'begin_time': task_begin_time,
                    'config_id': idx,
                    'operation': 'nfs_service'
                })

            if cfg.get('fs_volume'):
                subtasks.append({
                    'name': f'config_{idx}_fs_volume',
                    'description': f'CephFS volume for {config_name}',
                    'status': 'pending',
                    'begin_time': task_begin_time,
                    'config_id': idx,
                    'operation': 'fs_volume'
                })

            if cfg.get('fs_subvolume_groups'):
                for group_idx, group in enumerate(cfg['fs_subvolume_groups']):
                    subtasks.append({
                        'name': f'config_{idx}_subvol_group_{group_idx}',
                        'description': 'Subvolume group {} for {}'.format(
                            group["group_name"], config_name),
                        'status': 'pending',
                        'begin_time': task_begin_time,
                        'config_id': idx,
                        'operation': 'fs_subvolume_groups',
                        'group_name': group['group_name']
                    })

            if cfg.get('fs_subvolumes'):
                for subvol_idx, subvol in enumerate(cfg['fs_subvolumes']):
                    subtasks.append({
                        'name': f'config_{idx}_subvolume_{subvol_idx}',
                        'description': f'Subvolume {subvol["sub_name"]} '
                                       f'for {config_name}',
                        'status': 'pending',
                        'begin_time': task_begin_time,
                        'config_id': idx,
                        'operation': 'fs_subvolumes',
                        'sub_name': subvol['sub_name']
                    })

            if cfg.get('nfs_exports'):
                for export_idx, export in enumerate(cfg['nfs_exports']):
                    subtasks.append({
                        'name': f'config_{idx}_nfs_export_{export_idx}',
                        'description': f'NFS export {export["pseudo_path"]} '
                                       f'for {config_name}',
                        'status': 'pending',
                        'begin_time': task_begin_time,
                        'config_id': idx,
                        'operation': 'nfs_exports',
                        'pseudo_path': export['pseudo_path']
                    })

        # Update the current task metadata with subtasks
        current_task = TaskManager.current_task()
        if current_task:
            current_task.metadata['subtasks'] = subtasks

        # Create a thread-safe queue for status updates
        status_queue: Queue = Queue()

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
                        self._execute_single_config, cfg, idx,
                        lambda c_idx, op, st, item_idx=None: self._update_subtask_status(
                            c_idx, op, st, item_idx, status_queue)
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
                        config_name = self._get_config_name(configs[idx], idx)
                        err_result = ProvisionResult(
                            config_id=str(idx),
                            config_name=config_name
                        )
                        err_result.add_failure(
                            'execution',
                            'Configuration execution failed: {}'.format(
                                str(e)
                            ),
                            {'error': str(e)}
                        )
                        results_by_idx[idx] = err_result
                        # Mark all subtasks for this config as failed
                        config = configs[idx]
                        operations = []
                        if config.get('nfs_service'):
                            operations.append(('nfs_service', None))
                        if config.get('fs_volume'):
                            operations.append(('fs_volume', None))
                        if config.get('fs_subvolume_groups'):
                            for i in range(len(config['fs_subvolume_groups'])):
                                operations.append(('fs_subvolume_groups', i))
                        if config.get('fs_subvolumes'):
                            for i in range(len(config['fs_subvolumes'])):
                                operations.append(('fs_subvolumes', i))
                        if config.get('nfs_exports'):
                            for i in range(len(config['nfs_exports'])):
                                operations.append(('nfs_exports', i))

                        timestamp = datetime.now(timezone.utc).isoformat()
                        for op, idx_val in operations:
                            status_queue.put({
                                'config_idx': idx,
                                'operation': op,
                                'status': 'failed',
                                'item_index': idx_val,
                                'timestamp': timestamp
                            })
                        logger.exception(
                            'Configuration %d execution failed', idx
                        )

                # Status updates are now applied immediately, no need to process queue

                # Sort results by original index
                for idx in sorted(results_by_idx.keys()):
                    config_results.append(results_by_idx[idx].to_batch_dict())
        else:
            # Execute configurations sequentially
            for idx, cfg in enumerate(configs):
                try:
                    result = self._execute_single_config(
                        cfg, idx,
                        lambda c_idx, op, st, item_idx=None: self._update_subtask_status(
                            c_idx, op, st, item_idx, status_queue)
                    )
                    config_results.append(result.to_batch_dict())
                except Exception as e:  # pylint: disable=broad-except
                    config_name = self._get_config_name(cfg, idx)
                    err_result = ProvisionResult(
                        config_id=str(idx),
                        config_name=config_name
                    )
                    err_result.add_failure(
                        'execution',
                        'Configuration execution failed: {}'.format(str(e)),
                        {'error': str(e)}
                    )
                    config_results.append(err_result.to_batch_dict())
                    # Mark all subtasks for this config as failed
                    operations = []
                    if cfg.get('nfs_service'):
                        operations.append(('nfs_service', None))
                    if cfg.get('fs_volume'):
                        operations.append(('fs_volume', None))
                    if cfg.get('fs_subvolume_groups'):
                        for i in range(len(cfg['fs_subvolume_groups'])):
                            operations.append(('fs_subvolume_groups', i))
                    if cfg.get('fs_subvolumes'):
                        for i in range(len(cfg['fs_subvolumes'])):
                            operations.append(('fs_subvolumes', i))
                    if cfg.get('nfs_exports'):
                        for i in range(len(cfg['nfs_exports'])):
                            operations.append(('nfs_exports', i))

                    timestamp = datetime.now(timezone.utc).isoformat()
                    for op, idx_val in operations:
                        status_queue.put({
                            'config_idx': idx,
                            'operation': op,
                            'status': 'failed',
                            'item_index': idx_val,
                            'timestamp': timestamp
                        })
                    logger.exception('Configuration %d execution failed', idx)

        # Status updates are now applied immediately, no need to process queue

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
        index: int = 0,
        status_callback=None
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
        config_name = self._get_config_name(config, index)

        result = ProvisionResult(
            config_id=str(index),
            config_name=config_name
        )

        # Step 1: Apply NFS Service Specification
        nfs_service_config = config.get('nfs_service')
        if nfs_service_config and status_callback:
            status_callback(index, 'nfs_service', 'running')
        initial_result_count = len(result.results)
        self._apply_nfs_service(nfs_service_config, result)
        if nfs_service_config and status_callback:
            # Check if this operation succeeded by looking at the last result
            latest_results = result.results[initial_result_count:]
            operation_failed = any(r['status'] == 'failed' for r in latest_results)
            status = 'failed' if operation_failed else 'completed'
            status_callback(index, 'nfs_service', status)

        # Step 2: Create CephFS Volume
        fs_volume_config = config.get('fs_volume')
        if fs_volume_config and status_callback:
            status_callback(index, 'fs_volume', 'running')
        initial_result_count = len(result.results)
        self._create_fs_volume(fs_volume_config, result)
        if fs_volume_config and status_callback:
            latest_results = result.results[initial_result_count:]
            operation_failed = any(r['status'] == 'failed' for r in latest_results)
            status = 'failed' if operation_failed else 'completed'
            status_callback(index, 'fs_volume', status)

        # Step 3: Create Subvolume Groups
        subvol_groups = config.get('fs_subvolume_groups')
        if subvol_groups and status_callback:
            for group_idx, group in enumerate(subvol_groups):
                status_callback(index, 'fs_subvolume_groups', 'running', group_idx)
        initial_result_count = len(result.results)
        self._create_subvolume_groups(subvol_groups, result)
        if subvol_groups and status_callback:
            latest_results = result.results[initial_result_count:]
            # Check each subvolume group individually
            for group_idx, group in enumerate(subvol_groups):
                group_results = [r for r in latest_results if f'group[{group_idx}]' in r['operation']]
                operation_failed = any(r['status'] == 'failed' for r in group_results)
                status = 'failed' if operation_failed else 'completed'
                status_callback(index, 'fs_subvolume_groups', status, group_idx)

        # Step 4: Create Subvolumes
        subvolumes = config.get('fs_subvolumes')
        if subvolumes and status_callback:
            for subvol_idx, subvol in enumerate(subvolumes):
                status_callback(index, 'fs_subvolumes', 'running', subvol_idx)
        initial_result_count = len(result.results)
        self._create_subvolumes(subvolumes, result)
        if subvolumes and status_callback:
            latest_results = result.results[initial_result_count:]
            # Check each subvolume individually
            for subvol_idx, subvol in enumerate(subvolumes):
                subvol_results = [r for r in latest_results if f'[{subvol_idx}]' in r['operation']]
                operation_failed = any(r['status'] == 'failed' for r in subvol_results)
                status = 'failed' if operation_failed else 'completed'
                status_callback(index, 'fs_subvolumes', status, subvol_idx)

        # Step 5: Wait for NFS service if we created one and have exports
        nfs_exports = config.get('nfs_exports')
        if nfs_service_config and nfs_exports:
            service_id = nfs_service_config.get('service_id')
            if service_id:
                self._wait_for_nfs_service(service_id, result)

        # Step 6: Create NFS Exports
        if nfs_exports and status_callback:
            for export_idx, export in enumerate(nfs_exports):
                status_callback(index, 'nfs_exports', 'running', export_idx)
        initial_result_count = len(result.results)
        self._create_nfs_exports(nfs_exports, result)
        if nfs_exports and status_callback:
            latest_results = result.results[initial_result_count:]
            # Check each export individually
            for export_idx, export in enumerate(nfs_exports):
                export_results = [r for r in latest_results if f'[{export_idx}]' in r['operation']]
                operation_failed = any(r['status'] == 'failed' for r in export_results)
                status = 'failed' if operation_failed else 'completed'
                status_callback(index, 'nfs_exports', status, export_idx)

        return result

    def _get_config_name(self, config: Dict[str, Any], index: int) -> str:
        """
        Get a descriptive name for a configuration.

        :param config: Configuration dictionary
        :param index: Configuration index
        :return: Configuration name
        """
        # Extract configuration from the wrapper if present
        if 'configure_nfs_cephfs_export' in config:
            config = config['configure_nfs_cephfs_export']

        # Determine config name from service_id or volume name
        config_name = f'config_{index}'
        nfs_service_config = config.get('nfs_service')
        if nfs_service_config and nfs_service_config.get('service_id'):
            config_name = f'nfs_{nfs_service_config["service_id"]}'
        elif config.get('fs_volume', {}).get('name'):
            config_name = f'fs_{config["fs_volume"]["name"]}'

        return config_name

    def _update_subtask_status(self, config_idx: int, operation: str, status: str,
                              item_index: Optional[int] = None, status_queue: Optional[Queue] = None):
        """Update the status of a specific subtask."""
        current_task = TaskManager.current_task()
        if not current_task:
            return

        timestamp = datetime.now(timezone.utc).isoformat()

        # Apply the update immediately using the task's lock for thread safety
        with current_task.lock:
            self._apply_subtask_update(current_task, config_idx, operation, status, item_index, timestamp)

        # For backward compatibility, also put in queue if provided (though not used for immediate updates)
        if status_queue is not None:
            status_queue.put({
                'config_idx': config_idx,
                'operation': operation,
                'status': status,
                'item_index': item_index,
                'timestamp': timestamp
            })

    def _apply_subtask_update(self, current_task, config_idx: int, operation: str, status: str,
                            item_index: Optional[int], timestamp: str):
        """Apply a subtask status update to the task metadata."""
        # Find the correct subtask key
        if operation in ['nfs_service', 'fs_volume']:
            subtask_key = f'config_{config_idx}_{operation}'
        elif operation == 'fs_subvolume_groups':
            subtask_key = f'config_{config_idx}_subvol_group_{item_index}'
        elif operation == 'fs_subvolumes':
            subtask_key = f'config_{config_idx}_subvolume_{item_index}'
        elif operation == 'nfs_exports':
            subtask_key = f'config_{config_idx}_nfs_export_{item_index}'
        else:
            return  # Unknown operation

        # Find and update the subtask
        for subtask in current_task.metadata.get('subtasks', []):
            if subtask['name'] == subtask_key:
                old_status = subtask.get('status')
                subtask['status'] = status

                # Add begin_time when status changes to running (only if not already set)
                if status == 'running' and old_status != 'running' and 'begin_time' not in subtask:
                    subtask['begin_time'] = timestamp

                # Add end_time when status changes to completed or failed
                elif status in ['completed', 'failed'] and 'end_time' not in subtask:
                    subtask['end_time'] = timestamp

                break

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

                # Add a small delay to ensure the subvolume group is available
                time.sleep(1)

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

    def _wait_for_cephfs_path(
        self,
        fsname: str,
        path: str,
        result: ProvisionResult,
        operation_name: str,
        timeout: int = 30,
        poll_interval: int = 2
    ):
        """
        Wait for a CephFS path to exist.

        :param fsname: CephFS filesystem name
        :param path: Path to wait for
        :param result: ProvisionResult to record status
        :param operation_name: Name of the operation for logging
        :param timeout: Maximum time to wait in seconds
        :param poll_interval: Time between checks in seconds
        """
        start_time = time.time()
        while time.time() - start_time < timeout:
            try:
                # Try to stat the path using CephFS service
                cfs = CephFSService(fsname)
                # Check if path exists by trying to get its info
                try:
                    cfs.get_directory(path)
                    result.add_success(
                        operation_name,
                        'Path {} is now available in {}'.format(path, fsname),
                        {'fsname': fsname, 'path': path,
                         'wait_time': int(time.time() - start_time)}
                    )
                    return
                except Exception:
                    pass  # Path doesn't exist yet
            except Exception as e:
                logger.debug('Error checking path %s: %s', path, str(e))

            time.sleep(poll_interval)

        # Timeout reached
        result.add_failure(
            operation_name,
            'Timeout waiting for path {} to exist in {}'.format(path, fsname),
            {
                'fsname': fsname,
                'path': path,
                'timeout': timeout
            }
        )

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

                # Wait for the path to exist if it's not the root
                if path != '/':
                    op_name = 'create_nfs_export[{}]'.format(i)
                    self._wait_for_cephfs_path(fsname, path, result, op_name)

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
