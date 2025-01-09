from typing import Any

from .cluster import NFSCluster
from .ganesha_conf import QOSType, qos_bandwidth_checks


def export_block_qos_checks(cluster_id: str,
                            mgr_obj: Any,
                            qos_block: dict) -> None:
    """Validate the qos block of dict passed to apply_export method"""
    qos_enable = qos_block.get('enable_qos')
    enable_bw_ctrl = qos_block.get('enable_bw_control')
    if qos_enable is None or enable_bw_ctrl is None:
        raise Exception('The QOS block requires at least the enable_qos and enable_bw_control parameters')
    combined_bw_ctrl = qos_block.get('combined_rw_bw_control', False)
    del qos_block['enable_qos']
    del qos_block['enable_bw_control']
    if 'combined_rw_bw_control' in qos_block:
        del qos_block['combined_rw_bw_control']
    # if qos is disabled, then bandwidths should not be set
    if (isinstance(qos_enable, bool) and isinstance(enable_bw_ctrl, bool)
       and isinstance(combined_bw_ctrl, bool)) and (qos_enable ^ enable_bw_ctrl):
        raise Exception('Invalid values for the qos_enable, enable_bw_ctrl and combined_bw_ctrl parameters.')
    if not qos_enable and qos_block:
        raise Exception('Bandwidths should not be passed when qos_enable is false.')
    if qos_enable and not qos_block:
        raise Exception('Bandwidths should be set when qos_enable is true.')
    export_qos_checks(cluster_id, mgr_obj, combined_bw_ctrl=combined_bw_ctrl, **qos_block)


def export_qos_checks(cluster_id: str,
                      mgr_obj: Any,
                      combined_bw_ctrl: bool,
                      nfs_clust_obj: Any = None,
                      **kwargs: Any) -> None:
    """check cluster level qos is enabled to enable export level qos and validate bandwidths"""
    if not nfs_clust_obj:
        nfs_clust_obj = NFSCluster(mgr_obj)
    clust_qos_obj = nfs_clust_obj.get_cluster_qos_config(cluster_id)
    if not clust_qos_obj or (clust_qos_obj and not (clust_qos_obj.enable_qos)):
        raise Exception('To configure bandwidth control for export, you must first enable bandwidth control at the cluster level.')
    if clust_qos_obj.qos_type:
        if clust_qos_obj.qos_type == QOSType._2:
            raise Exception('Export-level QoS bandwidth control cannot be enabled if the QoS type at the cluster level is set to PerClient.')
        qos_bandwidth_checks(clust_qos_obj.qos_type, combined_bw_ctrl, **kwargs)
