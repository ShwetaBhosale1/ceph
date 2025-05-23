// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2023 IBM, Inc.
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 */

#ifndef  MON_NVMEGWMONITOR_H_
#define  MON_NVMEGWMONITOR_H_

#include "PaxosService.h"
#include "NVMeofGwMap.h"

struct LastBeacon {
  NvmeGwId gw_id;
  NvmeGroupKey group_key;

  // Comparison operators to allow usage as a map key
  bool operator<(const LastBeacon& other) const {
    if (gw_id != other.gw_id) return gw_id < other.gw_id;
    return group_key < other.group_key;
  }

  bool operator==(const LastBeacon& other) const {
    return gw_id == other.gw_id &&
      group_key == other.group_key;
  }
};

class NVMeofGwMon: public PaxosService,
                   public md_config_obs_t
{
  NVMeofGwMap map;  //NVMeGWMap
  NVMeofGwMap pending_map;
  std::map<LastBeacon, ceph::coarse_mono_clock::time_point> last_beacon;
  ceph::coarse_mono_clock::time_point last_tick;

public:
  NVMeofGwMon(Monitor &mn, Paxos &p, const std::string& service_name)
    : PaxosService(mn, p, service_name) {
    map.mon = &mn;
  }
  ~NVMeofGwMon() override {}

  // config observer
  std::vector<std::string> get_tracked_keys() const noexcept override {
    return {};
  }
  void handle_conf_change(
    const ConfigProxy& conf, const std::set<std::string> &changed) override {};

  // 3 pure virtual methods of the paxosService
  void create_initial() override {};
  void create_pending() override;
  void encode_pending(MonitorDBStore::TransactionRef t) override;

  void init() override;
  void on_shutdown() override;
  void on_restart() override;
  void update_from_paxos(bool *need_bootstrap) override;

  version_t get_trim_to() const override;

  bool preprocess_query(MonOpRequestRef op) override;
  bool prepare_update(MonOpRequestRef op) override;

  bool preprocess_command(MonOpRequestRef op);
  bool prepare_command(MonOpRequestRef op);

  void encode_full(MonitorDBStore::TransactionRef t) override {}

  bool preprocess_beacon(MonOpRequestRef op);
  bool prepare_beacon(MonOpRequestRef op);

  void tick() override;
  void print_summary(ceph::Formatter *f, std::ostream *ss) const;

  void check_subs(bool type);
  void check_sub(Subscription *sub);
  void check_sub_unconditional(Subscription *sub);
 
  const NVMeofGwMap& get_map() const { return map; }

  std::map<NvmeGroupKey, std::map<NvmeGwId, utime_t>> gws_deleting_time;

private:
  void synchronize_last_beacon();
  void process_gw_down(const NvmeGwId &gw_id,
     const NvmeGroupKey& group_key, bool &propose_pending,
     gw_availability_t avail);
  bool get_gw_by_addr(const  entity_addr_t &sub_addr,
       NvmeGwId &gw_id, NvmeGroupKey& group_key);
  epoch_t get_ack_map_epoch(bool gw_created, const NvmeGroupKey& group_key);
  void recreate_gw_epoch();
  void restore_pending_map_info(NVMeofGwMap & tmp_map);
};

#endif /* MON_NVMEGWMONITOR_H_ */
