//  Copyright 2019 U.C. Berkeley RISE Lab
//
//  Licensed under the Apache License, Version 2.0 (the "License");
//  you may not use this file except in compliance with the License.
//  You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
//  Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//  See the License for the specific language governing permissions and
//  limitations under the License.

#include "kvs/kvs_handlers.hpp"

void user_request_handler(
    unsigned &access_count, unsigned &seed, string &serialized, logger log,
    GlobalRingMap &global_hash_rings, LocalRingMap &local_hash_rings,
    map<Key, vector<PendingRequest>> &pending_requests,
    map<Key, std::multiset<TimePoint>> &key_access_tracker,
    map<Key, KeyProperty> &stored_key_map,
    map<Key, KeyReplication> &key_replication_map, set<Key> &local_changeset,
    ServerThread &wt, SerializerMap &serializers, SocketCache &pushers) {
  KeyRequest request;
  request.ParseFromString(serialized);

  KeyResponse response;
  string response_id = request.request_id();
  response.set_response_id(request.request_id());

  response.set_type(request.type());

  bool succeed;
  RequestType request_type = request.type();
  string response_address = request.response_address();

  for (const auto &tuple : request.tuples()) {
    // first check if the thread is responsible for the key
    Key key = tuple.key();
    string payload = tuple.payload();

    // Special DELETE key
    if (key == "DELETE") {
      log->info("Received DELETE request...clearing the map");
      unsigned long long consumption = 0;
      for (const auto &key_pair : stored_key_map) {
        consumption += key_pair.second.size_;
      }
      log->info("Previous map size: {} KB", consumption / 1000);
      stored_key_map.clear();
      consumption = 0;
      for (const auto &key_pair : stored_key_map) {
        consumption += key_pair.second.size_;
      }
      log->info("New map size: {} KB", consumption / 1000);
    }

    ServerThreadList threads = kHashRingUtil->get_responsible_threads(
        wt.replication_response_connect_address(), key, is_metadata(key),
        global_hash_rings, local_hash_rings, key_replication_map, pushers,
        kSelfTierIdVector, succeed, seed);

    if (succeed) {
      if (std::find(threads.begin(), threads.end(), wt) == threads.end()) {
        if (is_metadata(key)) {
          // this means that this node is not responsible for this metadata key
          KeyTuple *tp = response.add_tuples();

          tp->set_key(key);
          tp->set_lattice_type(tuple.lattice_type());
          tp->set_error(AnnaError::WRONG_THREAD);
        } else {
          // if we don't know what threads are responsible, we issue a rep
          // factor request and make the request pending
          kHashRingUtil->issue_replication_factor_request(
              wt.replication_response_connect_address(), key,
              global_hash_rings[Tier::MEMORY], local_hash_rings[Tier::MEMORY],
              pushers, seed);

          pending_requests[key].push_back(
              PendingRequest(request_type, tuple.lattice_type(), payload,
                             response_address, response_id));
        }
      } else { // if we know the responsible threads, we process the request
        KeyTuple *tp = response.add_tuples();
        tp->set_key(key);

        if (request_type == RequestType::GET) {
          if (stored_key_map.find(key) == stored_key_map.end() ||
              stored_key_map[key].type_ == LatticeType::NONE) {

            tp->set_error(AnnaError::KEY_DNE);
          } else {
            auto res = process_get(key, serializers[stored_key_map[key].type_]);
            tp->set_lattice_type(stored_key_map[key].type_);
            tp->set_payload(res.first);
            tp->set_error(res.second);
          }
        } else if (request_type == RequestType::PUT) {
          if (tuple.lattice_type() == LatticeType::NONE) {
            log->error("PUT request missing lattice type.");
          } else if (stored_key_map.find(key) != stored_key_map.end() &&
                     stored_key_map[key].type_ != LatticeType::NONE &&
                     stored_key_map[key].type_ != tuple.lattice_type()) {
            log->error(
                "Lattice type mismatch for key {}: query is {} but we expect "
                "{}.",
                key, LatticeType_Name(tuple.lattice_type()),
                LatticeType_Name(stored_key_map[key].type_));
          } else {
            process_put(key, tuple.lattice_type(), payload,
                        serializers[tuple.lattice_type()], stored_key_map);

            local_changeset.insert(key);
            tp->set_lattice_type(tuple.lattice_type());
          }
        } else {
          log->error("Unknown request type {} in user request handler.",
                     request_type);
        }

        if (tuple.address_cache_size() > 0 &&
            tuple.address_cache_size() != threads.size()) {
          tp->set_invalidate(true);
        }

        key_access_tracker[key].insert(std::chrono::system_clock::now());
        access_count += 1;
      }
    } else {
      pending_requests[key].push_back(
          PendingRequest(request_type, tuple.lattice_type(), payload,
                         response_address, response_id));
    }
  }

  if (response.tuples_size() > 0 && request.response_address() != "") {
    string serialized_response;
    response.SerializeToString(&serialized_response);
    kZmqUtil->send_string(serialized_response,
                          &pushers[request.response_address()]);
  }
}
