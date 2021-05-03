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

#include "route/routing_handlers.hpp"

void address_handler(logger log, string &serialized, SocketCache &pushers,
                     RoutingThread &rt, GlobalRingMap &global_hash_rings,
                     LocalRingMap &local_hash_rings,
                     map<Key, KeyReplication> &key_replication_map,
                     map<Key, vector<pair<Address, string>>> &pending_requests,
                     unsigned &seed) {

  KeyAddressRequest addr_request;
  addr_request.ParseFromString(serialized);

  KeyAddressResponse addr_response;
  addr_response.set_response_id(addr_request.request_id());
  bool succeed;

  int num_servers = 0;
  for (const auto &pair : global_hash_rings) {
    num_servers += pair.second.size();
  }

  bool respond = false;
  if (num_servers == 0) {
    addr_response.set_error(AnnaError::NO_SERVERS);

    for (const Key &key : addr_request.keys()) {
      KeyAddressResponse_KeyAddress *tp = addr_response.add_addresses();
      tp->set_key(key);
    }

    respond = true;
  } else { // if there are servers, attempt to return the correct threads
    for (const Key &key : addr_request.keys()) {
      log->info("Received key request for {}", key);
      ServerThreadList threads = {};

      if (key.length() >
          0) { // Only run this code is the key is a valid string.
        // Otherwise, an empty response will be sent.
        for (const Tier &tier : kAllTiers) {
          threads = kHashRingUtil->get_responsible_threads(
              rt.replication_response_connect_address(), key, is_metadata(key),
              global_hash_rings, local_hash_rings, key_replication_map, pushers,
              {tier}, succeed, seed);

          if (threads.size() > 0) {
            break;
          }

          if (!succeed) { // this means we don't have the replication factor for
                          // the key
            pending_requests[key].push_back(std::pair<Address, string>(
                addr_request.response_address(), addr_request.request_id()));
            return;
          }
        }
      }

      KeyAddressResponse_KeyAddress *tp = addr_response.add_addresses();
      tp->set_key(key);
      respond = true;

      for (const ServerThread &thread : threads) {
        tp->add_ips(thread.key_request_connect_address());
      }
    }
  }

  if (respond) {
    string serialized;
    addr_response.SerializeToString(&serialized);

    kZmqUtil->send_string(serialized,
                          &pushers[addr_request.response_address()]);
  }
}
