export module llvm.sakuin.optional_repro;

import std;

import sakuin.dht.peer_discovery;

export struct ReproPoll {
  std::optional<sakuin::dht::PeerDiscoveryStep> peer_discovery;
};

export ReproPoll assign_peer_discovery(
    sakuin::dht::PeerDiscoveryStep step) {
  ReproPoll result;
  result.peer_discovery = std::move(step);
  return result;
}
