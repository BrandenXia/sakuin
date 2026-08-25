export module llvm.sakuin.optional_backfill_repro;

import std;

import sakuin.dht.peer_discovery;
import sakuin.integration.metadata_backfill;

export struct BackfillReproPoll {
  std::optional<sakuin::dht::PeerDiscoveryStep> peer_discovery;
  std::optional<sakuin::integration::MetadataDiscoveryBackfillStep>
      metadata_backfill;
};

export BackfillReproPoll assign_peer_discovery(
    sakuin::dht::PeerDiscoveryStep step) {
  BackfillReproPoll result;
  result.peer_discovery = std::move(step);
  return result;
}
