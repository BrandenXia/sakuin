export module sakuin.dht.metadata_candidate;

import sakuin.core.ids;
import sakuin.core.time;
import sakuin.runtime.stream;

export namespace sakuin::dht {

struct PeerMetadataCandidate {
  core::InfoHash info_hash;
  runtime::StreamEndpoint peer;
  core::Timestamp observed_at;
};

} // namespace sakuin::dht
