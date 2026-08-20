export module sakuin.model.observation;

import sakuin.core.ids;
import sakuin.core.time;

export namespace sakuin::model {

struct ObservationRecord {
  core::InfoHash info_hash;
  core::Timestamp observed_at;

};

} // namespace sakuin::model
