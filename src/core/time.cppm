export module sakuin.core.time;

import std;

export namespace sakuin::core {

using Timestamp = std::chrono::system_clock::time_point;

using Duration = std::chrono::nanoseconds;

} // namespace sakuin::core
