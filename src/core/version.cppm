module;

#ifndef SAKUIN_VERSION
#define SAKUIN_VERSION "dev"
#endif

export module sakuin.core.version;

import std;

export namespace sakuin::core {

inline constexpr std::string_view version{SAKUIN_VERSION};

} // namespace sakuin::core
