add_rules("mode.debug", "mode.release")

set_languages("c++23")

set_policy("build.c++.modules", true)

add_requires("openssl")

target("sakuin-core")
  set_kind("static")
  add_files("src/core/**.cpp")
  add_files("src/core/**.cppm", {public = true})
  add_packages("openssl")

target("sakuin-storage")
  set_kind("static")
  add_files("src/storage/**.cppm", {public = true})
  add_deps("sakuin-core")

target("sakuin-model")
  set_kind("static")
  add_files("src/model/**.cppm", {public = true})
  add_deps("sakuin-core")

target("sakuin-storage-tests")
  set_kind("binary")
  add_files("tests/storage/**.cpp")
  add_deps("sakuin-core", "sakuin-model", "sakuin-storage")
  add_tests("storage")

-- target("dht")
--   set_kind("static")
--   add_deps("core", "model")
--
-- target("index")
--   set_kind("static")
--   add_deps("core", "model", "storage")
--
-- target("api")
--   set_kind("static")
--   add_deps("core", "model", "index", "scheduler", "storage")
--
-- target("app")
--   set_kind("binary")
--   add_deps("core", "storage", "model", "dht", "index", "scheduler", "api")
