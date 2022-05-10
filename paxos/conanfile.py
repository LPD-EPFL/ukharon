#!/usr/bin/env python3

from conans import ConanFile, CMake


class DoryPaxosConan(ConanFile):
    name = "dory-paxos"
    version = "0.0.1"
    license = "MIT"
    # url = "TODO"
    description = "RDMA Paxos"
    settings = {
        "os": None,
        "compiler": {
            "gcc": {"libcxx": "libstdc++11", "cppstd": ["17", "20"], "version": None},
            "clang": {"libcxx": "libstdc++11", "cppstd": ["17", "20"], "version": None},
        },
        "build_type": None,
        "arch": None,
    }
    options = {
        "shared": [True, False],
        "log_level": ["TRACE", "DEBUG", "INFO", "WARN", "ERROR", "CRITICAL", "OFF"],
        "internal_testing": [True, False],
        "device_memory": [True, False],
        "lto": [True, False],
    }
    default_options = {
        "shared": False,
        "internal_testing": True,
        "log_level": "INFO",
        "dory-ctrl:log_level": "OFF",
        "dory-conn:log_level": "INFO",
        "dory-crypto:log_level": "INFO",
        "device_memory": False,
        "lto": True,
    }
    generators = "cmake"
    exports_sources = "src/*"
    python_requires = "dory-compiler-options/0.0.1@dory/stable"

    def configure(self):
        pass

    def requirements(self):
        if self.options.internal_testing:
            self.requires("gtest/1.10.0")

        self.requires("dory-conn/0.0.1")
        self.requires("dory-memstore/0.0.1")
        self.requires("dory-ctrl/0.0.1")
        self.requires("dory-shared/0.0.1")
        self.requires("dory-extern/0.0.1")
        self.requires("dory-third-party/0.0.1")
        self.requires("lyra/1.5.1")

    # Although this is a header-only package, we still want to build the unit tests.
    def build(self):
        self.python_requires["dory-compiler-options"].module.setup_cmake(
            self.build_folder
        )
        generator = self.python_requires["dory-compiler-options"].module.generator()
        cmake = CMake(self, generator=generator)

        self.python_requires["dory-compiler-options"].module.set_options(cmake)
        lto_decision = self.python_requires[
            "dory-compiler-options"
        ].module.lto_decision(cmake, self.options.lto)
        cmake.definitions["DORY_LTO"] = str(lto_decision).upper()
        cmake.definitions["DORY_PAXOS_DM"] = str(self.options.device_memory).upper()
        cmake.definitions["DORY_INTERNAL_TESTING"] = self.options.internal_testing
        cmake.definitions["SPDLOG_ACTIVE_LEVEL"] = "SPDLOG_LEVEL_{}".format(
            self.options.log_level
        )

        cmake.configure(source_folder="src")
        cmake.build()

        if self.should_build and self.options.internal_testing:
            self.run("CTEST_OUTPUT_ON_FAILURE=1 GTEST_COLOR=1 ctest")

    def package(self):
        self.copy("*.hpp", dst="include/dory/paxos", src="src")
        self.copy("*.a", dst="lib", src="lib", keep_path=False)
        self.copy("*.so", dst="lib", src="lib", keep_path=False)

    def package_info(self):
        self.cpp_info.libs = ["dorypaxos"]
        self.cpp_info.cxxflags = self.python_requires[
            "dory-compiler-options"
        ].module.get_cxx_options_for(self.settings.compiler, self.settings.build_type)


if __name__ == "__main__":
    import os, pathlib, sys

    # Find dory root directory
    root_dir = pathlib.Path(os.path.dirname(os.path.abspath(__file__)))
    while not os.path.isfile(os.path.join(root_dir, ".dory-root")):
        root_dir = root_dir.parent

    sys.path.append(os.path.join(root_dir, "conan", "invoker"))

    import invoker

    invoker.run()
