#!/usr/bin/env python3

from conans import ConanFile, CMake


class DoryMembershipConanExport(ConanFile):
    name = "dory-membership-export"
    version = "0.0.1"
    license = "MIT"
    # url = "TODO"
    description = "Export of the RDMA membership"
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
        "fPIC": [True, False],
        "lto": [True, False],
        "log_level": ["TRACE", "DEBUG", "INFO", "WARN", "ERROR", "CRITICAL", "OFF"],
    }
    default_options = {
        "shared": True,
        "fPIC": True,
        "lto": True,
        "log_level": "INFO",
    }

    generators = "cmake"
    python_requires = "dory-compiler-options/0.0.1@dory/stable"
    exports_sources = "src/*"

    def requirements(self):
        self.requires("dory-membership/0.0.1")

    def build(self):
        generator = self.python_requires["dory-compiler-options"].module.generator()
        cmake = CMake(self, generator=generator)

        self.python_requires["dory-compiler-options"].module.set_options(cmake)
        lto_decision = self.python_requires[
            "dory-compiler-options"
        ].module.lto_decision(cmake, self.options.lto)
        cmake.definitions["DORY_LTO"] = str(lto_decision).upper()
        cmake.definitions["SPDLOG_ACTIVE_LEVEL"] = "SPDLOG_LEVEL_{}".format(
            self.options.log_level
        )

        cmake.configure(source_folder="src")
        cmake.build()

    def package(self):
        self.copy("membership.h", dst="include/", src="src")
        self.copy("*.a", dst="lib", src="lib", keep_path=False)
        self.copy("*.so", dst="lib", src="lib", keep_path=False)

    def package_info(self):
        self.cpp_info.libs = ["dorymembership"]
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

    invoker.run(outOfTree=True)
