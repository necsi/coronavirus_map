# Package

version       = "0.1.0"
author        = "Michael Buchel"
description   = "Coronavirus mapping package for necsi"
license       = "MIT"
srcDir        = "src"
bin           = @["coronavirus_map", "manual_override"]
binDir        = "bin"

backend       = "cpp"

# Dependencies

requires "nim >= 1.0.4"
