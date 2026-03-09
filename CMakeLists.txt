cmake_minimum_required(VERSION 3.16)
project(marina)

find_package(ament_cmake REQUIRED)
find_package(ament_cmake_python REQUIRED)

set(CARGO_TARGET_DIR "${CMAKE_CURRENT_BINARY_DIR}/cargo-target")

add_custom_command(
  OUTPUT "${CARGO_TARGET_DIR}/release/marina"
  COMMAND cargo build --release --target-dir "${CARGO_TARGET_DIR}"
  WORKING_DIRECTORY "${CMAKE_CURRENT_SOURCE_DIR}"
  COMMENT "Building marina with cargo"
)

add_custom_target(marina_binary ALL
  DEPENDS "${CARGO_TARGET_DIR}/release/marina"
)

install(
  PROGRAMS "${CARGO_TARGET_DIR}/release/marina"
  DESTINATION lib/${PROJECT_NAME}
)

ament_python_install_package(ros2_marina)

install(
  PROGRAMS "${CARGO_TARGET_DIR}/release/marina"
  DESTINATION "${PYTHON_INSTALL_DIR}/ros2_marina/_bin"
)

ament_package()
