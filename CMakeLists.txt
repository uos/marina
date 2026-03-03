cmake_minimum_required(VERSION 3.5)
project(marina)

find_package(ament_cmake REQUIRED)

if(NOT DEFINED CMAKE_SUPPRESS_DEVELOPER_WARNINGS)
     set(CMAKE_SUPPRESS_DEVELOPER_WARNINGS 1 CACHE INTERNAL "No dev warnings")
endif()

if(WIN32)
    set(SUFFIX ".exe")
else()
    set(SUFFIX "")
endif()

add_custom_target(cargo_target ALL
    COMMAND cargo build --profile colcon --locked
    WORKING_DIRECTORY ${CMAKE_SOURCE_DIR}
)

install(PROGRAMS
  ${CMAKE_SOURCE_DIR}/target/colcon/${PROJECT_NAME}${SUFFIX}
  DESTINATION lib/${PROJECT_NAME}
)

ament_package()
