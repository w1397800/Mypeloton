cmake_minimum_required(VERSION 3.10)

# Set the project name
project(MyProject)

# Specify the C++ standard
set(CMAKE_CXX_STANDARD 11)
set(CMAKE_CXX_STANDARD_REQUIRED True)
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)


# Add source files
set(SOURCE_FILES
    log_storage.cpp
    storage_util.cpp
)

# Include header files' directories
include_directories(${PROJECT_SOURCE_DIR})

# Create a library with the above source files
add_library(log_storage_lib ${SOURCE_FILES})

############################# clang-tidy #############################

if(CMAKE_VERSION VERSION_GREATER 3.6)
    # We check if the current version of CMake has modules for Clang tools
    find_program(CLANG_TIDY "clang-tidy")
    if(CLANG_TIDY)
      message(STATUS "clang-tidy found: ${CLANG_TIDY}")
    else()
      message(STATUS "clang-tidy not found")
    endif()
    
    if(CLANG_TIDY)
    set(CMAKE_CXX_CLANG_TIDY ${CLANG_TIDY})
    endif()
  
endif()



############################# gtest #############################

# # Enable testing
# enable_testing()

# # Include GoogleTest directory
# find_package(GTest REQUIRED)
# include_directories(${GTEST_INCLUDE_DIRS})

# # Add test executable
# add_executable(runTests unit_test.cpp)

# # Link test executable with GoogleTest and project library
# target_link_libraries(runTests ${GTEST_LIBRARIES} pthread log_storage_lib)

# # Add test
# add_test(NAME Storage_test COMMAND runTests)
