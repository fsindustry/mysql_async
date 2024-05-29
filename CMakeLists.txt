cmake_minimum_required(VERSION 3.10)

# 项目名称
project(GtidSyner)

# 指定使用C++11标准
set(CMAKE_CXX_STANDARD 11)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

# 禁用编译器特定扩展
set(CMAKE_CXX_EXTENSIONS OFF)

# 设置编译器
set(CMAKE_C_COMPILER gcc)
set(CMAKE_CXX_COMPILER g++)

# 设置编译标志
set(CMAKE_C_FLAGS "-g")
set(CMAKE_CXX_FLAGS "-g")

# 包含目录
include_directories(/home/fsindustry/develop/mariadb/mysql/include/mysql)

# 库目录和库
link_directories(/home/fsindustry/develop/mariadb/mysql/lib)
set(LIBS mariadbclient ev ssl crypto dl z pthread)
# set (CMAKE_CXX_LINK_EXECUTABLE "${CMAKE_CXX_LINK_EXECUTABLE} -ldl")

# 源文件
set(TEST_SOURCES mconn.cpp test.cpp mconn.h)

# 添加可执行文件
add_executable(test ${TEST_SOURCES})

# 链接库
target_link_libraries(test ${LIBS})


# 源文件
set(GTID_TEST_SOURCES gtid_syner_test.cpp
        gtid_syner/Gtid_MySQL_Result.cpp
        gtid_syner/Gtid_MySQL_Result.h
        gtid_syner/Gtid_MySQL_Conn.h
        gtid_syner/Gtid_MySQL_Conn.cpp
        gtid_syner/Gtid_MySQL_Conn.h
        gtid_syner/Gtid_MySQL_Conn.cpp
        gtid_syner/Gtid_MySQL_Conn.h
        gtid_syner/Gtid_MySQL_Conn_Mgr.cpp
        gtid_syner/Gtid_MySQL_Conn_Mgr.h)

# 添加可执行文件
add_executable(gtid_syner_test ${GTID_TEST_SOURCES})

# 链接库
target_link_libraries(gtid_syner_test ${LIBS})

