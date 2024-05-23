cmake_minimum_required(VERSION 3.10)

# 项目名称
project(TestProject)

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
set(SOURCES mconn.cpp test.cpp mconn.h)

# 添加可执行文件
add_executable(test ${SOURCES})

# 链接库
target_link_libraries(test ${LIBS})

