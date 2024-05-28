//
// Created by fsindustry on 5/27/24.
//

#ifndef GTIDSYNER_GTID_MYSQL_RESULT_H
#define GTIDSYNER_GTID_MYSQL_RESULT_H

#include <unordered_map>
#include <vector>
#include <mysql.h>

namespace gtid_syner {

    typedef std::unordered_map<std::string, std::string> map_row_t;
    typedef std::vector<map_row_t> vec_row_t;

    /**
     * store mysql result data
     */
    class Gtid_MySQL_Result {

    public:
        Gtid_MySQL_Result() {}

        Gtid_MySQL_Result(MYSQL *mysql, MYSQL_RES *res);

        Gtid_MySQL_Result(const Gtid_MySQL_Result &) = delete;

        Gtid_MySQL_Result &operator=(const Gtid_MySQL_Result &) = delete;

        virtual ~Gtid_MySQL_Result() {}

        bool init(MYSQL *mysql, MYSQL_RES *res);

        MYSQL_ROW fetch_row();

        unsigned int num_rows();

        const MYSQL_RES *result() { return m_res; }

        int result_data(vec_row_t &data);

        unsigned long *fetch_lengths();

        unsigned int fetch_num_fields();

        bool is_ok() { return m_error == 0; }

        int error() { return m_error; }

        const std::string &errstr() const { return m_errstr; }

    private:
        int m_error = -1;
        std::string m_errstr;

        MYSQL *m_mysql = nullptr;
        MYSQL_RES *m_res = nullptr;
        MYSQL_ROW m_cur_row;
        int m_row_cnt = 0;
        int m_field_cnt = 0;

    };

} // gtid_syner
#endif //GTIDSYNER_GTID_MYSQL_RESULT_H