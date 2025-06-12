#include "query5.hpp"
#include <iostream>
#include <fstream>
#include <sstream>
#include <thread>
#include <mutex>
#include <algorithm>

// Function to parse command line arguments
bool parseArgs(int argc, char* argv[], std::string& r_name, std::string& start_date, std::string& end_date,
               int& num_threads, std::string& table_path, std::string& result_path) {
    for (int i = 1; i < argc; ++i) {
        if (strcmp(argv[i], "--r_name") == 0 && i + 1 < argc)
            r_name = argv[++i];
        else if (strcmp(argv[i], "--start_date") == 0 && i + 1 < argc)
            start_date = argv[++i];
        else if (strcmp(argv[i], "--end_date") == 0 && i + 1 < argc)
            end_date = argv[++i];
        else if (strcmp(argv[i], "--threads") == 0 && i + 1 < argc)
            num_threads = std::stoi(argv[++i]);
        else if (strcmp(argv[i], "--table_path") == 0 && i + 1 < argc)
            table_path = argv[++i];
        else if (strcmp(argv[i], "--result_path") == 0 && i + 1 < argc)
            result_path = argv[++i];
        else {
            std::cerr << "Unknown or incomplete argument: " << argv[i] << std::endl;
            return false;
        }
    }
    return !(r_name.empty() || start_date.empty() || end_date.empty() || table_path.empty() || result_path.empty());
}


bool loadTable(const std::string& filename, const std::vector<std::string>& columns,
               std::vector<std::map<std::string, std::string>>& table) {
    std::ifstream file(filename);
    if (!file.is_open()) return false;

    std::string line;
    while (getline(file, line)) {
        std::map<std::string, std::string> row;
        std::stringstream ss(line);
        std::string field;
        size_t i = 0;

        while (getline(ss, field, '|') && i < columns.size()) {
            row[columns[i++]] = field;
        }
        table.push_back(row);
    }
    return true;
}

// Function to read TPCH data from the specified paths
bool readTPCHData(const std::string& path,
                  std::vector<std::map<std::string, std::string>>& customer_data,
                  std::vector<std::map<std::string, std::string>>& orders_data,
                  std::vector<std::map<std::string, std::string>>& lineitem_data,
                  std::vector<std::map<std::string, std::string>>& supplier_data,
                  std::vector<std::map<std::string, std::string>>& nation_data,
                  std::vector<std::map<std::string, std::string>>& region_data) {
    return loadTable(path + "/customer.tbl", {"C_CUSTKEY", "C_NAME", "C_ADDRESS", "C_NATIONKEY", "C_PHONE", "C_ACCTBAL", "C_MKTSEGMENT", "C_COMMENT"}, customer_data) &&
           loadTable(path + "/orders.tbl", {"O_ORDERKEY", "O_CUSTKEY", "O_ORDERSTATUS", "O_TOTALPRICE", "O_ORDERDATE", "O_ORDERPRIORITY", "O_CLERK", "O_SHIPPRIORITY", "O_COMMENT"}, orders_data) &&
           loadTable(path + "/lineitem.tbl", {"L_ORDERKEY", "L_PARTKEY", "L_SUPPKEY", "L_LINENUMBER", "L_QUANTITY", "L_EXTENDEDPRICE", "L_DISCOUNT", "L_TAX", "L_RETURNFLAG", "L_LINESTATUS", "L_SHIPDATE", "L_COMMITDATE", "L_RECEIPTDATE", "L_SHIPINSTRUCT", "L_SHIPMODE", "L_COMMENT"}, lineitem_data) &&
           loadTable(path + "/supplier.tbl", {"S_SUPPKEY", "S_NAME", "S_ADDRESS", "S_NATIONKEY", "S_PHONE", "S_ACCTBAL", "S_COMMENT"}, supplier_data) &&
           loadTable(path + "/nation.tbl", {"N_NATIONKEY", "N_NAME", "N_REGIONKEY", "N_COMMENT"}, nation_data) &&
           loadTable(path + "/region.tbl", {"R_REGIONKEY", "R_NAME", "R_COMMENT"}, region_data);
}


// Function to execute TPCH Query 5 using multithreading
    // TODO: Implement TPCH Query 5 using multithreading
bool executeQuery5(const std::string& r_name, const std::string& start_date, const std::string& end_date,
                   int num_threads,
                   const std::vector<std::map<std::string, std::string>>& customer_data,
                   const std::vector<std::map<std::string, std::string>>& orders_data,
                   const std::vector<std::map<std::string, std::string>>& lineitem_data,
                   const std::vector<std::map<std::string, std::string>>& supplier_data,
                   const std::vector<std::map<std::string, std::string>>& nation_data,
                   const std::vector<std::map<std::string, std::string>>& region_data,
                   std::map<std::string, double>& results) {

    // Step 1: Filter region to get region_key
    std::set<std::string> region_keys;
    for (const auto& region : region_data) {
        if (region.at("r_name") == r_name) {
            region_keys.insert(region.at("r_regionkey"));
        }
    }

    if (region_keys.empty()) {
        std::cerr << "Region not found.\n";
        return false;
    }

    // Step 2: Get nations in this region
    std::set<std::string> nation_keys;
    std::map<std::string, std::string> nation_name_map;
    for (const auto& nation : nation_data) {
        if (region_keys.count(nation.at("n_regionkey"))) {
            nation_keys.insert(nation.at("n_nationkey"));
            nation_name_map[nation.at("n_nationkey")] = nation.at("n_name");
        }
    }

    // Step 3: Get suppliers in this region
    std::set<std::string> supplier_keys;
    std::map<std::string, std::string> supplier_nation_map;
    for (const auto& supplier : supplier_data) {
        if (nation_keys.count(supplier.at("s_nationkey"))) {
            supplier_keys.insert(supplier.at("s_suppkey"));
            supplier_nation_map[supplier.at("s_suppkey")] = supplier.at("s_nationkey");
        }
    }

    // Step 4: Get customers in this region
    std::set<std::string> customer_keys;
    for (const auto& customer : customer_data) {
        if (nation_keys.count(customer.at("c_nationkey"))) {
            customer_keys.insert(customer.at("c_custkey"));
        }
    }

    // Step 5: Get valid orders
    std::set<std::string> order_keys;
    std::map<std::string, std::string> order_customer_map;
    for (const auto& order : orders_data) {
        if (customer_keys.count(order.at("o_custkey")) &&
            order.at("o_orderdate") >= start_date &&
            order.at("o_orderdate") < end_date) {
            order_keys.insert(order.at("o_orderkey"));
            order_customer_map[order.at("o_orderkey")] = order.at("o_custkey");
        }
    }

    // Step 6: Multithreaded lineitem scan
    auto worker = [&](int thread_id, int start, int end, std::map<std::string, double>& local_result) {
        for (int i = start; i < end; ++i) {
            const auto& item = lineitem_data[i];

            std::string orderkey = item.at("l_orderkey");
            std::string suppkey = item.at("l_suppkey");

            if (!order_keys.count(orderkey)) continue;
            if (!supplier_keys.count(suppkey)) continue;

            std::string nationkey = supplier_nation_map[suppkey];
            std::string nationname = nation_name_map[nationkey];

            double extended_price = std::stod(item.at("l_extendedprice"));
            double discount = std::stod(item.at("l_discount"));
            double revenue = extended_price * (1.0 - discount);

            local_result[nationname] += revenue;
        }
    };

    std::vector<std::thread> threads;
    std::vector<std::map<std::string, double>> partial_results(num_threads);

    int chunk_size = lineitem_data.size() / num_threads;
    for (int i = 0; i < num_threads; ++i) {
        int start = i * chunk_size;
        int end = (i == num_threads - 1) ? lineitem_data.size() : (i + 1) * chunk_size;
        threads.emplace_back(worker, i, start, end, std::ref(partial_results[i]));
    }

    for (auto& t : threads) t.join();

    // Combine partial results
    for (const auto& part : partial_results) {
        for (const auto& [nation, revenue] : part) {
            results[nation] += revenue;
        }
    }

    return true;
}

bool outputResults(const std::string& result_path, const std::map<std::string, double>& results) {
    std::ofstream out(result_path);
    if (!out.is_open()) {
        std::cerr << "Failed to open result file: " << result_path << std::endl;
        return false;
    }

    out << "NATION\tREVENUE\n";
    for (const auto& [nation, revenue] : results) {
        out << nation << "\t" << std::fixed << std::setprecision(2) << revenue << "\n";
    }

    out.close();
    return true;
}

