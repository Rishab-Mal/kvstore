#pragma once

#include <cstdint>
#include <string>
#include <string_view>

namespace kv {

class Store;

enum class CmdType { Get, Set, Del, Invalid };

struct Command {
    CmdType type = CmdType::Invalid;
    std::string_view key;
    std::string_view value;
    std::string error;
};

Command parse(std::string_view line);

std::string execute(Store& store, const Command& cmd);

void execute_into(Store& store, const Command& cmd, std::string& out, uint64_t* log_seq = nullptr);

}
