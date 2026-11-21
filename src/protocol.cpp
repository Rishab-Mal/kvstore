#include "protocol.hpp"

#include <cctype>
#include <exception>

#include "store.hpp"

namespace kv {

namespace {

bool is_space(char c) { return c == ' ' || c == '\t'; }

std::string_view trim_left(std::string_view s) {
    size_t i = 0;
    while (i < s.size() && is_space(s[i])) ++i;
    return s.substr(i);
}

std::string_view next_token(std::string_view& rest) {
    rest = trim_left(rest);
    size_t end = 0;
    while (end < rest.size() && !is_space(rest[end])) ++end;
    std::string_view token = rest.substr(0, end);
    rest.remove_prefix(end);
    return token;
}

bool iequals(std::string_view a, std::string_view b) {
    if (a.size() != b.size()) return false;
    for (size_t i = 0; i < a.size(); ++i) {
        if (std::toupper(static_cast<unsigned char>(a[i])) !=
            std::toupper(static_cast<unsigned char>(b[i]))) {
            return false;
        }
    }
    return true;
}

Command invalid(std::string message) {
    Command cmd;
    cmd.error = std::move(message);
    return cmd;
}

Command wrong_args(std::string_view name) {
    return invalid("wrong number of arguments for '" + std::string(name) + "'");
}

}

Command parse(std::string_view line) {
    if (!line.empty() && line.back() == '\r') line.remove_suffix(1);

    std::string_view rest = line;
    const std::string_view name = next_token(rest);
    if (name.empty()) return invalid("empty command");

    if (iequals(name, "GET") || iequals(name, "DEL")) {
        const bool is_get = iequals(name, "GET");
        Command cmd;
        cmd.type = is_get ? CmdType::Get : CmdType::Del;
        cmd.key = next_token(rest);
        if (cmd.key.empty() || !trim_left(rest).empty()) return wrong_args(is_get ? "GET" : "DEL");
        return cmd;
    }

    if (iequals(name, "SET")) {
        Command cmd;
        cmd.type = CmdType::Set;
        cmd.key = next_token(rest);
        cmd.value = trim_left(rest);
        if (cmd.key.empty() || cmd.value.empty()) return wrong_args("SET");
        return cmd;
    }

    constexpr size_t kMaxEcho = 32;
    return invalid("unknown command '" + std::string(name.substr(0, kMaxEcho)) + "'");
}

void execute_into(Store& store, const Command& cmd, std::string& out, uint64_t* log_seq) {
    if (log_seq) *log_seq = 0;
    try {
        switch (cmd.type) {
            case CmdType::Get: {
                const size_t mark = out.size();
                out += "VALUE ";
                if (store.append_value(cmd.key, out)) {
                    out += '\n';
                } else {
                    out.resize(mark);
                    out += "NOT_FOUND\n";
                }
                return;
            }
            case CmdType::Set:
                store.set(cmd.key, cmd.value, log_seq);
                out += "OK\n";
                return;
            case CmdType::Del:
                out += store.del(cmd.key, log_seq) ? "DELETED\n" : "NOT_FOUND\n";
                return;
            case CmdType::Invalid:
                out += "ERR ";
                out += cmd.error;
                out += '\n';
                return;
        }
    } catch (const std::exception& e) {
        out += "ERR ";
        out += e.what();
        out += '\n';
    }
}

std::string execute(Store& store, const Command& cmd) {
    std::string out;
    execute_into(store, cmd, out);
    return out;
}

}
