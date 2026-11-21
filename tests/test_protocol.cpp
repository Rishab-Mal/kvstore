#include <gtest/gtest.h>

#include <string>

#include "protocol.hpp"
#include "store.hpp"

using kv::CmdType;
using kv::parse;

TEST(Parse, Get) {
    const auto c = parse("GET foo");
    EXPECT_TRUE(c.type == CmdType::Get);
    EXPECT_EQ(c.key, "foo");
}

TEST(Parse, Set) {
    const auto c = parse("SET foo bar");
    EXPECT_TRUE(c.type == CmdType::Set);
    EXPECT_EQ(c.key, "foo");
    EXPECT_EQ(c.value, "bar");
}

TEST(Parse, SetValueKeepsInnerSpaces) {
    const auto c = parse("SET greeting hello there   world");
    EXPECT_TRUE(c.type == CmdType::Set);
    EXPECT_EQ(c.value, "hello there   world");
}

TEST(Parse, Del) {
    const auto c = parse("DEL foo");
    EXPECT_TRUE(c.type == CmdType::Del);
    EXPECT_EQ(c.key, "foo");
}

TEST(Parse, CaseInsensitiveCommandNames) {
    EXPECT_TRUE(parse("get a").type == CmdType::Get);
    EXPECT_TRUE(parse("SeT a b").type == CmdType::Set);
    EXPECT_TRUE(parse("del a").type == CmdType::Del);
}

TEST(Parse, KeysAreCaseSensitive) {
    EXPECT_EQ(parse("GET Foo").key, "Foo");
}

TEST(Parse, StripsCarriageReturn) {
    const auto c = parse("GET foo\r");
    EXPECT_TRUE(c.type == CmdType::Get);
    EXPECT_EQ(c.key, "foo");
}

TEST(Parse, ToleratesExtraWhitespace) {
    const auto c = parse("  GET \t foo  ");
    EXPECT_TRUE(c.type == CmdType::Get);
    EXPECT_EQ(c.key, "foo");
}

TEST(Parse, EmptyLine) {
    const auto c = parse("");
    EXPECT_TRUE(c.type == CmdType::Invalid);
    EXPECT_EQ(c.error, "empty command");
}

TEST(Parse, WhitespaceOnlyLine) {
    EXPECT_TRUE(parse("   \t\r").type == CmdType::Invalid);
}

TEST(Parse, UnknownCommand) {
    const auto c = parse("FLY away");
    EXPECT_TRUE(c.type == CmdType::Invalid);
    EXPECT_EQ(c.error, "unknown command 'FLY'");
}

TEST(Parse, UnknownCommandEchoIsCapped) {
    const auto c = parse(std::string(1000, 'Q'));
    EXPECT_TRUE(c.type == CmdType::Invalid);
    EXPECT_TRUE(c.error.size() < 100);
}

TEST(Parse, WrongArgumentCounts) {
    EXPECT_EQ(parse("GET").error, "wrong number of arguments for 'GET'");
    EXPECT_EQ(parse("GET a b").error, "wrong number of arguments for 'GET'");
    EXPECT_EQ(parse("DEL").error, "wrong number of arguments for 'DEL'");
    EXPECT_EQ(parse("DEL a b").error, "wrong number of arguments for 'DEL'");
    EXPECT_EQ(parse("SET").error, "wrong number of arguments for 'SET'");
    EXPECT_EQ(parse("SET a").error, "wrong number of arguments for 'SET'");
    EXPECT_EQ(parse("SET a   ").error, "wrong number of arguments for 'SET'");
}

TEST(Parse, VeryLongValue) {
    const std::string value(1024 * 1024, 'v');
    const auto c = parse("SET big " + value);
    EXPECT_TRUE(c.type == CmdType::Set);
    EXPECT_EQ(c.value.size(), value.size());
}

TEST(Execute, Replies) {
    kv::Store store;
    EXPECT_EQ(kv::execute(store, parse("GET a")), "NOT_FOUND\n");
    EXPECT_EQ(kv::execute(store, parse("SET a 1")), "OK\n");
    EXPECT_EQ(kv::execute(store, parse("GET a")), "VALUE 1\n");
    EXPECT_EQ(kv::execute(store, parse("SET a two words")), "OK\n");
    EXPECT_EQ(kv::execute(store, parse("GET a")), "VALUE two words\n");
    EXPECT_EQ(kv::execute(store, parse("DEL a")), "DELETED\n");
    EXPECT_EQ(kv::execute(store, parse("DEL a")), "NOT_FOUND\n");
    EXPECT_EQ(kv::execute(store, parse("")), "ERR empty command\n");
    EXPECT_EQ(kv::execute(store, parse("SET a")), "ERR wrong number of arguments for 'SET'\n");
}

TEST(Execute, IntoAppendsReplies) {
    kv::Store store;
    std::string out;
    kv::execute_into(store, parse("SET a 1"), out);
    kv::execute_into(store, parse("GET a"), out);
    kv::execute_into(store, parse("GET b"), out);
    EXPECT_EQ(out, "OK\nVALUE 1\nNOT_FOUND\n");
}

TEST(Execute, VeryLongValueRoundTrip) {
    kv::Store store;
    const std::string value(1024 * 1024, 'v');
    const std::string line = "SET big " + value;
    EXPECT_EQ(kv::execute(store, parse(line)), "OK\n");
    EXPECT_EQ(kv::execute(store, parse("GET big")), "VALUE " + value + "\n");
}
