#include "ns3/core-module.h"
#include <json/json.h>  // 引入 jsoncpp 库
#include <iostream>

using namespace ns3;

int main(int argc, char *argv[])
{
    // 创建一个简单的 JSON 数据
    Json::Value root;
    root["name"] = "NS3 Example";
    root["version"] = 3.37;
    root["status"] = "success";

    // 将 JSON 数据转换为字符串
    Json::StreamWriterBuilder writer;
    std::string jsonString = Json::writeString(writer, root);

    // 打印 JSON 数据
    std::cout << "Generated JSON: " << jsonString << std::endl;

    return 0;
}
