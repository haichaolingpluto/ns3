/*
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation;
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA  02111-1307  USA
 */

#include "ns3/applications-module.h"
#include "ns3/core-module.h"
#include "ns3/internet-module.h"
#include "ns3/network-module.h"
#include "ns3/point-to-point-module.h"

// Default Network Topology
//
//       10.1.1.0
// n0 -------------- n1
//    point-to-point
//

using namespace ns3;

NS_LOG_COMPONENT_DEFINE("FirstScriptExample");//向ns3系统注册一个名为FirstScriptExample的记录组件。只有定义了记录组件，才能在仿真脚本中使用Logging系统自定义输出语句

int main(int argc, char* argv[])
{

    CommandLine cmd(__FILE__);
    cmd.Parse(argc, argv);//这两行代码比表示用户可以用命令行来访问代码中的全局变量和ns-3中的属性系统


    NS_LOG_INFO("Creating Topology");

    Time::SetResolution(Time::NS);
    LogComponentEnable("UdpEchoClientApplication", LOG_LEVEL_INFO);
    LogComponentEnable("UdpEchoServerApplication", LOG_LEVEL_INFO);

    //1.创建节点
    NodeContainer nodes;
    nodes.Create(2);

    //2.为节点创建P2P类型的链路，并配置链路属性
    PointToPointHelper pointToPoint;
    pointToPoint.SetDeviceAttribute("DataRate", StringValue("5Mbps")); //设置传输速率为5Mbps
    pointToPoint.SetChannelAttribute("Delay", StringValue("2ms")); //设置传输信道传播时延为2ms

    //3.安装链路，生成网卡
    NetDeviceContainer devices;//创建网络设备
    //pointToPoint.Install(nodes)函数内；创建了两个ppp网络设备对象POintToPointDevice和一个ppp信道对象PointToPointChannel。连接节点与信道
    devices = pointToPoint.Install(nodes); 

    //4.安装协议栈
    InternetStackHelper stack;
    stack.Install(nodes); //为网络中的结点安装TCP/IP协议栈

    //5.为网卡配置IP
    Ipv4AddressHelper address;
    address.SetBase("10.1.1.0", "255.255.255.0");//为网络设备分配ip地址。起始地址为10.1.1.0

    //6.生成网络接口
    Ipv4InterfaceContainer interfaces = address.Assign(devices);

    UdpEchoServerHelper echoServer(9);//监听9号端口

    ApplicationContainer serverApps = echoServer.Install(nodes.Get(1));//将Application安装在节点上。
    serverApps.Start(Seconds(1.0)); //Application在第1秒开始运行并接受9号端口的数据
    serverApps.Stop(Seconds(10.0)); //Application在第10秒停止

    //7.配置应用
    UdpEchoClientHelper echoClient(interfaces.GetAddress(1), 9);
    echoClient.SetAttribute("MaxPackets", UintegerValue(1));
    echoClient.SetAttribute("Interval", TimeValue(Seconds(1.0)));
    echoClient.SetAttribute("PacketSize", UintegerValue(1024));

    ApplicationContainer clientApps = echoClient.Install(nodes.Get(0));
    clientApps.Start(Seconds(2.0));
    clientApps.Stop(Seconds(10.0));//在模拟启动后两秒向节点1的9号端口发送一个1024比特的UDP数据包，在模拟启动后10秒停止

    //ASCII Tracing
    //AsciiTraceHelper ascii;
    //pointToPoint.EnableAsciiAll(ascii.CreateFileStream("myfirst.tr"));
    //CreateFileStream("myfirst.tr")创建一个名为myfirst.tr的文件，而函数EnableAsciiAll的作用是通知helper将所有的关于point-to-point设备的仿真信息都打印成ASCII Tracing格式
    //生成的myfirst.tr就包含本文所要收集的信息

    //PCAP Tracing
    pointToPoint.EnableAsciiAll("myfirst");

    //8.开始仿真
    Simulator::Run();
    Simulator::Destroy();
    return 0;
}
