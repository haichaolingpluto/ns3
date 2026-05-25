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
#include "ns3/csma-module.h"
#include "ns3/internet-module.h"
#include "ns3/ipv4-global-routing-helper.h"
#include "ns3/network-module.h"
#include "ns3/point-to-point-module.h"

// Default Network Topology
//
//       10.1.1.0
// n0 -------------- n1   n2   n3   n4
//    point-to-point  |    |    |    |
//                    ================
//                      LAN 10.1.2.0

using namespace ns3;

NS_LOG_COMPONENT_DEFINE("SecondScriptExample");

int
main(int argc, char* argv[])
{
    bool verbose = true; //定义变量，用于决定是否开启2个UdpApplication的Logging组件；默认true开启
    uint32_t nCsma = 3; //LAN中另有3个node

    CommandLine cmd(__FILE__); //命令行
    cmd.AddValue("nCsma", "Number of \"extra\" CSMA nodes/devices", nCsma);
    cmd.AddValue("verbose", "Tell echo applications to log if true", verbose);

    //命令行参数设置是否开启logging
    cmd.Parse(argc, argv);

    if (verbose)
    {
        LogComponentEnable("UdpEchoClientApplication", LOG_LEVEL_INFO);
        LogComponentEnable("UdpEchoServerApplication", LOG_LEVEL_INFO);
    }

    nCsma = nCsma == 0 ? 1 : nCsma;

    //创建使用P2P链路链接的两个node
    NodeContainer p2pNodes;
    p2pNodes.Create(2);

    //创建另一个NodeContainer类对象,用于总线(CSMA)网络
    NodeContainer csmaNodes;
    csmaNodes.Add(p2pNodes.Get(1));//将之前的P2P的NodeContainer的第二个节点添加到CSMA的NodeContainer中去,以获得CSMA device. 这个Node将有两个device(也就是两个网络设备)
    csmaNodes.Create(nCsma);//再创建一个CSMA总线上的一个node

    //设置P2P传输的属性,注意使用helper的固定格式
    PointToPointHelper pointToPoint;
    pointToPoint.SetDeviceAttribute("DataRate", StringValue("5Mbps"));
    pointToPoint.SetChannelAttribute("Delay", StringValue("2ms"));

    //安装P2P网络设备
    NetDeviceContainer p2pDevices;
    p2pDevices = pointToPoint.Install(p2pNodes);

    //设置CSMA传输的属性,注意使用helper的固定格式
    CsmaHelper csma;
    csma.SetChannelAttribute("DataRate", StringValue("100Mbps"));
    csma.SetChannelAttribute("Delay", TimeValue(NanoSeconds(6560)));//因为CSMA同一信道上不允许有多个不同数据率的设备

    //安装CSMA的网络设备
    NetDeviceContainer csmaDevices;
    csmaDevices = csma.Install(csmaNodes);

    //安装网络协议
    InternetStackHelper stack;
    stack.Install(p2pNodes.Get(0));//P2P链路的第一个节点.P2P链路的第二个节点包含在csmaNodes中
    stack.Install(csmaNodes);

    //分配P2P的IP地址
    Ipv4AddressHelper address;
    address.SetBase("10.1.1.0", "255.255.255.0");
    Ipv4InterfaceContainer p2pInterfaces;
    p2pInterfaces = address.Assign(p2pDevices);

    //分配CSMA的IP地址
    address.SetBase("10.1.2.0", "255.255.255.0");
    Ipv4InterfaceContainer csmaInterfaces;
    csmaInterfaces = address.Assign(csmaDevices);

    //监听9号端口
    UdpEchoServerHelper echoServer(9);

    ApplicationContainer serverApps = echoServer.Install(csmaNodes.Get(nCsma));//将Server服务安装在CSMA网段的最后一个结点上,nCsma是可变的,所以不能用3
    serverApps.Start(Seconds(1.0));
    serverApps.Stop(Seconds(10.0));

    UdpEchoClientHelper echoClient(csmaInterfaces.GetAddress(nCsma), 9);
    echoClient.SetAttribute("MaxPackets", UintegerValue(1));
    echoClient.SetAttribute("Interval", TimeValue(Seconds(1.0)));
    echoClient.SetAttribute("PacketSize", UintegerValue(1024));

    //在P2P的第一个节点安装客户端应用程序
    ApplicationContainer clientApps = echoClient.Install(p2pNodes.Get(0));
    clientApps.Start(Seconds(2.0));
    clientApps.Stop(Seconds(10.0));

    Ipv4GlobalRoutingHelper::PopulateRoutingTables();//开启全局路由

    pointToPoint.EnablePcapAll("second");//开启P2PHelper类对象的pcap,"second"为保存文件的前缀名
    csma.EnablePcap("second", csmaDevices.Get(1), true);//开启csmaHelper类对象的pcap,使用csma网段索引为1的设备(第二个),进行sniff,true开启Promiscuous mode
    //前缀后的节点号是"全局符号点",不用担心名称相同
    /*
    csma.EnablePcap:
        这是NS-3中的一个成员函数，用于启用PCAP数据包捕获功能。
        EnablePcap函数可以将指定设备上的网络数据包捕获并保存到文件中，文件名为指定的前缀加上.pcap扩展名。它通常用于调试和数据分析。
        该函数的第一个参数是文件名的前缀，第二个参数是需要捕获数据包的网络设备，第三个参数控制是否启用捕获传出的数据包。
    
    "second":
        这是文件名的前缀。模拟运行后，所有捕获的数据包将被保存到名为second-1.pcap（如果捕获的是csmaDevices.Get(1)的第一个设备）文件中。
        如果你捕获多个设备的数据包，它们会按设备编号生成多个文件（例如：second-0.pcap, second-1.pcap等）。
    
    csmaDevices.Get(1):
        csmaDevices是一个NodeContainer或NetDeviceContainer，它包含多个网络设备（NetDevice）。
        Get(1)函数调用获取csmaDevices容器中的第二个网络设备（索引从0开始）。也就是说，csmaDevices.Get(1)返回第二个NetDevice对象。
        这个设备对象用于指定你要捕获数据包的设备。
    
    true:
        这是EnablePcap的第三个参数，控制是否捕获传出的数据包。如果设置为true，则会捕获该设备发送的数据包；如果设置为false，则只捕获接收到的数据包。
        在这里，true表示你想捕获这个设备的所有数据包，包括发送和接收的数据包。
        
    */

    Simulator::Run();
    Simulator::Destroy();
    return 0;
}
