#include "ns3/core-module.h"
#include "ns3/network-module.h"
#include "ns3/internet-module.h"
#include "ns3/mobility-module.h"
#include "ns3/applications-module.h"
#include "ns3/point-to-point-module.h"
#include "ns3/netanim-module.h"

using namespace ns3;
/*
       /-----n2  
n0---n1
       \-----n3

n0发包,n2、n3接受
*/

int main(int argc, char *argv[])
{
    // 启用日志记录（可选）：显示客户端和服务器的通信信息
    LogComponentEnable("UdpEchoClientApplication", LOG_LEVEL_INFO);
    LogComponentEnable("UdpEchoServerApplication", LOG_LEVEL_INFO);

    //创建节点
    NodeContainer p2pNodes;
    p2pNodes.Create(4);

    //创建P2P链路，并设置P2P链路的传输的属性
    PointToPointHelper pointToPoint;
    pointToPoint.SetDeviceAttribute("DataRate", StringValue("5Mbps"));;
    pointToPoint.SetChannelAttribute("Delay", StringValue("2ms"));

    //定义3条链路（不同网段）
    NodeContainer link1 = NodeContainer(p2pNodes.Get(0), p2pNodes.Get(1));
    NodeContainer link2 = NodeContainer(p2pNodes.Get(1), p2pNodes.Get(2));
    NodeContainer link3 = NodeContainer(p2pNodes.Get(1), p2pNodes.Get(3));

    //安装网卡在每条链路的结点上
    NetDeviceContainer p2pdevices1,p2pdevices2,p2pdevices3;
    p2pdevices1 = pointToPoint.Install(link1);
    p2pdevices2 = pointToPoint.Install(link2);
    p2pdevices3 = pointToPoint.Install(link3);

    //安装网络协议
    InternetStackHelper stack;
    stack.Install(p2pNodes);

    //分配第一个链路的IP地址
    Ipv4AddressHelper address;
    Ipv4InterfaceContainer p2pInterfaces_0_1,p2pInterfaces_1_2,p2pInterfaces_1_3;
    address.SetBase("10.1.1.0", "255.255.255.0");
    p2pInterfaces_0_1 = address.Assign(p2pdevices1);
    
    address.SetBase("10.1.2.0", "255.255.255.0");
    p2pInterfaces_1_2 = address.Assign(p2pdevices2);

    address.SetBase("10.1.3.0", "255.255.255.0");
    p2pInterfaces_1_3 = address.Assign(p2pdevices3);


    //多播源为节点0，
    Ipv4Address multicastSource ("10.1.1.1");
    Ipv4Address multicastGroup ("225.1.2.4");

    //配置多播的发送方节点0
    Ipv4StaticRoutingHelper multicast;
    Ptr<Node> sender = p2pNodes.Get(0);
    Ptr<NetDevice> senderIf = p2pdevices1.Get(0);
    multicast.SetDefaultMulticastRoute(sender, senderIf);

    //配置节点1为组播路由器
    Ptr<Node> multicastRouter = p2pNodes.Get (1);
    Ptr<NetDevice> inputIf = p2pdevices1.Get (1);
    NetDeviceContainer outputDevices;
    outputDevices.Add(p2pdevices3.Get(0));
    outputDevices.Add(p2pdevices2.Get(0));
    multicast.AddMulticastRoute(multicastRouter, multicastSource, multicastGroup, inputIf, outputDevices);
    


    //安装接收端应用程序
    ApplicationContainer replicaApps;
    PacketSinkHelper sink ("ns3::UdpSocketFactory",InetSocketAddress(Ipv4Address::GetAny(), 9));
    replicaApps.Add(sink.Install(p2pNodes.Get(3)));
    replicaApps.Add(sink.Install(p2pNodes.Get(2)));
    replicaApps.Start(Seconds(0.0));
    replicaApps.Stop(Seconds(10.0));


 
    
    //安装发送端应用程序
    ApplicationContainer clientApps;
    OnOffHelper onoff ("ns3::UdpSocketFactory",Address (InetSocketAddress ("225.1.2.4", 9)));
    onoff.SetAttribute ("PacketSize", UintegerValue (1024));
    onoff.SetAttribute ("DataRate", DataRateValue (DataRate ("1Mbps")));
    clientApps.Add(onoff.Install(p2pNodes.Get(0)));
    clientApps.Start(Seconds(0.0));
    clientApps.Stop(Seconds(10.0));
  

    // pointToPoint.EnablePcapAll("../ns-3.37/scratch/Pcap_file/mulyicastExample");


    AnimationInterface anim("../ns-3.37/scratch/myBigDataTest/netanim_file/test.xml");
     anim.SetConstantPosition(p2pNodes.Get(0), 45.0, 90.0);
     anim.SetConstantPosition(p2pNodes.Get(1), 20.0, 60.0);
     anim.SetConstantPosition(p2pNodes.Get(2), 60.0, 60.0);
     anim.SetConstantPosition(p2pNodes.Get(3), 45.0, 20.0);

    Simulator::Run ();
    Simulator::Destroy ();
}