/*
 * Copyright (c) 2008 INRIA
 *
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
 *
 * Author: Mohamed Amine Ismail <amine.ismail@sophia.inria.fr>
 */
/*
    提供创建普通UDP客户端和服务器的辅助工具
        UdpServer：接收UDP数据包并统计接收数量（不主动响应）
        UdpClient：周期性向指定目标发送UDP数据包
    
    用于单向UDP通信，适合无需响应的场景
*/


#include "udp-client-server-helper.h"

#include "ns3/string.h"
#include "ns3/udp-client.h"
#include "ns3/udp-server.h"
#include "ns3/udp-trace-client.h"
#include "ns3/uinteger.h"

namespace ns3
{

    // 默认构造函数：设置工厂类型为UdpServer
    UdpServerHelper::UdpServerHelper()
    {   
        m_factory.SetTypeId(UdpServer::GetTypeId()); // 工厂生产UdpServer对象
    }

    // 带端口参数的构造函数：设置监听端口属性 
    UdpServerHelper::UdpServerHelper(uint16_t port)
    {
        m_factory.SetTypeId(UdpServer::GetTypeId());
        SetAttribute("Port", UintegerValue(port));
    }

    //设置属性
    void
    UdpServerHelper::SetAttribute(std::string name, const AttributeValue& value)
    {
        m_factory.Set(name, value);     // 将属性保存到工厂，后续创建对象时生效
    }

    //在节点容器中安装服务器应用
    ApplicationContainer
    UdpServerHelper::Install(NodeContainer c)
    {
        ApplicationContainer apps;                      // 步骤1：创建空应用容器
        for (NodeContainer::Iterator i = c.Begin(); i != c.End(); ++i)
        {
            Ptr<Node> node = *i;                        // 获取当前节点指针

            m_server = m_factory.Create<UdpServer>();   // 通过工厂创建UdpServer实例
            node->AddApplication(m_server);             // 将应用绑定到节点
            apps.Add(m_server);                         // 添加到应用容器
        }
        return apps;
    }

    // 获取最后一个创建的服务器实例（用于调试或直接操作）
    Ptr<UdpServer>
    UdpServerHelper::GetServer()
    {
        return m_server;
    }


    //默认构造函数：设置工厂类型为UdpClient
    UdpClientHelper::UdpClientHelper()
    {
        m_factory.SetTypeId(UdpClient::GetTypeId());
    }

    // 指定目标IP和端口的构造函数
    UdpClientHelper::UdpClientHelper(Address address, uint16_t port)
    {
        m_factory.SetTypeId(UdpClient::GetTypeId());
        SetAttribute("RemoteAddress", AddressValue(address));   // 设置目标地址
        SetAttribute("RemotePort", UintegerValue(port));        // 设置目标端口
    }

    //使用地址对象（如InetSocketAddress）的构造函数
    UdpClientHelper::UdpClientHelper(Address address)
    {
        m_factory.SetTypeId(UdpClient::GetTypeId());
        SetAttribute("RemoteAddress", AddressValue(address));
    }

    // 设置属性（如Interval、MaxPackets、PacketSize）
    void
    UdpClientHelper::SetAttribute(std::string name, const AttributeValue& value)
    {
        m_factory.Set(name, value);
    }

    // 在节点容器中安装客户端应用
    ApplicationContainer
    UdpClientHelper::Install(NodeContainer c)
    {
        ApplicationContainer apps;
        for (NodeContainer::Iterator i = c.Begin(); i != c.End(); ++i)
        {
            Ptr<Node> node = *i;
            Ptr<UdpClient> client = m_factory.Create<UdpClient>();
            node->AddApplication(client);
            apps.Add(client);
        }
        return apps;
    }

    /*  UdpTraceClientHelper 实现   */

    //默认构造函数：设置工厂类型为UdpTraceClient
    UdpTraceClientHelper::UdpTraceClientHelper()
    {
        m_factory.SetTypeId(UdpTraceClient::GetTypeId());
    }

    // 指定目标IP、端口和跟踪文件的构造函数
    UdpTraceClientHelper::UdpTraceClientHelper(Address address, uint16_t port, std::string filename)
    {
        m_factory.SetTypeId(UdpTraceClient::GetTypeId());
        SetAttribute("RemoteAddress", AddressValue(address));
        SetAttribute("RemotePort", UintegerValue(port));
        SetAttribute("TraceFilename", StringValue(filename));
    }

    UdpTraceClientHelper::UdpTraceClientHelper(Address address, std::string filename)
    {
        m_factory.SetTypeId(UdpTraceClient::GetTypeId());
        SetAttribute("RemoteAddress", AddressValue(address));
        SetAttribute("TraceFilename", StringValue(filename));
    }

    // 设置属性（如PacketSizeAdjustment）
    void
    UdpTraceClientHelper::SetAttribute(std::string name, const AttributeValue& value)
    {
        m_factory.Set(name, value);
    }

    // 在节点容器中安装跟踪客户端应用
    ApplicationContainer
    UdpTraceClientHelper::Install(NodeContainer c)
    {
        ApplicationContainer apps;
        for (NodeContainer::Iterator i = c.Begin(); i != c.End(); ++i)
        {
            Ptr<Node> node = *i;
            Ptr<UdpTraceClient> client = m_factory.Create<UdpTraceClient>();
            node->AddApplication(client);
            apps.Add(client);
        }
        return apps;
    }

} // namespace ns3
