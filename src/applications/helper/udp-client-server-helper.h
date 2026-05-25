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
#ifndef UDP_CLIENT_SERVER_HELPER_H    //防止头文件重复包含
#define UDP_CLIENT_SERVER_HELPER_H

#include "ns3/application-container.h"    //管理应用程序容器
#include "ns3/ipv4-address.h"     //IPv4地址操作
#include "ns3/node-container.h"     //节点容器管理
#include "ns3/object-factory.h"     //动态创建对象（工厂模式）
#include "ns3/udp-client.h"
#include "ns3/udp-server.h"         //UDP应用的具体实现类。

#include <stdint.h>

namespace ns3
{
/**创建UDP服务器应用的辅助类，用于接收数据包并统计信息
 * 
 * \ingroup udpclientserver
 * \brief Create a server application which waits for input UDP packets
 *        and uses the information carried into their payload to compute
 *        delay and to determine if some packets are lost.
 */
class UdpServerHelper
{
  public:
    /**
     * Create UdpServerHelper which will make life easier for people trying
     * to set up simulations with udp-client-server application.
     *
     */
    UdpServerHelper();  // 默认构造函数

    /**
     * Create UdpServerHelper which will make life easier for people trying
     * to set up simulations with udp-client-server application.
     *
     * \param port The port the server will wait on for incoming packets
     */
    UdpServerHelper(uint16_t port);   // 指定端口的构造函数

    /**
     * Record an attribute to be set in each Application after it is is created.
     *
     * \param name the name of the attribute to set
     * \param value the value of the attribute to set
     */
    void SetAttribute(std::string name, const AttributeValue& value);

    /**
     * Create one UDP server application on each of the Nodes in the
     * NodeContainer.
     *
     * \param c The nodes on which to create the Applications.  The nodes
     *          are specified by a NodeContainer.
     * \returns The applications created, one Application per Node in the
     *          NodeContainer.
     */
    ApplicationContainer Install(NodeContainer c);

    /**
     * \brief Return the last created server.
     *
     * This function is mainly used for testing.
     *
     * \returns a Ptr to the last created server application
     */
    Ptr<UdpServer> GetServer(); // 获取最后一个创建的服务器实例,这个功能主要被用来测试

  private:
    ObjectFactory m_factory; //!< Object factory. 对象工厂，用于创建UdpServer
    Ptr<UdpServer> m_server; //!< The last created server application. 保存最后一个服务器实例的指针
};

/**
 * \ingroup udpclientserver
 * \brief Create a client application which sends UDP packets carrying
 *  a 32bit sequence number and a 64 bit time stamp.
 *
 */
class UdpClientHelper
{
  public:
    /**
     * Create UdpClientHelper which will make life easier for people trying
     * to set up simulations with udp-client-server.
     *
     */
    UdpClientHelper();  //默认构造函数

    /**
     *  Create UdpClientHelper which will make life easier for people trying
     * to set up simulations with udp-client-server. Use this variant with
     * addresses that do not include a port value (e.g., Ipv4Address and
     * Ipv6Address).
     *
     * \param ip The IP address of the remote UDP server
     * \param port The port number of the remote UDP server
     */

    UdpClientHelper(Address ip, uint16_t port);   // 指定目标IP和端口


    /**
     *  Create UdpClientHelper which will make life easier for people trying
     * to set up simulations with udp-client-server. Use this variant with
     * addresses that do include a port value (e.g., InetSocketAddress and
     * Inet6SocketAddress).
     *
     * \param addr The address of the remote UDP server
     */

    UdpClientHelper(Address addr);  // 直接使用地址对象（如InetSocketAddress）

    /**
     * Record an attribute to be set in each Application after it is is created.
     *
     * \param name the name of the attribute to set
     * \param value the value of the attribute to set
     */
    void SetAttribute(std::string name, const AttributeValue& value);

    /**
     * \param c the nodes
     *
     * Create one UDP client application on each of the input nodes
     *
     * \returns the applications created, one application per input node.
     */
    ApplicationContainer Install(NodeContainer c);  

  private:
    ObjectFactory m_factory; //!< Object factory.   对象工厂，用于创建UdpClient
};

/**
 * \ingroup udpclientserver
 * Create UdpTraceClient application which sends UDP packets based on a trace
 * file of an MPEG4 stream. Trace files could be downloaded form :
 * https://web.archive.org/web/20190907061916/http://www2.tkn.tu-berlin.de/research/trace/ltvt.html
 * (the 2 first lines of the file should be removed)
 * A valid trace file is a file with 4 columns:
 * \li -1- the first one represents the frame index
 * \li -2- the second one indicates the type of the frame: I, P or B
 * \li -3- the third one indicates the time on which the frame was generated by the encoder
 * \li -4- the fourth one indicates the frame size in byte
 */
class UdpTraceClientHelper 
//基于MPEG4流跟踪文件生成UDP流量（模拟真实流量模式）
//构造函数：指定目标地址和跟踪文件路径 
{
  public:
    /**
     * Create UdpTraceClientHelper which will make life easier for people trying
     * to set up simulations with udp-client-server.
     *
     */
    UdpTraceClientHelper();

    /**
     * Create UdpTraceClientHelper which will make life easier for people trying
     * to set up simulations with udp-client-server. Use this variant with
     * addresses that do not include a port value (e.g., Ipv4Address and
     * Ipv6Address).
     *
     * \param ip The IP address of the remote UDP server
     * \param port The port number of the remote UDP server
     * \param filename the file from which packet traces will be loaded
     */
    UdpTraceClientHelper(Address ip, uint16_t port, std::string filename);
    /**
     * Create UdpTraceClientHelper which will make life easier for people trying
     * to set up simulations with udp-client-server. Use this variant with
     * addresses that do include a port value (e.g., InetSocketAddress and
     * Inet6SocketAddress).
     *
     * \param addr The address of the remote UDP server
     * \param filename the file from which packet traces will be loaded
     */
    UdpTraceClientHelper(Address addr, std::string filename);//使用时需搭配包含端口号的地址类型（例如InetSocketAddress和Inet6SocketAddress）。

    /**
     * Record an attribute to be set in each Application after it is is created.
     *
     * \param name the name of the attribute to set
     * \param value the value of the attribute to set
     */
    void SetAttribute(std::string name, const AttributeValue& value);

    /**
     * \param c the nodes
     *
     * Create one UDP trace client application on each of the input nodes
     *
     * \returns the applications created, one application per input node.
     */
    ApplicationContainer Install(NodeContainer c);

  private:
    ObjectFactory m_factory; //!< Object factory. 对象工厂，用于创建UdpTraceClient对象
};

} // namespace ns3

#endif /* UDP_CLIENT_SERVER_H */
