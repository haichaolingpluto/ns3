#include "ns3/core-module.h"
#include "ns3/network-module.h"
#include "ns3/internet-module.h"
#include "ns3/applications-module.h"
#include "ns3/csma-module.h"
#include "ns3/ipv4-global-routing-helper.h"
#include "ns3/flow-monitor-module.h"

#include <iostream>
#include <sstream>
#include <thread>
#include <mutex>
#include <deque>
#include <chrono>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <string>
#include <atomic>
#include <random>
#include <vector>
#include <cmath>
#include <map>
#include <unordered_map>
#include <unordered_set>
#include <fstream>
#include <algorithm>
#include <iomanip>
#include <regex>
#include <cstring>

using namespace ns3;

// ============================================================================
// 端口约定（控制面 + 数据面）
// ============================================================================
static const uint16_t PORT_TASK_IN   = 9000; // Host -> in1 (TASKREQ 控制面)
static const uint16_t PORT_TO_OUT    = 9001; // in1  -> out (TASK 控制面转发)
static const uint16_t PORT_TO_WORKER = 9100; // out  -> Worker (TASK 控制面)
static const uint16_t PORT_PROBE_OUT_TO_WORKER = 9250; // out -> Worker PROBE

static const uint16_t PORT_STATUS_WORKER_TO_OUT = 9300; // Worker -> out STATUS
static const uint16_t PORT_STATUS_OUT_TO_IN     = 9301; // out -> in1 STATUS

// 选路结果通知：out -> Host（控制面）
static const uint16_t PORT_ASSIGN_OUT_TO_HOST = 9400;

// 数据面必须经过 out
static const uint16_t PORT_DATA_IN  = 9500; // Host -> out -> Worker
static const uint16_t PORT_DATA_OUT = 9501; // Worker -> out -> Host

// ACK：Host 收齐 output 后通知 in1（任务完成）
static const uint16_t PORT_ACK_HOST_TO_IN = 9600;


// ============================================================================
// 全局参数
// ============================================================================
static std::string g_modeStr = "active"; // 兼容旧命令行：不再分 active/passive，仅保留参数不报错
static double g_probePeriod = 1.0;       // out 主动探测周期
static double g_utilPeriod  = 1.0;       // worker UTIL 采样周期

static uint32_t g_plannedJobs = 20;
static std::string g_tasksFile = "";     // tasks.csv
static std::string g_csvPath = "";       // events csv

// 任务产生时间：截断正态 N(mu, sigma^2) ，限制在 [lo, hi]
static double   g_submitLo    = 0.0;
static double   g_submitHi    = 10.0;
static double   g_submitMu    = 5.0;      // 默认均值
static double   g_submitSigma = 1.6667;   // 默认标准差（大致让 0~10 覆盖 3σ）
static uint64_t g_submitSeed  = 12345;    // 可复现实验

// ================= Console noise control =================
static bool   g_quietProbe = true;      // 默认静默 PROBE/PROBE-STATUS
static double g_anomPrintPeriod = 10.0; // 异常打印最小间隔（秒）
static double g_stuckTimeout = 60.0;    // 多久没“进展”就认为卡住（秒）

static std::map<std::string, double> g_lastAnomPrint; // 每个节点上次异常打印时间
static double g_lastProgressTime = 0.0;               // 最近一次“进展”时间（任务推进）

//对task进行调整
static double   g_bytesScale   = 1.0;   // 例如 0.01 缩小 100 倍
static uint64_t g_capInBytes   = 0;     // 0=不裁剪，否则每任务输入上限
static uint64_t g_capOutBytes  = 0;
static uint64_t g_capCtrlBytes = 0;


// 数据面发送速率
static std::string g_linkRateStr = "250Mbps";  // 链路带宽（CSMA）
static std::string g_appRateStr  = "250Mbps";  // 数据面发送速率（Host/Worker tick）
static DataRate    g_appRate("250Mbps");


// tick 发送：每 tick 批量发若干大包，减少事件量
static double g_flowTick = 0.05;         // 50ms
static uint32_t g_maxUdpPayload = 60000; // 60KB payload（配合 MTU=65535 避免分片/爆炸）

// ============================================================================
// FlowMonitor
// ============================================================================
static bool g_enableFlowMon = true;
static std::string g_flowPrefix = "flowmon";
static FlowMonitorHelper g_flowHelper;
static Ptr<FlowMonitor> g_flowMon;
static Ptr<Ipv4FlowClassifier> g_flowClassifier;

// ============================================================================
// 轻量 CSV Logger（论文友好）
// ============================================================================
static std::mutex g_csvMutex;
static std::ofstream g_csv;
static bool g_csvEnabled = false;

static void CsvOpen(const std::string &path)
{
  if (path.empty()) return;
  g_csv.open(path, std::ios::out | std::ios::trunc);
  if (!g_csv.is_open()) {
    std::cerr << "[CSV] failed to open: " << path << "\n";
    return;
  }
  g_csvEnabled = true;
  g_csv << "time,event,node,taskId,bytes_or_N,ctas,server,base,used,free,extra\n";
  g_csv.flush();
  std::cout << "[CSV] write to " << path << "\n";
}

static void CsvClose()
{
  if (!g_csvEnabled) return;
  std::lock_guard<std::mutex> lk(g_csvMutex);
  g_csv.flush();
  g_csv.close();
  g_csvEnabled = false;
}

static void CsvEvent(double t,
                     const std::string &event,
                     const std::string &node,
                     uint64_t taskId,
                     uint64_t bytesOrN,
                     uint32_t ctas,
                     const std::string &server,
                     uint32_t base,
                     uint32_t used,
                     uint32_t free,
                     const std::string &extra)
{
  if (!g_csvEnabled) return;
  std::lock_guard<std::mutex> lk(g_csvMutex);

  auto esc = [](std::string s){
    for (auto &c: s) if (c==',') c=';';
    return s;
  };

  g_csv << t << ","
        << esc(event) << ","
        << esc(node) << ","
        << taskId << ","
        << bytesOrN << ","
        << ctas << ","
        << esc(server) << ","
        << base << ","
        << used << ","
        << free << ","
        << esc(extra)
        << "\n";
}

// ============================================================================
// 工具：Ipv4Address -> string（不要用 ToString）
// ============================================================================
static std::string IpToString(Ipv4Address a)
{
  std::ostringstream oss;
  oss << a;
  return oss.str();
}

// ============================================================================
// 字符串解析工具（控制面消息用）
// ============================================================================
static uint64_t ParseU64 (const std::string &s, const std::string &key)
{
  size_t p = s.find(key);
  if (p == std::string::npos) return 0;
  p += key.size();
  size_t q = p;
  while (q < s.size() && !isspace((unsigned char)s[q])) q++;
  return std::stoull(s.substr(p, q - p));
}
static uint32_t ParseU32 (const std::string &s, const std::string &key)
{
  size_t p = s.find(key);
  if (p == std::string::npos) return 0;
  p += key.size();
  size_t q = p;
  while (q < s.size() && !isspace((unsigned char)s[q])) q++;
  return (uint32_t) std::stoul(s.substr(p, q - p));
}
static double ParseF64 (const std::string &s, const std::string &key)
{
  size_t p = s.find(key);
  if (p == std::string::npos) return 0.0;
  p += key.size();
  size_t q = p;
  while (q < s.size() && !isspace((unsigned char)s[q])) q++;
  return std::stod(s.substr(p, q - p));
}
static std::string ParseStr (const std::string &s, const std::string &key)
{
  size_t p = s.find(key);
  if (p == std::string::npos) return "";
  p += key.size();
  size_t q = p;
  while (q < s.size() && !isspace((unsigned char)s[q])) q++;
  return s.substr(p, q - p);
}

// ============================================================================
// UDP 发送/接收（字符串）
// ============================================================================
static void UdpSendStr(Ptr<Socket> sock, Ipv4Address dst, uint16_t port, const std::string &msg)
{
  Ptr<Packet> p = Create<Packet>((const uint8_t*)msg.data(), msg.size());
  sock->SendTo(p, 0, InetSocketAddress(dst, port));
}

static std::string RecvStr(Ptr<Socket> sock, Address &from)
{
  Ptr<Packet> p = sock->RecvFrom(from);
  if (!p) return "";
  std::string s;
  s.resize(p->GetSize());
  p->CopyData((uint8_t*)s.data(), s.size());
  return s;
}


static bool ShouldPrintAnom(const std::string &key)
{
  // 同一个 key 最多每 5s 打印一次，避免刷屏
  static std::map<std::string, double> last;
  double now = ns3::Simulator::Now().GetSeconds();
  auto it = last.find(key);
  if (it != last.end() && (now - it->second) < 5.0) return false;
  last[key] = now;
  return true;
}


// ============================================================================
// 数据面 Header：taskId(8) + kind(1)
// kind=1 input, kind=2 output
// ============================================================================
class DataHeader : public Header
{
public:
  DataHeader() : m_taskId(0), m_kind(0) {}
  DataHeader(uint64_t id, uint8_t kind) : m_taskId(id), m_kind(kind) {}

  static TypeId GetTypeId()
  {
    static TypeId tid = TypeId("ns3::DataHeader")
      .SetParent<Header>()
      .AddConstructor<DataHeader>();
    return tid;
  }
  virtual TypeId GetInstanceTypeId() const override { return GetTypeId(); }

  virtual void Serialize(Buffer::Iterator start) const override
  {
    start.WriteHtonU64(m_taskId);
    start.WriteU8(m_kind);
  }

  virtual uint32_t Deserialize(Buffer::Iterator start) override
  {
    m_taskId = start.ReadNtohU64();
    m_kind   = start.ReadU8();
    return GetSerializedSize();
  }

  virtual uint32_t GetSerializedSize() const override { return 8 + 1; }

  virtual void Print(std::ostream &os) const override
  {
    os << "id=" << m_taskId << " kind=" << (uint32_t)m_kind;
  }

  uint64_t GetTaskId() const { return m_taskId; }
  uint8_t  GetKind()   const { return m_kind; }

private:
  uint64_t m_taskId;
  uint8_t  m_kind;
};

NS_OBJECT_ENSURE_REGISTERED(DataHeader);

static Ptr<Packet> MakeDataPacket(uint64_t taskId, uint8_t kind, uint32_t payloadBytes)
{
  Ptr<Packet> p = Create<Packet>(payloadBytes);
  DataHeader h(taskId, kind);
  p->AddHeader(h); // ✅ ns-3 标准方式：把 header 加到包头
  return p;
}

static bool ParseDataPacket(Ptr<Packet> p, uint64_t &taskId, uint8_t &kind, uint32_t &payloadBytes)
{
  if (!p) return false;
  if (p->GetSize() < (8 + 1)) return false;

  DataHeader h;
  p->PeekHeader(h); // ✅ 不移除 header，方便 out 转发时保持包结构
  taskId = h.GetTaskId();
  kind   = h.GetKind();
  payloadBytes = p->GetSize() - (8 + 1);
  return true;
}


// ============================================================================
// GPU 规格 & Kernel 形状（CTA baseline 计算）
// ============================================================================
struct GpuSpec {
  uint32_t sms         = 64;
  uint32_t threadsPerSM= 2048;
  uint32_t regsPerSM   = 65536;
  uint32_t smemPerSM   = 98304;
  uint32_t blocksPerSM = 32;
};

struct KernelShape {
  uint32_t tpb  = 256;
  uint32_t regs = 40;
  uint32_t smem = 16384;
};

static uint32_t ComputeBaseline (const GpuSpec &s, const KernelShape &k)
{
  if (k.tpb == 0) return 0;
  uint64_t totalThreads = uint64_t(s.sms) * s.threadsPerSM;
  uint64_t totalRegs    = uint64_t(s.sms) * s.regsPerSM;
  uint64_t totalSmem    = uint64_t(s.sms) * s.smemPerSM;
  uint64_t totalBlocks  = uint64_t(s.sms) * s.blocksPerSM;

  uint64_t perThreads = k.tpb;
  uint64_t perRegs    = uint64_t(k.regs) * k.tpb;
  uint64_t perSmem    = k.smem;

  uint64_t byThreads = perThreads ? (totalThreads / perThreads) : totalThreads;
  uint64_t byRegs    = perRegs    ? (totalRegs    / perRegs)    : totalRegs;
  uint64_t bySmem    = perSmem    ? (totalSmem    / perSmem)    : totalSmem;
  uint64_t byBlocks  = totalBlocks;

  uint64_t m1 = std::min(byThreads, byRegs);
  uint64_t m2 = std::min(bySmem, byBlocks);
  uint64_t m  = std::min(m1, m2);
  return (uint32_t)m;
}

// ============================================================================
// Accel-Sim 映射脚本：gpu_time_raw.sh N -> ms（返回 s）
// ============================================================================
static double RunGpuScript(uint64_t N)
{
  std::ostringstream cmd;
  cmd << "/home/a/accel-sim-framework/gpu_time_raw.sh " << N;

  FILE *pipe = popen(cmd.str().c_str(), "r");
  if (!pipe) {
    std::cerr << "[Worker] popen failed: " << cmd.str() << "\n";
    return 1.0;
  }

  char buf[256];
  std::string output;
  while (fgets(buf, sizeof(buf), pipe) != nullptr) output += buf;

  int rc = pclose(pipe);
  if (rc != 0) {
    std::cerr << "[Worker] script rc=" << rc << " output=" << output << "\n";
  }

  std::stringstream ss(output);
  double val = 0.0;
  ss >> val;
  if (ss.fail()) {
    std::cerr << "[Worker] script output not double: " << output << "\n";
    return 1.0;
  }
  return val / 1000.0; // ms -> s
}

// ============================================================================
// in1 全局资源表
// ============================================================================
struct ResEntry {
  uint32_t base{0};
  uint32_t used{0};
  uint32_t free{0};
  double   tLast{0.0};
};

static std::mutex g_resMutex;
static std::map<std::string, ResEntry> g_resTable;

static void ResInit(const std::string &node, uint32_t base)
{
  std::lock_guard<std::mutex> lk(g_resMutex);
  g_resTable[node] = ResEntry{base, 0u, base, 0.0};
}

static ResEntry ResGet(const std::string &node)
{
  std::lock_guard<std::mutex> lk(g_resMutex);
  return g_resTable[node];
}

static void ResSet(const std::string &node, uint32_t base, uint32_t used, uint32_t free, double tLast)
{
  std::lock_guard<std::mutex> lk(g_resMutex);
  auto &r = g_resTable[node];
  r.base = base;
  r.used = std::min(used, base);
  r.free = (r.used > base) ? 0 : (base - r.used);
  // 如果 worker 发来的 free 更可信，也可以直接用 free；这里用 used 推导保持一致
  if (free <= base) r.free = free;
  r.tLast = tLast;
}

static void ResAlloc(const std::string &node, uint32_t ctas)
{
  std::lock_guard<std::mutex> lk(g_resMutex);
  auto &r = g_resTable[node];
  r.used = std::min(r.base, r.used + ctas);
  r.free = (r.used > r.base) ? 0 : (r.base - r.used);
}

static std::string ResSummary()
{
  std::lock_guard<std::mutex> lk(g_resMutex);
  std::ostringstream oss;
  for (auto &kv : g_resTable) {
    oss << kv.first << "(b=" << kv.second.base
        << " u=" << kv.second.used
        << " f=" << kv.second.free
        << " t=" << std::fixed << std::setprecision(3) << kv.second.tLast
        << ") ";
  }
  return oss.str();
}

// ============================================================================
// FlowMonitor 分类与导出
// ============================================================================
static std::string ClassByDstPort(uint16_t dstPort)
{
  if (dstPort == PORT_TASK_IN || dstPort == PORT_TO_OUT || dstPort == PORT_TO_WORKER
      || dstPort == PORT_DATA_IN || dstPort == PORT_DATA_OUT) return "DATA_TASK";

  if (dstPort == PORT_PROBE_OUT_TO_WORKER || dstPort == PORT_STATUS_WORKER_TO_OUT
      || dstPort == PORT_STATUS_OUT_TO_IN || dstPort == PORT_ASSIGN_OUT_TO_HOST
      || dstPort == PORT_ACK_HOST_TO_IN) return "CTRL_AD";

  return "OTHER";
}

static void DumpFlowMonitor(const std::string &prefix)
{
  if (!g_enableFlowMon || !g_flowMon || !g_flowClassifier) return;

  g_flowMon->CheckForLostPackets();
  g_flowMon->SerializeToXmlFile(prefix + ".xml", true, true);

  std::ofstream f(prefix + ".csv", std::ios::out | std::ios::trunc);
  f << "flowId,srcAddr,srcPort,dstAddr,dstPort,class,"
       "txPkts,rxPkts,lostPkts,txBytes,rxBytes,"
       "timeFirstTx,timeLastRx,duration,throughput_Mbps,meanDelay_ms,meanJitter_ms\n";

  uint64_t dataBytes = 0, ctrlBytes = 0, otherBytes = 0;
  double globalFirstTx = 1e100;
  double globalLastRx  = 0.0;

  auto stats = g_flowMon->GetFlowStats();
  for (const auto &kv : stats) {
    FlowId id = kv.first;
    const FlowMonitor::FlowStats &st = kv.second;
    Ipv4FlowClassifier::FiveTuple t = g_flowClassifier->FindFlow(id);

    std::string cls = ClassByDstPort(t.destinationPort);

    double firstTx = st.timeFirstTxPacket.GetSeconds();
    double lastRx  = st.timeLastRxPacket.GetSeconds();
    double duration = (lastRx > firstTx) ? (lastRx - firstTx) : 0.0;

    double thrMbps = (duration > 0) ? (st.rxBytes * 8.0 / duration / 1e6) : 0.0;
    double meanDelayMs  = (st.rxPackets > 0) ? (st.delaySum.GetSeconds() / st.rxPackets * 1000.0) : 0.0;
    double meanJitterMs = (st.rxPackets > 1) ? (st.jitterSum.GetSeconds() / (st.rxPackets - 1) * 1000.0) : 0.0;

    f << id << ","
      << t.sourceAddress << "," << t.sourcePort << ","
      << t.destinationAddress << "," << t.destinationPort << ","
      << cls << ","
      << st.txPackets << "," << st.rxPackets << "," << st.lostPackets << ","
      << st.txBytes << "," << st.rxBytes << ","
      << firstTx << "," << lastRx << "," << duration << ","
      << thrMbps << "," << meanDelayMs << "," << meanJitterMs
      << "\n";

    if (cls == "DATA_TASK") dataBytes += st.txBytes;
    else if (cls == "CTRL_AD") ctrlBytes += st.txBytes;
    else otherBytes += st.txBytes;

    if (st.txPackets > 0) globalFirstTx = std::min(globalFirstTx, firstTx);
    if (st.rxPackets > 0) globalLastRx  = std::max(globalLastRx, lastRx);
  }
  f.close();

  double makespan = (globalLastRx > globalFirstTx && globalFirstTx < 1e90) ? (globalLastRx - globalFirstTx) : 0.0;
  double dataMbps  = (makespan > 0) ? (dataBytes * 8.0 / makespan / 1e6) : 0.0;
  double ctrlMbps  = (makespan > 0) ? (ctrlBytes * 8.0 / makespan / 1e6) : 0.0;
  double totalMbps = (makespan > 0) ? ((dataBytes + ctrlBytes + otherBytes) * 8.0 / makespan / 1e6) : 0.0;

  double ctrlShare = (dataBytes + ctrlBytes > 0) ? (double)ctrlBytes / (double)(dataBytes + ctrlBytes) : 0.0;

  std::ofstream s(prefix + "_summary.csv", std::ios::out | std::ios::trunc);
  s << "makespan_s,data_txBytes,ctrl_txBytes,other_txBytes,"
       "data_Mbps,ctrl_Mbps,total_Mbps,ctrl_share\n";
  s << makespan << ","
    << dataBytes << "," << ctrlBytes << "," << otherBytes << ","
    << dataMbps << "," << ctrlMbps << "," << totalMbps << ","
    << ctrlShare << "\n";
  s.close();

  auto cyan  = "\033[36m";
  auto green = "\033[32m";
  auto yellow= "\033[33m";
  auto reset = "\033[0m";

  std::cout << cyan << "\n====================== FlowMonitor Busy Summary ======================\n" << reset;
  std::cout << "  Output Prefix: " << prefix << "  (xml/csv/summary.csv)\n";
  std::cout << "  Makespan(s): " << std::fixed << std::setprecision(6) << makespan << "\n";
  std::cout << green << "  DATA_TASK: txBytes=" << dataBytes << "  Mbps=" << dataMbps << reset << "\n";
  std::cout << yellow<< "  CTRL_AD  : txBytes=" << ctrlBytes << "  Mbps=" << ctrlMbps << "  share=" << ctrlShare << reset << "\n";
  std::cout << "  TOTAL    : txBytes=" << (dataBytes+ctrlBytes+otherBytes) << "  Mbps=" << totalMbps << "\n";
  std::cout << cyan << "=====================================================================\n\n" << reset;

  NS_LOG_UNCOND("[FlowMon] wrote: " << prefix << ".xml/.csv and " << prefix << "_summary.csv");
}

// ============================================================================
// WorkerApp：硬排队 + 输入到齐才能算 + compute done 释放 GPU + 开始输出
// - PROBE：响应 STATUS(reason=PROBE)
// - START：资源变化被动更新 STATUS(reason=START)
// - DONE ：compute done 资源释放 STATUS(reason=DONE freed=...)
// - DATA_IN：从 out 收输入数据，累计到齐 -> 才允许 TryStart
// - DATA_OUT：向 out 发输出数据（大流量）
// ============================================================================
class WorkerApp : public Application
{
public:
  void Setup(const std::string &name,
             const GpuSpec &spec,
             const KernelShape &ks,
             Ipv4Address outOnSbus,   // out 在 out-s 总线上的地址（Worker 发送 STATUS/DATA_OUT 都发给这个）
             double utilPeriod)
  {
    m_name = name;
    m_spec = spec;
    m_ks = ks;
    m_baseline = ComputeBaseline(spec, ks);
    m_outOnSbus = outOnSbus;
    m_utilPeriod = utilPeriod;
  }

private:
  struct Job {
    uint64_t id{0};
    uint64_t N{0};
    uint32_t ctas{0};
    std::string server;

    uint64_t inBytes{0}, outBytes{0};
    uint64_t inRecv{0};
    bool inReady{false};

    double HS{0.0};
    double TIN_ENQ{0.0};
    double TIN_DISP{0.0};

    double TW_RECV{0.0};
    double TIN_START{0.0};
    double TIN_DONE{0.0};
  };

  struct DoneInfo {
    uint64_t id{0};
    double execSec{0.0};
    double tStart{0.0};
    double wallSec{0.0};
  };

  struct OutFlow {
    uint64_t id{0};
    uint64_t remaining{0};
    EventId ev;
    bool started{false};
  };

  virtual void StartApplication() override
  {
    // TASK 控制面
    m_rxTask = Socket::CreateSocket(GetNode(), UdpSocketFactory::GetTypeId());
    m_rxTask->Bind(InetSocketAddress(Ipv4Address::GetAny(), PORT_TO_WORKER));
    m_rxTask->SetRecvCallback(MakeCallback(&WorkerApp::OnTask, this));

    // PROBE 控制面
    m_rxProbe = Socket::CreateSocket(GetNode(), UdpSocketFactory::GetTypeId());
    m_rxProbe->Bind(InetSocketAddress(Ipv4Address::GetAny(), PORT_PROBE_OUT_TO_WORKER));
    m_rxProbe->SetRecvCallback(MakeCallback(&WorkerApp::OnProbe, this));

    // DATA_IN 数据面（从 out 来）
    m_rxDataIn = Socket::CreateSocket(GetNode(), UdpSocketFactory::GetTypeId());
    m_rxDataIn->Bind(InetSocketAddress(Ipv4Address::GetAny(), PORT_DATA_IN));
    m_rxDataIn->SetRecvCallback(MakeCallback(&WorkerApp::OnDataIn, this));

    // STATUS / DATA_OUT 发送
    m_txStatus = Socket::CreateSocket(GetNode(), UdpSocketFactory::GetTypeId());
    m_txDataOut = Socket::CreateSocket(GetNode(), UdpSocketFactory::GetTypeId());

    NS_LOG_UNCOND("[" << m_name << "] Worker start baseline=" << m_baseline
                 << " (S1/S2 同构) ");

    Simulator::Schedule(Seconds(m_utilPeriod), &WorkerApp::LogUtil, this);
    Simulator::Schedule(Seconds(0.02), &WorkerApp::PollDoneQueue, this);
  }

  virtual void StopApplication() override
  {
    if (m_rxTask) { m_rxTask->Close(); m_rxTask = nullptr; }
    if (m_rxProbe){ m_rxProbe->Close(); m_rxProbe = nullptr; }
    if (m_rxDataIn){ m_rxDataIn->Close(); m_rxDataIn = nullptr; }
    if (m_txStatus){ m_txStatus->Close(); m_txStatus = nullptr; }
    if (m_txDataOut){ m_txDataOut->Close(); m_txDataOut = nullptr; }
  }

  uint32_t GetUsedAllocated() const
  {
    uint32_t running = m_runningCtas.load(std::memory_order_relaxed);
    uint32_t queued  = m_queuedCtas.load(std::memory_order_relaxed);
    uint64_t used = (uint64_t)running + (uint64_t)queued;
    if (used > m_baseline) used = m_baseline;
    return (uint32_t)used;
  }

  uint32_t GetFreeAllocated() const
  {
    uint32_t used = GetUsedAllocated();
    return (used > m_baseline) ? 0 : (m_baseline - used);
  }

  void SendStatus(const std::string &reason, uint64_t doneId, uint32_t freed)
  {
    double now = Simulator::Now().GetSeconds();
    uint32_t used = GetUsedAllocated();
    uint32_t free = GetFreeAllocated();

    std::ostringstream oss;
    oss << "STATUS node="<<m_name
        << " base="<<m_baseline
        << " used="<<used
        << " free="<<free
        << " t="<<now
        << " reason="<<reason;

    if (reason == "DONE") {
      oss << " id="<<doneId
          << " freed="<<freed;
    }

    std::string msg = oss.str();
    
    if (reason != "PROBE") {
      NS_LOG_UNCOND("["<<m_name<<"] " << msg); // DONE 等必须打印
    } 
    else {
    // PROBE 的 STATUS 默认静默；只有检测到明显异常才提示
      bool bad = (used > m_baseline) || (free > m_baseline);
      if (!g_quietProbe || (bad && ShouldPrintAnom(m_name + ":PROBE_STATUS_BAD"))) {
        NS_LOG_UNCOND("["<<m_name<<"] " << msg);
      }
    } 

    CsvEvent(now, "STATUS_TX", m_name, doneId, 0, freed, "", m_baseline, used, free, "reason="+reason);
    UdpSendStr(m_txStatus, m_outOnSbus, PORT_STATUS_WORKER_TO_OUT, msg);
  }

  void OnProbe(Ptr<Socket> sock)
  {
    Address from;
    std::string s = RecvStr(sock, from);
    if (s.empty()) return;

    double now = Simulator::Now().GetSeconds();
    
    if (!g_quietProbe) {
      NS_LOG_UNCOND("["<<m_name<<"] PROBE_RX t="<<now);
    }


    CsvEvent(now, "PROBE_RX", m_name, 0, 0, 0, "", m_baseline, GetUsedAllocated(), GetFreeAllocated(), "");

    SendStatus("PROBE", 0, 0);
  }

  void OnTask(Ptr<Socket> sock)
  {
    Address from;
    std::string s = RecvStr(sock, from);
    if (s.empty()) return;

    Job j;
    j.id   = ParseU64(s, "id=");
    j.N    = ParseU64(s, "N=");
    j.ctas = ParseU32(s, "CTAS=");
    j.server = ParseStr(s, "server=");
    j.HS = ParseF64(s, "HS=");
    j.TIN_ENQ = ParseF64(s, "TIN_ENQ=");
    j.TIN_DISP= ParseF64(s, "TIN_DISP=");

    j.inBytes  = ParseU64(s, "IN=");
    j.outBytes = ParseU64(s, "OUT=");
    j.inRecv = 0;
    j.inReady = (j.inBytes == 0);

    j.TW_RECV = Simulator::Now().GetSeconds();

    {
      std::lock_guard<std::mutex> lk(m_qMutex);
      m_queue.push_back(j);
      m_queuedCtas.fetch_add(j.ctas, std::memory_order_relaxed);
      m_jobById[j.id] = j; // 用于 DATA_IN 到达时查
    }

    double now = Simulator::Now().GetSeconds();
    NS_LOG_UNCOND("["<<m_name<<"] WORKER_ENQ id="<<j.id<<" N="<<j.N<<" CTAS="<<j.ctas
                 <<" IN="<<j.inBytes<<" OUT="<<j.outBytes
                 <<" inReady="<<(j.inReady?1:0)
                 <<" TW_RECV="<<j.TW_RECV<<" qlen="<<m_queue.size());

    CsvEvent(now, "WORKER_ENQ", m_name, j.id, j.N, j.ctas, j.server,
             m_baseline, GetUsedAllocated(), GetFreeAllocated(),
             "IN="+std::to_string(j.inBytes)+" OUT="+std::to_string(j.outBytes));

    // 注意：没有收够 input 的任务不能启动
    if (j.inReady) TryStart();
  }

  void OnDataIn(Ptr<Socket> sock)
  {
    Address from;
    Ptr<Packet> p = sock->RecvFrom(from);
    if (!p) return;

    uint64_t id; uint8_t kind; uint32_t payload;
    if (!ParseDataPacket(p, id, kind, payload)) return;
    if (kind != 1) return;

    double now = Simulator::Now().GetSeconds();
    bool becameReady = false;

    {
      std::lock_guard<std::mutex> lk(m_qMutex);
      auto it = m_jobById.find(id);
      if (it == m_jobById.end()) return;

      Job &jj = it->second;
      if (jj.inRecv == 0) jj.TIN_START = now;
      jj.inRecv += payload;

      if (!jj.inReady && jj.inRecv >= jj.inBytes) {
        jj.inReady = true;
        jj.TIN_DONE = now;
        becameReady = true;
      }

      // 同步回队列里同 id 的副本（队列里存的是 Job 拷贝）
      for (auto &qj : m_queue) {
        if (qj.id == id) {
          qj.inRecv = jj.inRecv;
          qj.inReady = jj.inReady;
          qj.TIN_START = jj.TIN_START;
          qj.TIN_DONE  = jj.TIN_DONE;
          break;
        }
      }
    }

    if (becameReady) {
      NS_LOG_UNCOND("["<<m_name<<"] DATA_IN_DONE id="<<id<<" bytes="<<payload<<" t="<<now);
      CsvEvent(now, "DATA_IN_DONE", m_name, id, 0, 0, "", 0,0,0, "");
      TryStart();
    }
  }

  void TryStart()
  {
    while (true)
    {
      Job job;
      {
        std::lock_guard<std::mutex> lk(m_qMutex);
        if (m_queue.empty()) return;

        // 必须 input 到齐
        if (!m_queue.front().inReady) return;

        uint32_t running = m_runningCtas.load(std::memory_order_relaxed);
        if (running + m_queue.front().ctas > m_baseline) return;

        job = m_queue.front();
        m_queue.pop_front();

        m_queuedCtas.fetch_sub(job.ctas, std::memory_order_relaxed);
        m_runningCtas.fetch_add(job.ctas, std::memory_order_relaxed);
      }

      double tStart = Simulator::Now().GetSeconds();

      NS_LOG_UNCOND("["<<m_name<<"] WORKER_START id="<<job.id<<" t="<<tStart
                   <<" running="<<m_runningCtas.load()
                   <<" queued="<<m_queuedCtas.load());

      CsvEvent(tStart, "WORKER_START", m_name, job.id, job.N, job.ctas, job.server,
               m_baseline, GetUsedAllocated(), GetFreeAllocated(), "");

      // 资源变化：被动 STATUS 更新（START）
      SendStatus("START", 0, 0);

      // 记录运行中 job（用于 compute done 后启动 DATA_OUT）
      {
        std::lock_guard<std::mutex> lk(m_runMutex);
        m_runningJobs[job.id] = job;
        m_runningStart[job.id] = tStart;
      }

      // 线程：跑 accel-sim 脚本（绝不调用 Simulator）
      std::thread th([this, job, tStart](){
        auto w0 = std::chrono::steady_clock::now();
        double execSec = RunGpuScript(job.N);
        auto w1 = std::chrono::steady_clock::now();
        double wall = std::chrono::duration<double>(w1-w0).count();

        DoneInfo di;
        di.id = job.id;
        di.execSec = execSec;
        di.tStart = tStart;
        di.wallSec = wall;

        {
          std::lock_guard<std::mutex> lk(m_doneMutex);
          m_doneQ.push_back(di);
        }
      });
      th.detach();
    }
  }

  void PollDoneQueue()
  {
    std::deque<DoneInfo> local;
    {
      std::lock_guard<std::mutex> lk(m_doneMutex);
      local.swap(m_doneQ);
    }

    for (auto &di : local) {
      // 目标完成时间：tStart + execSec（如果脚本返回晚了，则 delay=0 立即完成）
      double target = di.tStart + di.execSec;
      double now = Simulator::Now().GetSeconds();
      double delay = target - now;
      if (delay < 0) delay = 0;

      Simulator::Schedule(Seconds(delay), &WorkerApp::OnComputeDone, this, di.id, di.execSec, di.wallSec, di.tStart);
    }

    Simulator::Schedule(Seconds(0.02), &WorkerApp::PollDoneQueue, this);
  }

  void StartDataOut(uint64_t id, uint64_t bytes)
  {
    if (bytes == 0) return;

    double now = Simulator::Now().GetSeconds();

    OutFlow &f = m_outFlows[id];
    f.id = id;
    f.remaining = bytes;
    f.started = true;

    NS_LOG_UNCOND("["<<m_name<<"] DATA_OUT_START id="<<id<<" bytes="<<bytes<<" rate="<<g_appRateStr);
    CsvEvent(now, "DATA_OUT_START", m_name, id, bytes, 0, m_name, 0,0,0, "rate="+g_appRateStr);

    f.ev = Simulator::Schedule(Seconds(g_flowTick), &WorkerApp::OutFlowTick, this, id);
  }

  void OutFlowTick(uint64_t id)
  {
    auto it = m_outFlows.find(id);
    if (it == m_outFlows.end()) return;
    OutFlow &f = it->second;

    if (f.remaining == 0) {
      m_outFlows.erase(it);
      return;
    }

    uint64_t bytesPerTick = (uint64_t)((double)g_appRate.GetBitRate()/8.0 * g_flowTick);

    uint64_t sendNow = std::min<uint64_t>(f.remaining, bytesPerTick);

    while (sendNow > 0) {
      uint32_t payload = (uint32_t)std::min<uint64_t>(sendNow, g_maxUdpPayload);
      Ptr<Packet> p = MakeDataPacket(id, 2, payload);
      m_txDataOut->SendTo(p, 0, InetSocketAddress(m_outOnSbus, PORT_DATA_OUT));
      f.remaining -= payload;
      sendNow -= payload;
    }

    f.ev = Simulator::Schedule(Seconds(g_flowTick), &WorkerApp::OutFlowTick, this, id);
  }

  void OnComputeDone(uint64_t id, double execSec, double wallSec, double tStart)
  {
    Job job;
    bool ok = false;
    {
      std::lock_guard<std::mutex> lk(m_runMutex);
      auto it = m_runningJobs.find(id);
      if (it != m_runningJobs.end()) {
        job = it->second;
        m_runningJobs.erase(it);
        m_runningStart.erase(id);
        ok = true;
      }
    }
    if (!ok) return;

    double tDone = Simulator::Now().GetSeconds();

    // ✅ compute done：立刻释放 GPU
    m_runningCtas.fetch_sub(job.ctas, std::memory_order_relaxed);

    NS_LOG_UNCOND("["<<m_name<<"] WORKER_DONE id="<<job.id<<" t="<<tDone
                 <<" exec="<<execSec<<" wall="<<wallSec
                 <<" running="<<m_runningCtas.load()
                 <<" queued="<<m_queuedCtas.load());

    CsvEvent(tDone, "WORKER_DONE", m_name, job.id, job.N, job.ctas, job.server,
             m_baseline, GetUsedAllocated(), GetFreeAllocated(),
             "exec="+std::to_string(execSec));

    // 资源变化：被动 STATUS 更新（DONE）
    SendStatus("DONE", job.id, job.ctas);

    // ✅ compute done 后才开始输出（输出不占 GPU）
    StartDataOut(job.id, job.outBytes);


    // 继续尝试启动后续任务（硬排队）
    TryStart();
  }

  void LogUtil()
  {
    double now = Simulator::Now().GetSeconds();
    uint32_t running = m_runningCtas.load(std::memory_order_relaxed);
    double util = (m_baseline==0)?0.0: (double)running/(double)m_baseline;

    NS_LOG_UNCOND("["<<m_name<<"] UTIL t="<<now<<" running="<<running<<" base="<<m_baseline<<" util="<<util);
    CsvEvent(now, "UTIL", m_name, 0, 0, 0, "", m_baseline, running, (m_baseline-running),
             "util="+std::to_string(util));

    Simulator::Schedule(Seconds(m_utilPeriod), &WorkerApp::LogUtil, this);
  }

private:
  std::string m_name;
  GpuSpec m_spec;
  KernelShape m_ks;
  uint32_t m_baseline{0};
  Ipv4Address m_outOnSbus;
  double m_utilPeriod{1.0};

  Ptr<Socket> m_rxTask;
  Ptr<Socket> m_rxProbe;
  Ptr<Socket> m_rxDataIn;
  Ptr<Socket> m_txStatus;
  Ptr<Socket> m_txDataOut;

  std::mutex m_qMutex;
  std::deque<Job> m_queue;
  std::unordered_map<uint64_t, Job> m_jobById;

  std::atomic<uint32_t> m_runningCtas{0};
  std::atomic<uint32_t> m_queuedCtas{0};

  // 运行中任务
  std::mutex m_runMutex;
  std::unordered_map<uint64_t, Job> m_runningJobs;
  std::unordered_map<uint64_t, double> m_runningStart;

  // 线程 -> ns3 主线程：done 队列
  std::mutex m_doneMutex;
  std::deque<DoneInfo> m_doneQ;

  // 输出流
  std::unordered_map<uint64_t, OutFlow> m_outFlows;
};

// ============================================================================
// OutApp：
// - TASK：按 server=S1/S2 转发到 Worker；并发 ASSIGN 给 Host（触发 Host 发 DATA_IN）
// - PROBE：周期向 S1/S2 发送探测
// - STATUS：转发到 in1（PROBE/START/DONE 都会更新 in 表）
// - DATA_IN：Host -> out -> Worker（基于 taskId 查映射）
// - DATA_OUT：Worker -> out -> Host
// ============================================================================
class OutApp : public Application
{
public:
  void Setup(Ipv4Address s1Addr, Ipv4Address s2Addr, Ipv4Address in1Addr, Ipv4Address hostAddr, double probePeriod)
  {
    m_s1Addr = s1Addr;
    m_s2Addr = s2Addr;
    m_in1Addr = in1Addr;
    m_hostAddr = hostAddr;
    m_probePeriod = probePeriod;
  }
  Ptr<Socket> m_txAssign;

private:
  virtual void StartApplication() override
  {

    m_txAssign = Socket::CreateSocket(GetNode(), UdpSocketFactory::GetTypeId());

    // TASK 控制面
    m_rxTaskFromIn = Socket::CreateSocket(GetNode(), UdpSocketFactory::GetTypeId());
    m_rxTaskFromIn->Bind(InetSocketAddress(Ipv4Address::GetAny(), PORT_TO_OUT));
    m_rxTaskFromIn->SetRecvCallback(MakeCallback(&OutApp::OnTaskFromIn, this));

    // STATUS 控制面
    m_rxStatusFromW = Socket::CreateSocket(GetNode(), UdpSocketFactory::GetTypeId());
    m_rxStatusFromW->Bind(InetSocketAddress(Ipv4Address::GetAny(), PORT_STATUS_WORKER_TO_OUT));
    m_rxStatusFromW->SetRecvCallback(MakeCallback(&OutApp::OnStatusFromWorker, this));

    // DATA_IN 数据面
    m_rxDataIn = Socket::CreateSocket(GetNode(), UdpSocketFactory::GetTypeId());
    m_rxDataIn->Bind(InetSocketAddress(Ipv4Address::GetAny(), PORT_DATA_IN));
    m_rxDataIn->SetRecvCallback(MakeCallback(&OutApp::OnDataInFromHost, this));

    // DATA_OUT 数据面
    m_rxDataOut = Socket::CreateSocket(GetNode(), UdpSocketFactory::GetTypeId());
    m_rxDataOut->Bind(InetSocketAddress(Ipv4Address::GetAny(), PORT_DATA_OUT));
    m_rxDataOut->SetRecvCallback(MakeCallback(&OutApp::OnDataOutFromWorker, this));

    // TX sockets
    m_txToWorker = Socket::CreateSocket(GetNode(), UdpSocketFactory::GetTypeId());
    m_txProbe    = Socket::CreateSocket(GetNode(), UdpSocketFactory::GetTypeId());
    m_txStatusToIn = Socket::CreateSocket(GetNode(), UdpSocketFactory::GetTypeId());
    m_txAssignToHost = Socket::CreateSocket(GetNode(), UdpSocketFactory::GetTypeId());
    m_txDataToWorker = Socket::CreateSocket(GetNode(), UdpSocketFactory::GetTypeId());
    m_txDataToHost   = Socket::CreateSocket(GetNode(), UdpSocketFactory::GetTypeId());

    NS_LOG_UNCOND("[out] OutApp start S1="<<m_s1Addr<<" S2="<<m_s2Addr<<" Host="<<m_hostAddr<<" in1="<<m_in1Addr);

    // 永远启用周期 PROBE（“主动模式”职责）
    Simulator::Schedule(Seconds(m_probePeriod), &OutApp::SendProbeAll, this);
  }

  virtual void StopApplication() override
  {
    if (m_rxTaskFromIn){ m_rxTaskFromIn->Close(); m_rxTaskFromIn=nullptr; }
    if (m_rxStatusFromW){ m_rxStatusFromW->Close(); m_rxStatusFromW=nullptr; }
    if (m_rxDataIn){ m_rxDataIn->Close(); m_rxDataIn=nullptr; }
    if (m_rxDataOut){ m_rxDataOut->Close(); m_rxDataOut=nullptr; }

    if (m_txToWorker){ m_txToWorker->Close(); m_txToWorker=nullptr; }
    if (m_txProbe){ m_txProbe->Close(); m_txProbe=nullptr; }
    if (m_txStatusToIn){ m_txStatusToIn->Close(); m_txStatusToIn=nullptr; }
    if (m_txAssignToHost){ m_txAssignToHost->Close(); m_txAssignToHost=nullptr; }
    if (m_txDataToWorker){ m_txDataToWorker->Close(); m_txDataToWorker=nullptr; }
    if (m_txDataToHost){ m_txDataToHost->Close(); m_txDataToHost=nullptr; }
  }

  void OnTaskFromIn(Ptr<Socket> sock)
  {
    Address from;
    std::string s = RecvStr(sock, from);
    if (s.empty()) return;

    uint64_t id = ParseU64(s, "id=");
    std::string server = ParseStr(s, "server=");
    if (server.empty()) server = "S1";
    Ipv4Address dst = (server=="S2") ? m_s2Addr : m_s1Addr;

    // 记录 taskId -> worker 映射（数据面转发用）
    m_taskToWorker[id] = dst;

    double now = Simulator::Now().GetSeconds();
    NS_LOG_UNCOND("[out] FWD_TASK -> "<<server<<" "<<s);
    CsvEvent(now, "OUT_FWD_TASK", "out", id, ParseU64(s,"N="), ParseU32(s,"CTAS="),
            server, 0,0,0, "dst="+IpToString(dst));

    // 控制面转发到 worker
    UdpSendStr(m_txToWorker, dst, PORT_TO_WORKER, s);

    // 立刻通知 Host 该任务分配到哪个 worker（让 Host 开始发 DATA_IN）
    std::ostringstream a;
    a << "ASSIGN id=" << id
      << " server=" << server
      << " dst=" << IpToString(dst)
      << " t=" << now;

    if (!g_quietProbe) {
      NS_LOG_UNCOND("[out] " << a.str());
    }
    CsvEvent(now, "ASSIGN_TX", "out", id, 0,0, server, 0,0,0, "");

    UdpSendStr(m_txAssign, m_hostAddr, PORT_ASSIGN_OUT_TO_HOST, a.str());
  }


  void OnStatusFromWorker(Ptr<Socket> sock)
  {
    Address from;
    std::string s = RecvStr(sock, from);
    if (s.empty()) return;

    double now = Simulator::Now().GetSeconds();
    NS_LOG_UNCOND("[out] STATUS_RX "<<s);

    std::string node = ParseStr(s, "node=");
    std::string reason = ParseStr(s, "reason=");
    uint32_t base = ParseU32(s, "base=");
    uint32_t used = ParseU32(s, "used=");
    uint32_t free = ParseU32(s, "free=");
    uint64_t id = ParseU64(s, "id=");
    uint32_t freed = ParseU32(s, "freed=");

    CsvEvent(now, "OUT_STATUS_RX", "out", id, 0, freed, node, base, used, free, "reason="+reason);

    // 转发给 in1（“被动更新表”职责）
    UdpSendStr(m_txStatusToIn, m_in1Addr, PORT_STATUS_OUT_TO_IN, s);
  }

  void OnDataInFromHost(Ptr<Socket> sock)
  {
    Address from;
    Ptr<Packet> p = sock->RecvFrom(from);
    if (!p) return;

    uint64_t id; uint8_t kind; uint32_t payload;
    if (!ParseDataPacket(p, id, kind, payload)) return;
    if (kind != 1) return;

    auto it = m_taskToWorker.find(id);
    if (it == m_taskToWorker.end()) {
      // 还没收到 TASK 映射时的兜底：直接丢（正常不会发生）
      return;
    }
    Ipv4Address dst = it->second;

    // 转发到 worker
    m_txDataToWorker->SendTo(p, 0, InetSocketAddress(dst, PORT_DATA_IN));
  }

  void OnDataOutFromWorker(Ptr<Socket> sock)
  {
    Address from;
    Ptr<Packet> p = sock->RecvFrom(from);
    if (!p) return;

    uint64_t id; uint8_t kind; uint32_t payload;
    if (!ParseDataPacket(p, id, kind, payload)) return;
    if (kind != 2) return;

    // 转发到 Host
    m_txDataToHost->SendTo(p, 0, InetSocketAddress(m_hostAddr, PORT_DATA_OUT));
  }

  void SendProbeAll()
  {
    double now = Simulator::Now().GetSeconds();
    std::ostringstream oss; oss << "PROBE t="<<now;

    NS_LOG_UNCOND("[out] PROBE_TX "<<oss.str()<<" dst=S1");
    CsvEvent(now, "PROBE_TX", "out", 0,0,0,"S1",0,0,0,"");
    UdpSendStr(m_txProbe, m_s1Addr, PORT_PROBE_OUT_TO_WORKER, oss.str());

    NS_LOG_UNCOND("[out] PROBE_TX "<<oss.str()<<" dst=S2");
    CsvEvent(now, "PROBE_TX", "out", 0,0,0,"S2",0,0,0,"");
    UdpSendStr(m_txProbe, m_s2Addr, PORT_PROBE_OUT_TO_WORKER, oss.str());

    Simulator::Schedule(Seconds(m_probePeriod), &OutApp::SendProbeAll, this);
  }

private:
  Ipv4Address m_s1Addr, m_s2Addr, m_in1Addr, m_hostAddr;
  double m_probePeriod{1.0};

  Ptr<Socket> m_rxTaskFromIn;
  Ptr<Socket> m_rxStatusFromW;
  Ptr<Socket> m_rxDataIn;
  Ptr<Socket> m_rxDataOut;

  Ptr<Socket> m_txToWorker;
  Ptr<Socket> m_txProbe;
  Ptr<Socket> m_txStatusToIn;
  Ptr<Socket> m_txAssignToHost;
  Ptr<Socket> m_txDataToWorker;
  Ptr<Socket> m_txDataToHost;

  std::unordered_map<uint64_t, Ipv4Address> m_taskToWorker;
};

// ============================================================================
// SchedulerApp（in1）：
// - 普通任务队列
// - 维护资源表（来自 STATUS：PROBE/START/DONE）
// - 轮询公平选路（S1/S2 同容量，多一个选择）
// - compute done 释放 GPU：由 STATUS(reason=DONE) 触发更新表（被动）
// - 任务完成：由 Host ACK 驱动（output 发完并收齐）
// ============================================================================
class SchedulerApp : public Application
{
public:
  void Setup(Ipv4Address outAddr,
             const GpuSpec &specS1,
             const GpuSpec &specS2,
             const KernelShape &ks,
             uint32_t plannedJobs)
  {
    m_outAddr = outAddr;
    m_ks = ks;
    m_plannedJobs = plannedJobs;

    uint32_t base1 = ComputeBaseline(specS1, ks);
    uint32_t base2 = ComputeBaseline(specS2, ks);
    ResInit("S1", base1);
    ResInit("S2", base2);
  }

private:
  struct NetJob {
    uint64_t id{0};
    uint64_t N{0};
    uint32_t ctas{0};

    uint64_t inBytes{0}, outBytes{0};
    uint64_t ctrlBytes{0};
    double durationS{0.0};
    double planGpu{0.0};

    double HS{0.0};
    double TIN_ENQ{0.0};
    double TIN_DISP{0.0};
  };

  virtual void StartApplication() override
  {
    // Host -> in1 TASKREQ
    m_rxTaskReq = Socket::CreateSocket(GetNode(), UdpSocketFactory::GetTypeId());
    m_rxTaskReq->Bind(InetSocketAddress(Ipv4Address::GetAny(), PORT_TASK_IN));
    m_rxTaskReq->SetRecvCallback(MakeCallback(&SchedulerApp::OnTaskReq, this));

    // out -> in1 STATUS
    m_rxStatus = Socket::CreateSocket(GetNode(), UdpSocketFactory::GetTypeId());
    m_rxStatus->Bind(InetSocketAddress(Ipv4Address::GetAny(), PORT_STATUS_OUT_TO_IN));
    m_rxStatus->SetRecvCallback(MakeCallback(&SchedulerApp::OnStatus, this));

    // Host -> in1 ACK
    m_rxAck = Socket::CreateSocket(GetNode(), UdpSocketFactory::GetTypeId());
    m_rxAck->Bind(InetSocketAddress(Ipv4Address::GetAny(), PORT_ACK_HOST_TO_IN));
    m_rxAck->SetRecvCallback(MakeCallback(&SchedulerApp::OnAck, this));

    // in1 -> out TASK
    m_txToOut = Socket::CreateSocket(GetNode(), UdpSocketFactory::GetTypeId());

    NS_LOG_UNCOND("[in1] Scheduler start (统一机制) table="<<ResSummary());
    CsvEvent(Simulator::Now().GetSeconds(), "TABLE_INIT", "in1", 0,0,0,"", 0,0,0, ResSummary());
  }

  virtual void StopApplication() override
  {
    if (m_rxTaskReq){ m_rxTaskReq->Close(); m_rxTaskReq=nullptr; }
    if (m_rxStatus){ m_rxStatus->Close(); m_rxStatus=nullptr; }
    if (m_rxAck){ m_rxAck->Close(); m_rxAck=nullptr; }
    if (m_txToOut){ m_txToOut->Close(); m_txToOut=nullptr; }
  }

  static double Clamp(double x, double lo, double hi)
  {
    if (x < lo) return lo;
    if (x > hi) return hi;
    return x;
  }

  static uint64_t Align256(uint64_t x)
  {
    return (x / 256) * 256;
  }

  // 把 (duration, plan_gpu) 映射成 N（用于 accel-sim）
  // 设计目标：plan_gpu 与 duration 都能影响 N；同时保证 N 在 [8000,90000] 且 256 对齐
  uint64_t MapToN(double durationS, double planGpu)
  {
    double fracGpu = Clamp(planGpu / 8.0, 0.0, 1.0);
    double normDur = Clamp(durationS / 200.0, 0.0, 1.0);
    double score = 0.5 * fracGpu + 0.5 * normDur;

    const uint64_t Nmin = 8000, Nmax = 90000;
    uint64_t N = (uint64_t)std::llround((double)Nmin + score * (double)(Nmax - Nmin));
    N = Align256(N);
    if (N < Nmin) N = Nmin;
    if (N > Nmax) N = Nmax;
    return N;
  }

  uint32_t MapToCtas(double planGpu, uint32_t baseCta)
  {
    double fracGpu = Clamp(planGpu / 8.0, 0.01, 1.0); // 至少给 1% 避免 0 CTA
    uint32_t ctas = (uint32_t)std::ceil(fracGpu * (double)baseCta);
    if (ctas < 1) ctas = 1;
    if (ctas > baseCta) ctas = baseCta;
    return ctas;
  }

  void OnTaskReq(Ptr<Socket> sock)
  {
    Address from;
    std::string s = RecvStr(sock, from);
    if (s.empty()) return;

    uint64_t id = ParseU64(s, "id=");
    uint64_t ctrlB = ParseU64(s, "CTRL=");
    uint64_t inB   = ParseU64(s, "IN=");
    uint64_t outB  = ParseU64(s, "OUT=");
    double durS    = ParseF64(s, "DUR=");
    double planGpu = ParseF64(s, "GPU=");
    double HS      = ParseF64(s, "HS=");

    if (id == 0) return;

    ResEntry r1 = ResGet("S1");
    uint64_t N = MapToN(durS, planGpu);
    uint32_t ctas = MapToCtas(planGpu, r1.base);

    NetJob j;
    j.id = id;
    j.N = N;
    j.ctas = ctas;
    j.ctrlBytes = ctrlB;
    j.inBytes = inB;
    j.outBytes = outB;
    j.durationS = durS;
    j.planGpu = planGpu;
    j.HS = HS;
    j.TIN_ENQ = Simulator::Now().GetSeconds();

    m_queue.push_back(j);
    m_jobInfo[id] = j;

    double now = Simulator::Now().GetSeconds();
    NS_LOG_UNCOND("[in1] IN_ENQ id="<<j.id<<" N="<<j.N<<" CTAS="<<j.ctas
                 <<" IN="<<j.inBytes<<" OUT="<<j.outBytes
                 <<" GPU="<<j.planGpu<<" DUR="<<j.durationS
                 <<" TIN_ENQ="<<j.TIN_ENQ
                 <<" qlen="<<m_queue.size()
                 <<" table="<<ResSummary());

    CsvEvent(now, "IN_ENQ", "in1", j.id, j.N, j.ctas, "",
             0,0,0, "IN="+std::to_string(j.inBytes)+" OUT="+std::to_string(j.outBytes)+" "+ResSummary());

    TryDispatch();
  }

  std::string SelectWorkerRR(uint32_t needCtas)
  {
    ResEntry r1 = ResGet("S1");
    ResEntry r2 = ResGet("S2");

    bool ok1 = (r1.free >= needCtas);
    bool ok2 = (r2.free >= needCtas);

    if (!ok1 && !ok2) return "";

    if (ok1 && ok2) {
      // 轮询公平
      std::string pick = (m_rrNext == 0) ? "S1" : "S2";
      m_rrNext = 1 - m_rrNext;
      return pick;
    }

    // 只有一个可用：选可用的（不强制翻转 rr）
    return ok1 ? "S1" : "S2";
  }

  void TryDispatch()
  {
    while (!m_queue.empty())
    {
      NetJob &j = m_queue.front();
      std::string server = SelectWorkerRR(j.ctas);
      if (server.empty()) break;

      // 乐观分配：in 表先扣资源；后续会被 STATUS(PROBE/START/DONE) 校准
      ResAlloc(server, j.ctas);
      ResEntry r = ResGet(server);

      double tDisp = Simulator::Now().GetSeconds();
      double waitIn = tDisp - j.TIN_ENQ;
      j.TIN_DISP = tDisp;

      std::ostringstream msg;
      msg << "TASK id="<<j.id
          << " N="<<j.N
          << " CTAS="<<j.ctas
          << " server="<<server
          << " IN="<<j.inBytes
          << " OUT="<<j.outBytes
          << " CTRL="<<j.ctrlBytes
          << " DUR="<<j.durationS
          << " GPU="<<j.planGpu
          << " HS="<<j.HS
          << " TIN_ENQ="<<j.TIN_ENQ
          << " TIN_DISP="<<j.TIN_DISP;

      UdpSendStr(m_txToOut, m_outAddr, PORT_TO_OUT, msg.str());

      NS_LOG_UNCOND("[in1] DISPATCH id="<<j.id<<" -> "<<server
                   <<" waitIn="<<waitIn
                   <<" "<<server<<"(b="<<r.base<<" u="<<r.used<<" f="<<r.free<<")"
                   <<" qRemain="<<(m_queue.size()-1));

      CsvEvent(tDisp, "DISPATCH", "in1", j.id, j.N, j.ctas, server,
               r.base, r.used, r.free,
               "waitIn="+std::to_string(waitIn)+" "+ResSummary());

      m_queue.pop_front();
    }
  }

  void OnStatus(Ptr<Socket> sock)
  {
    Address from;
    std::string s = RecvStr(sock, from);
    if (s.empty()) return;

    std::string node = ParseStr(s, "node=");
    std::string reason = ParseStr(s, "reason=");
    uint32_t base = ParseU32(s, "base=");
    uint32_t used = ParseU32(s, "used=");
    uint32_t free = ParseU32(s, "free=");
    double t = ParseF64(s, "t=");
    uint64_t doneId = ParseU64(s, "id=");
    uint32_t freed  = ParseU32(s, "freed=");

    double now = Simulator::Now().GetSeconds();
    NS_LOG_UNCOND("[in1] STATUS_RX "<<s);
    CsvEvent(now, "STATUS_RX", "in1", doneId, 0, freed, node, base, used, free, "reason="+reason);

    if (!node.empty()) {
      // ✅ 以 worker 报告为准校准表（被动更新核心）
      ResSet(node, base, used, free, t);
    }

    // 资源变化后尝试继续派发
    TryDispatch();
  }

  void OnAck(Ptr<Socket> sock)
  {
    Address from;
    std::string s = RecvStr(sock, from);
    if (s.empty()) return;

    uint64_t id = ParseU64(s, "id=");
    double tAck = ParseF64(s, "t=");

    double now = Simulator::Now().GetSeconds();

    if (m_ackSet.insert(id).second) {
      m_acked++;
      NS_LOG_UNCOND("[in1] ACK_RX id="<<id<<" acked="<<m_acked<<"/"<<m_plannedJobs<<" t="<<tAck);
      CsvEvent(now, "ACK_RX", "in1", id, 0,0,"Host", 0,0,0, "");

      // 任务级指标（以 ACK 定义“任务完成”）
      auto it = m_jobInfo.find(id);
      if (it != m_jobInfo.end()) {
        const NetJob &j = it->second;
        double resp = tAck - j.HS;
        CsvEvent(now, "TASK_METRICS", "in1", id, j.N, j.ctas, "",
                 0,0,0, "resp="+std::to_string(resp));
      }
    }

    if (m_acked >= m_plannedJobs) {
      double grace = 0.5;
      NS_LOG_UNCOND("[SYS] STOP_CALL now="<<now<<" grace="<<grace<<" (all ACK received)");
      CsvEvent(now, "ALL_DONE", "in1", 0,0,0,"", 0,0,0, "stopAfter="+std::to_string(grace));
      Simulator::Stop(Seconds(grace));
    }
  }

private:
  Ipv4Address m_outAddr;
  KernelShape m_ks;

  Ptr<Socket> m_rxTaskReq;
  Ptr<Socket> m_rxStatus;
  Ptr<Socket> m_rxAck;
  Ptr<Socket> m_txToOut;

  std::deque<NetJob> m_queue;
  std::unordered_map<uint64_t, NetJob> m_jobInfo;

  uint32_t m_plannedJobs{0};
  uint32_t m_acked{0};
  std::unordered_set<uint64_t> m_ackSet;

  int m_rrNext{0}; // 0->S1, 1->S2
};

// ============================================================================
// tasks.csv 读取（Host）
// 需要字段：task_id, submit_time_s, duration_s, plan_gpu, ctrl_bytes, input_bytes, output_bytes
// ============================================================================

static std::vector<std::string> SplitCsvLine(const std::string &line)
{
  std::vector<std::string> out;
  std::string cur;
  bool inQuote = false;
  for (char c : line) {
    if (c == '"') { inQuote = !inQuote; continue; }
    if (c == ',' && !inQuote) { out.push_back(cur); cur.clear(); }
    else cur.push_back(c);
  }
  out.push_back(cur);
  return out;
}

// static bool LoadTasksCsv(const std::string &path, uint32_t maxJobs, std::vector<TaskRow> &out)
// {
//   std::ifstream fin(path);
//   if (!fin.is_open()) return false;

//   std::string header;
//   if (!std::getline(fin, header)) return false;
//   auto cols = SplitCsvLine(header);

//   std::unordered_map<std::string, int> idx;
//   for (int i=0;i<(int)cols.size();++i) {
//     idx[cols[i]] = i;
//   }

//   auto need = [&](const std::string &k)->bool{ return idx.find(k)!=idx.end(); };
//   if (!need("task_id") || !need("submit_time_s") || !need("duration_s") || !need("plan_gpu")
//       || !need("ctrl_bytes") || !need("input_bytes") || !need("output_bytes")) {
//     return false;
//   }

//   std::string line;
//   while (std::getline(fin, line)) {
//     if (line.empty()) continue;
//     auto v = SplitCsvLine(line);
//     auto get = [&](const std::string &k)->std::string{
//       int i = idx[k];
//       if (i < 0 || i >= (int)v.size()) return "";
//       return v[i];
//     };

//     TaskRow r;
//     r.id = std::stoull(get("task_id"));
//     r.submit = std::stod(get("submit_time_s"));
//     r.duration = std::stod(get("duration_s"));
//     r.planGpu  = std::stod(get("plan_gpu"));
//     r.ctrlB = std::stoull(get("ctrl_bytes"));
//     r.inB   = std::stoull(get("input_bytes"));
//     r.outB  = std::stoull(get("output_bytes"));

//     out.push_back(r);
//     if (maxJobs > 0 && out.size() >= maxJobs) break;
//   }

//   return !out.empty();
// }

// ============================================================================
// Host TaskGen：
// - 读取 tasks.csv
// - submit_time 到时发送 TASKREQ 到 in1（控制面）
// - 收到 out 的 ASSIGN 后，开始向 out 发 DATA_IN（大流量，tick 发送）
// - 接收 out 转发回来的 DATA_OUT，收齐后发 ACK 给 in1（任务完成定义）
// ============================================================================
class TaskGenApp : public Application
{
public:
  void Setup(Ipv4Address in1Addr,
             uint32_t plannedJobs,
             const std::string &tasksFile)
  {
    m_in1Addr = in1Addr;
    m_plannedJobs = plannedJobs;
    m_tasksFile = tasksFile;
  }
  Ptr<Socket> m_rxAssign;


private:
  struct TaskItem {
    uint64_t taskId{0};
    double   submitS{0.0};
    double   durS{0.0};
    double   planGpu{0.0};
    uint64_t ctrlB{0};
    uint64_t inB{0};
    uint64_t outB{0};
  };
  std::mt19937_64 m_rng;

  // 截断正态：拒绝采样，保证一定落在 [lo, hi]
  double SampleSubmitTruncNormal(double mu, double sigma, double lo, double hi)
  {
    if (sigma <= 0) return std::min(hi, std::max(lo, mu));
    std::normal_distribution<double> nd(mu, sigma);

    for (int k = 0; k < 10000; ++k) {
      double x = nd(m_rng);
      if (x >= lo && x <= hi) return x;
    }
    // 极端情况下兜底：截断到区间（避免死循环）
    double x = mu;
    if (x < lo) x = lo;
    if (x > hi) x = hi;
    return x;
  }


  // --- data plane + ack sockets ---
  Ptr<Socket> m_txDataIn;   // Host -> out  (PORT_DATA_IN)
  Ptr<Socket> m_rxDataOut;  // out  -> Host (PORT_DATA_OUT)
  Ptr<Socket> m_txAck;      // Host -> in1  (PORT_ACK_HOST_TO_IN)

  // task lookup
  std::unordered_map<uint64_t, TaskItem> m_taskById;

  // input flow state
  struct InFlow {
    uint64_t id{0};
    Ipv4Address dst;
    uint64_t remaining{0};
    EventId ev;
    bool active{false};
  };
  std::unordered_map<uint64_t, InFlow> m_inFlows;

  // output recv state
  std::unordered_map<uint64_t, uint64_t> m_outRecv; // taskId -> bytes received



  // ------- helpers -------
  static std::vector<std::string> SplitCsvSimple(const std::string &line)
  {
    // 简单 split（你的 csv 没有带引号逗号嵌套的话足够）
    std::vector<std::string> out;
    std::string cur;
    std::stringstream ss(line);
    while (std::getline(ss, cur, ',')) out.push_back(cur);
    return out;
  }

  static uint64_t ToU64Safe(const std::string &s, uint64_t def=0)
  {
    if (s.empty()) return def;
    try { return (uint64_t) std::stoull(s); }
    catch (...) { return def; }
  }

  static double ToF64Safe(const std::string &s, double def=0.0)
  {
    if (s.empty()) return def;
    try { return std::stod(s); }
    catch (...) { return def; }
  }

  static uint64_t ScaleCapBytes(uint64_t b, double scale, uint64_t cap)
  {
    long double v = (long double)b * (long double)scale;
    uint64_t x = (v < 1.0L) ? 1ULL : (uint64_t) llround((double)v);
    if (cap > 0) x = std::min<uint64_t>(x, cap);
    return x;
  }

  bool LoadTasksFromCsv(const std::string &path, uint32_t maxJobs)
  {
    std::ifstream fin(path);
    if (!fin.is_open()) {
      NS_LOG_UNCOND("[Host] tasksFile provided but empty/unreadable: " << path);
      return false;
    }

    std::string header;
    if (!std::getline(fin, header)) {
      NS_LOG_UNCOND("[Host] tasksFile header missing: " << path);
      return false;
    }

    auto cols = SplitCsvSimple(header);
    std::map<std::string, int> idx;
    for (int i=0;i<(int)cols.size();++i) {
      idx[cols[i]] = i;
    }

    auto Need = [&](const std::string &c)->bool { return idx.find(c) != idx.end(); };

    // 你现在真实用到的最小列集合（按你给的表头）
    std::vector<std::string> required = {
    "task_id","duration_s","plan_gpu","ctrl_bytes","input_bytes","output_bytes"
    };

    for (auto &c : required) {
      if (!Need(c)) {
        NS_LOG_UNCOND("[Host] tasks.csv missing required column: " << c);
        NS_LOG_UNCOND("[Host] header=" << header);
        return false;
      }
    }

    m_tasks.clear();
    m_tasks.reserve(maxJobs);

    std::string line;
    uint32_t rowCount = 0;
    while (std::getline(fin, line)) {
      if (line.empty()) continue;
      auto f = SplitCsvSimple(line);
      // 行字段数不足则跳过
      if ((int)f.size() < (int)cols.size()) continue;

      TaskItem t;
      t.taskId  = ToU64Safe(f[idx["task_id"]], 0);
      t.submitS = SampleSubmitTruncNormal(g_submitMu, g_submitSigma, g_submitLo, g_submitHi);
      t.durS    = ToF64Safe(f[idx["duration_s"]], 0.0);
      t.planGpu = ToF64Safe(f[idx["plan_gpu"]], 0.0);

      uint64_t ctrlB = ToU64Safe(f[idx["ctrl_bytes"]], 1);
      uint64_t inB   = ToU64Safe(f[idx["input_bytes"]], 1);
      uint64_t outB  = ToU64Safe(f[idx["output_bytes"]], 1);

      // 缩放 + 裁剪（解决“任务太大跑几十分钟/几小时”）
      t.ctrlB = ScaleCapBytes(ctrlB, g_bytesScale, g_capCtrlBytes);
      t.inB   = ScaleCapBytes(inB,   g_bytesScale, g_capInBytes);
      t.outB  = ScaleCapBytes(outB,  g_bytesScale, g_capOutBytes);

      // 合理范围（避免奇怪值）
      if (t.taskId == 0) continue;
      if (t.submitS < 0) t.submitS = 0;
      if (t.durS <= 0) t.durS = 1.0;
      t.planGpu = std::max(0.0, std::min(8.0, t.planGpu)); // 0~8

      m_tasks.push_back(t);
      rowCount++;
      if (m_tasks.size() >= maxJobs) break;
    }

    fin.close();

    // 按提交时间排序（防止 tasks.csv 乱序导致奇怪调度）
    std::sort(m_tasks.begin(), m_tasks.end(),
              [](const TaskItem &a, const TaskItem &b){ return a.submitS < b.submitS; });

    NS_LOG_UNCOND("[Host] loaded tasks=" << m_tasks.size()
                 << " from " << path
                 << " bytesScale=" << g_bytesScale
                 << " capIn=" << g_capInBytes
                 << " capOut=" << g_capOutBytes
                 << " capCtrl=" << g_capCtrlBytes);

    return !m_tasks.empty();
  }

  // 发送 TASKREQ 给 in1（你原来怎么编码消息就怎么保持一致）
  void SendTaskReq(const TaskItem &t)
  {
    double now = Simulator::Now().GetSeconds();

    std::ostringstream oss;
    oss << "TASKREQ id=" << t.taskId
        << " CTRL=" << t.ctrlB
        << " IN="   << t.inB
        << " OUT="  << t.outB
        << " DUR="  << t.durS
        << " GPU="  << t.planGpu
        << " HS="   << now;

    UdpSendStr(m_tx, m_in1Addr, PORT_TASK_IN, oss.str());

    NS_LOG_UNCOND("[Host] SEND " << oss.str());
    CsvEvent(now, "HOST_SEND", "Host", t.taskId, t.inB, 0, "", 0,0,0,
             "outB="+std::to_string(t.outB)+" ctrlB="+std::to_string(t.ctrlB));
  }

  virtual void StartApplication() override
  {
    m_tx = Socket::CreateSocket(GetNode(), UdpSocketFactory::GetTypeId());

    // ASSIGN RX
    m_rxAssign = Socket::CreateSocket(GetNode(), UdpSocketFactory::GetTypeId());
    m_rxAssign->Bind(InetSocketAddress(Ipv4Address::GetAny(), PORT_ASSIGN_OUT_TO_HOST));
    m_rxAssign->SetRecvCallback(MakeCallback(&TaskGenApp::OnAssign, this));

    // ✅ DATA plane sockets
    m_txDataIn  = Socket::CreateSocket(GetNode(), UdpSocketFactory::GetTypeId());
    m_rxDataOut = Socket::CreateSocket(GetNode(), UdpSocketFactory::GetTypeId());
    m_rxDataOut->Bind(InetSocketAddress(Ipv4Address::GetAny(), PORT_DATA_OUT));
    m_rxDataOut->SetRecvCallback(MakeCallback(&TaskGenApp::OnDataOut, this));

    // ✅ ACK socket
    m_txAck = Socket::CreateSocket(GetNode(), UdpSocketFactory::GetTypeId());

    if (m_tasksFile.empty()) {
      NS_LOG_UNCOND("[Host] tasksFile is empty, stop.");
      Simulator::Stop();
      return;
    }

    m_rng.seed(g_submitSeed + (uint64_t)GetNode()->GetId());

    if (!LoadTasksFromCsv(m_tasksFile, m_plannedJobs)) {
      NS_LOG_UNCOND("[Host] failed to load tasksFile=" << m_tasksFile);
      Simulator::Stop();
      return;
    }

    // ✅ 建立 taskId -> TaskRow 映射（OnAssign 要用）
    m_taskById.clear();
    for (auto &t : m_tasks) {
      m_taskById[t.taskId] = t;
    }

    for (auto &t : m_tasks) {
      Simulator::Schedule(Seconds(t.submitS), &TaskGenApp::SendTaskReq, this, t);
    }

    NS_LOG_UNCOND("[Host] planned csv jobs=" << m_tasks.size());
  }


  virtual void StopApplication() override
  {
    if (m_tx) { m_tx->Close(); m_tx = nullptr; }
    if (m_txDataIn) { m_txDataIn->Close(); m_txDataIn = nullptr; }
    if (m_rxDataOut) { m_rxDataOut->Close(); m_rxDataOut = nullptr; }
    if (m_txAck) { m_txAck->Close(); m_txAck = nullptr; }
  }

  
  void OnAssign(Ptr<Socket> sock)
  {
    Address from;
    std::string s = RecvStr(sock, from);
    if (s.empty()) return;

    uint64_t id = ParseU64(s, "id=");
    std::string dstStr = ParseStr(s, "dst=");
    Ipv4Address dst(dstStr.c_str());

    double now = Simulator::Now().GetSeconds();
    if (!g_quietProbe) {
      NS_LOG_UNCOND("[Host] ASSIGN_RX " << s);
    }
    CsvEvent(now, "ASSIGN_RX", "Host", id, 0,0, "", 0,0,0, "");

    auto it = m_taskById.find(id);
    if (it == m_taskById.end()) {
      NS_LOG_UNCOND("[Host] ASSIGN_RX but taskId not found id="<<id);
      return;
    }

    StartInputFlow(id, dst, it->second.inB);
  }


  void StartInputFlow(uint64_t id, Ipv4Address dst, uint64_t inBytes)
  {
    if (inBytes == 0) return;

    InFlow &f = m_inFlows[id];
    f.id = id;
    f.dst = dst;
    f.remaining = inBytes;
    f.active = true;

    double now = Simulator::Now().GetSeconds();
    NS_LOG_UNCOND("[Host] DATA_IN_START id="<<id<<" bytes="<<inBytes<<" dst="<<dst);
    CsvEvent(now, "HOST_DATA_IN_START", "Host", id, inBytes, 0, "", 0,0,0, "dst="+IpToString(dst));

    f.ev = Simulator::Schedule(Seconds(g_flowTick), &TaskGenApp::InputFlowTick, this, id);
  }

  void InputFlowTick(uint64_t id)
  {
    auto it = m_inFlows.find(id);
    if (it == m_inFlows.end()) return;
    InFlow &f = it->second;
    if (!f.active) return;

    if (f.remaining == 0) {
      f.active = false;
      m_inFlows.erase(it);
      return;
    }

    uint64_t bytesPerTick = (uint64_t)((double)g_appRate.GetBitRate()/8.0 * g_flowTick);

    uint64_t sendNow = std::min<uint64_t>(f.remaining, bytesPerTick);

    while (sendNow > 0) {
      uint32_t payload = (uint32_t)std::min<uint64_t>(sendNow, g_maxUdpPayload);
      Ptr<Packet> p = MakeDataPacket(id, 1, payload); // kind=1 input
      m_txDataIn->SendTo(p, 0, InetSocketAddress(f.dst, PORT_DATA_IN));
      f.remaining -= payload;
      sendNow -= payload;
    }

    f.ev = Simulator::Schedule(Seconds(g_flowTick), &TaskGenApp::InputFlowTick, this, id);
  }

  void OnDataOut(Ptr<Socket> sock)
  {
    Address from;
    Ptr<Packet> p = sock->RecvFrom(from);
    if (!p) return;

    uint64_t id; uint8_t kind; uint32_t payload;
    if (!ParseDataPacket(p, id, kind, payload)) return;
    if (kind != 2) return; // output

    auto it = m_taskById.find(id);
    if (it == m_taskById.end()) return;

    m_outRecv[id] += payload;

    uint64_t need = it->second.outB;
    if (m_outRecv[id] >= need) {
      double now = Simulator::Now().GetSeconds();
      std::ostringstream ack;
      ack << "ACK id=" << id << " t=" << now;

      UdpSendStr(m_txAck, m_in1Addr, PORT_ACK_HOST_TO_IN, ack.str());

      NS_LOG_UNCOND("[Host] ACK_TX id="<<id<<" outRecv="<<m_outRecv[id]<<"/"<<need);
      CsvEvent(now, "HOST_ACK_TX", "Host", id, m_outRecv[id], 0, "", 0,0,0, "");
    }
  }


private:
  Ipv4Address m_in1Addr;
  Ptr<Socket> m_tx;

  uint32_t m_plannedJobs{0};
  std::string m_tasksFile;

  std::vector<TaskItem> m_tasks;
};


static inline void MarkProgress()
{
  g_lastProgressTime = Simulator::Now().GetSeconds();
}


// ============================================================================
// main
// ============================================================================
int main(int argc, char *argv[])
{

  GlobalValue::Bind("SimulatorImplementationType", StringValue("ns3::RealtimeSimulatorImpl"));

  CommandLine cmd;

  // 兼容旧参数：mode 不再影响机制
  cmd.AddValue("mode", "kept for compatibility (ignored)", g_modeStr);

  cmd.AddValue("probePeriod", "Probe period (sec)", g_probePeriod);
  cmd.AddValue("utilPeriod", "Worker UTIL sampling period (sec)", g_utilPeriod);
  cmd.AddValue("plannedJobs", "Total jobs to run from tasksFile", g_plannedJobs);
  cmd.AddValue("tasksFile", "Path to tasks.csv", g_tasksFile);
  cmd.AddValue("csv", "CSV output path (events)", g_csvPath);

  cmd.AddValue("linkRate", "CSMA link bandwidth (DataRate)", g_linkRateStr);
  cmd.AddValue("appRate",  "App DATA_IN/DATA_OUT sending rate", g_appRateStr);

  cmd.AddValue("flowmon", "Enable FlowMonitor", g_enableFlowMon);
  cmd.AddValue("flowPrefix", "FlowMonitor output prefix", g_flowPrefix);

  //控制输出
  cmd.AddValue("quietProbe", "Silence PROBE/PROBE-STATUS logs unless anomaly", g_quietProbe);
  cmd.AddValue("anomPeriod", "Min seconds between anomaly prints", g_anomPrintPeriod);
  cmd.AddValue("stuckTimeout", "No progress timeout (sec) -> print anomaly", g_stuckTimeout);

  //控制任务
  cmd.AddValue("tasksFile", "CSV tasks file path", g_tasksFile);
  cmd.AddValue("bytesScale", "Scale ctrl/input/output bytes from tasks.csv", g_bytesScale);
  cmd.AddValue("capInBytes", "Cap per-task input bytes (0=off)", g_capInBytes);
  cmd.AddValue("capOutBytes", "Cap per-task output bytes (0=off)", g_capOutBytes);
  cmd.AddValue("capCtrlBytes", "Cap per-task ctrl bytes (0=off)", g_capCtrlBytes);

  cmd.AddValue("submitLo",    "Truncated normal submit time lower bound (s)", g_submitLo);
  cmd.AddValue("submitHi",    "Truncated normal submit time upper bound (s)", g_submitHi);
  cmd.AddValue("submitMu",    "Normal submit time mean (s)", g_submitMu);
  cmd.AddValue("submitSigma", "Normal submit time stddev (s)", g_submitSigma);
  cmd.AddValue("submitSeed",  "Random seed for submit time generator", g_submitSeed);

  cmd.Parse(argc, argv);

  g_appRate = DataRate(g_appRateStr);

  if (g_csvPath.empty()) g_csvPath = "events.csv";
  CsvOpen(g_csvPath);

  // 拓扑：h -- in1 -- r0 -- out -- (S1,S2)
  NodeContainer h, inNodes, rNodes, outNodes, sNodes;
  h.Create(1);
  inNodes.Create(1);
  rNodes.Create(1);
  outNodes.Create(1);
  sNodes.Create(2);

  CsmaHelper csma;
  csma.SetChannelAttribute("DataRate", DataRateValue(DataRate(g_linkRateStr)));

  csma.SetChannelAttribute("Delay", TimeValue(MilliSeconds(0.1)));
  // ✅ 调大 MTU，配合 60KB UDP payload，避免分片/卡死
  csma.SetDeviceAttribute("Mtu", UintegerValue(65535));

  NetDeviceContainer d_h_in  = csma.Install(NodeContainer(h.Get(0), inNodes.Get(0)));
  NetDeviceContainer d_in_r  = csma.Install(NodeContainer(inNodes.Get(0), rNodes.Get(0)));
  NetDeviceContainer d_out_r = csma.Install(NodeContainer(outNodes.Get(0), rNodes.Get(0)));
  NetDeviceContainer d_out_s = csma.Install(NodeContainer(outNodes.Get(0), sNodes.Get(0), sNodes.Get(1)));

  InternetStackHelper stack;
  stack.Install(h);
  stack.Install(inNodes);
  stack.Install(rNodes);
  stack.Install(outNodes);
  stack.Install(sNodes);

  Ipv4AddressHelper addr;
  Ipv4InterfaceContainer if_h_in, if_in_r, if_out_r, if_out_s;

  addr.SetBase("10.1.1.0","255.255.255.0");
  if_h_in = addr.Assign(d_h_in);

  addr.NewNetwork();
  if_in_r = addr.Assign(d_in_r);

  addr.NewNetwork();
  if_out_r = addr.Assign(d_out_r);

  addr.NewNetwork();
  if_out_s = addr.Assign(d_out_s);

  Ipv4GlobalRoutingHelper::PopulateRoutingTables();

  // 关键地址
  Ipv4Address hostAddr = if_h_in.GetAddress(0);
  Ipv4Address in1Addr  = if_h_in.GetAddress(1);
  Ipv4Address outAddr  = if_out_r.GetAddress(0);

  // out_s: 0=out, 1=S1, 2=S2
  Ipv4Address outOnSbus = if_out_s.GetAddress(0);
  Ipv4Address s1Addr    = if_out_s.GetAddress(1);
  Ipv4Address s2Addr    = if_out_s.GetAddress(2);

  KernelShape ks;
  // ✅ S1/S2 同容量
  GpuSpec specS1; specS1.sms = 64;
  GpuSpec specS2 = specS1;

  std::cout << "S1 baseline CTA="<<ComputeBaseline(specS1, ks)<<"\n";
  std::cout << "S2 baseline CTA="<<ComputeBaseline(specS2, ks)<<"\n";

  // FlowMonitor
  if (g_enableFlowMon) {
    g_flowMon = g_flowHelper.InstallAll();
    g_flowClassifier = DynamicCast<Ipv4FlowClassifier>(g_flowHelper.GetClassifier());
    NS_LOG_UNCOND("[FlowMon] Installed on all nodes");
  }

  // Host
  Ptr<TaskGenApp> hostApp = CreateObject<TaskGenApp>();
  hostApp->Setup(in1Addr, g_plannedJobs, g_tasksFile);
  h.Get(0)->AddApplication(hostApp);
  hostApp->SetStartTime(Seconds(0.5));

  // in1 Scheduler
  Ptr<SchedulerApp> sched = CreateObject<SchedulerApp>();
  sched->Setup(outAddr, specS1, specS2, ks, g_plannedJobs);
  inNodes.Get(0)->AddApplication(sched);
  sched->SetStartTime(Seconds(0.4));

  // out
  Ptr<OutApp> outApp = CreateObject<OutApp>();
  outApp->Setup(s1Addr, s2Addr, in1Addr, hostAddr, g_probePeriod);
  outNodes.Get(0)->AddApplication(outApp);
  outApp->SetStartTime(Seconds(0.4));

  // S1 Worker
  Ptr<WorkerApp> w1 = CreateObject<WorkerApp>();
  w1->Setup("S1", specS1, ks, outOnSbus, g_utilPeriod);
  sNodes.Get(0)->AddApplication(w1);
  w1->SetStartTime(Seconds(0.4));

  // S2 Worker
  Ptr<WorkerApp> w2 = CreateObject<WorkerApp>();
  w2->Setup("S2", specS2, ks, outOnSbus, g_utilPeriod);
  sNodes.Get(1)->AddApplication(w2);
  w2->SetStartTime(Seconds(0.4));

  // 最大兜底停止时间
  Simulator::Stop(Seconds(36000.0));
  Simulator::Run();

  // FlowMonitor dump
  DumpFlowMonitor(g_flowPrefix);

  // 记录结束
  CsvEvent(Simulator::Now().GetSeconds(), "SIM_END", "sys", 0,0,0,"",0,0,0,"");

  Simulator::Destroy();
  CsvClose();
  return 0;
}
