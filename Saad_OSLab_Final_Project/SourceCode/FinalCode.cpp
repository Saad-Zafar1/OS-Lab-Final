#include <iostream>
#include <vector>
#include <deque>
#include <map>
#include <thread>
#include <atomic>
#include <random>
#include <chrono>
#include <mutex>
#include <condition_variable>
#include <semaphore.h>
#include <algorithm>
#include <iomanip>
#include <sstream>
#include <fstream>

struct Process {
    int pid;
    int arrivalTime;
    int burstTime;
    int remainingTime;
    int priority;
    std::vector<int> maxDemand;

    int startTime = -1;
    int completionTime = -1;

    Process(int pid_, int at, int bt, int pr, const std::vector<int>& req)
        : pid(pid_), arrivalTime(at), burstTime(bt), remainingTime(bt),
          priority(pr), maxDemand(req) {}

    std::string toString() const {
        std::ostringstream oss;
        oss << "PID=" << pid
            << " AT=" << arrivalTime
            << " BT=" << burstTime
            << " RT=" << remainingTime
            << " PR=" << priority
            << " MaxDemand=[";
        for (size_t i=0; i<maxDemand.size(); i++){
            oss << maxDemand[i] << (i+1<maxDemand.size()? ",":"");
        }
        oss << "]";
        return oss.str();
    }
};

class BoundedBuffer {
private:
    std::vector<Process*> buffer;
    int capacity;
    int head;
    int tail;

    sem_t semEmpty;
    sem_t semFull;
    std::mutex mtx;

public:
    explicit BoundedBuffer(int cap)
        : buffer(cap, nullptr), capacity(cap), head(0), tail(0)
    {
        sem_init(&semEmpty, 0, cap);
        sem_init(&semFull, 0, 0);
    }

    ~BoundedBuffer(){
        sem_destroy(&semEmpty);
        sem_destroy(&semFull);
    }

    void push(Process* p){
        sem_wait(&semEmpty);
        {
            std::lock_guard<std::mutex> lock(mtx);
            buffer[tail] = p;
            tail = (tail + 1) % capacity;
        }
        sem_post(&semFull);
    }

    Process* pop(){
        sem_wait(&semFull);
        Process* p;
        {
            std::lock_guard<std::mutex> lock(mtx);
            p = buffer[head];
            buffer[head] = nullptr;
            head = (head + 1) % capacity;
        }
        sem_post(&semEmpty);
        return p;
    }

    bool tryPop(Process*& out){
        if (sem_trywait(&semFull) != 0) return false;
        {
            std::lock_guard<std::mutex> lock(mtx);
            out = buffer[head];
            buffer[head] = nullptr;
            head = (head + 1) % capacity;
        }
        sem_post(&semEmpty);
        return true;
    }

    int sizeApprox(){
        std::lock_guard<std::mutex> lock(mtx);
        int count = 0;
        for (auto* x : buffer) if (x != nullptr) count++;
        return count;
    }
};

static bool vecLE(const std::vector<int>& need, const std::vector<int>& work){
    for (size_t i=0; i<need.size(); i++) 
        if (need[i] > work[i]) return false;
    return true;
}

class ResourceManager {
private:
    int m;
    std::vector<int> available;
    int maxPidTracked;
    std::vector<std::vector<int>> maxMat;
    std::vector<std::vector<int>> allocMat;
    std::mutex rmMtx;

public:
    ResourceManager(const std::vector<int>& initialAvailable, int maxPidHint = 5000)
        : m((int)initialAvailable.size()), available(initialAvailable),
          maxPidTracked(maxPidHint),
          maxMat(maxPidHint, std::vector<int>(m, 0)),
          allocMat(maxPidHint, std::vector<int>(m, 0)) {}

    int resourceTypes() const { return m; }

    void registerProcess(const Process& p){
        std::lock_guard<std::mutex> lock(rmMtx);
        if (p.pid >= maxPidTracked) return;
        maxMat[p.pid] = p.maxDemand;
    }

    bool isSafeState(std::vector<int>& safeSequenceOut){
        std::vector<int> work = available;
        std::vector<bool> finish(maxPidTracked, true);

        for (int pid=0; pid<maxPidTracked; pid++){
            bool active = false;
            for (int j=0; j<m; j++){
                if (maxMat[pid][j] != 0 || allocMat[pid][j] != 0) { 
                    active = true; 
                    break; 
                }
            }
            finish[pid] = !active;
        }

        safeSequenceOut.clear();
        bool progress = true;
        
        while (progress){
            progress = false;
            for (int pid=0; pid<maxPidTracked; pid++){
                if (finish[pid]) continue;

                std::vector<int> need(m);
                for (int j=0; j<m; j++) 
                    need[j] = maxMat[pid][j] - allocMat[pid][j];

                if (vecLE(need, work)){
                    for (int j=0; j<m; j++) 
                        work[j] += allocMat[pid][j];
                    finish[pid] = true;
                    safeSequenceOut.push_back(pid);
                    progress = true;
                }
            }
        }

        for (int pid=0; pid<maxPidTracked; pid++){
            if (!finish[pid]) return false;
        }
        return true;
    }

    bool requestResources(Process& p, std::vector<int>& safeSequenceOut){
        std::lock_guard<std::mutex> lock(rmMtx);
        if (p.pid >= maxPidTracked) return false;

        for (int j=0; j<m; j++){
            if (p.maxDemand[j] > available[j]) return false;
        }

        for (int j=0; j<m; j++){
            available[j] -= p.maxDemand[j];
            allocMat[p.pid][j] += p.maxDemand[j];
        }

        bool safe = isSafeState(safeSequenceOut);
        if (!safe){
            for (int j=0; j<m; j++){
                available[j] += p.maxDemand[j];
                allocMat[p.pid][j] -= p.maxDemand[j];
            }
            safeSequenceOut.clear();
            return false;
        }
        return true;
    }

    void releaseAll(Process& p){
        std::lock_guard<std::mutex> lock(rmMtx);
        if (p.pid >= maxPidTracked) return;

        for (int j=0; j<m; j++){
            available[j] += allocMat[p.pid][j];
            allocMat[p.pid][j] = 0;
            maxMat[p.pid][j] = 0;
        }
    }

    std::vector<int> getAvailable(){
        std::lock_guard<std::mutex> lock(rmMtx);
        return available;
    }

    std::map<int, std::vector<int>> getAllocations(){
        std::lock_guard<std::mutex> lock(rmMtx);
        std::map<int, std::vector<int>> result;
        for (int pid=0; pid<maxPidTracked; pid++){
            bool hasAlloc = false;
            for (int j=0; j<m; j++){
                if (allocMat[pid][j] > 0){
                    hasAlloc = true;
                    break;
                }
            }
            if (hasAlloc) result[pid] = allocMat[pid];
        }
        return result;
    }
};

struct GanttSlice{
    int pid;
    int start;
    int end;
};

class Scheduler {
private:
    int quantum;
    int currentTime = 0;
    std::map<int, Process*> allProcs;
    std::deque<Process*> ready;
    std::vector<GanttSlice> gantt;
    std::mutex schedMtx;

public:
    explicit Scheduler(int q=2): quantum(q) {}

    void registerProcess(Process* p){ 
        std::lock_guard<std::mutex> lock(schedMtx);
        allProcs[p->pid] = p; 
    }

    void addReady(Process* p){ 
        std::lock_guard<std::mutex> lock(schedMtx);
        ready.push_back(p); 
    }

    int readyCount() const { 
        return (int)ready.size(); 
    }

    int timeNow() const { 
        return currentTime; 
    }

    Process* dispatchOneCycle(){
        std::lock_guard<std::mutex> lock(schedMtx);
        if (ready.empty()) return nullptr;

        int n = (int)ready.size();

        if (n <= 5){
            auto it = std::min_element(ready.begin(), ready.end(),
                [](Process* a, Process* b){
                    if (a->priority != b->priority) 
                        return a->priority < b->priority;
                    return a->arrivalTime < b->arrivalTime;
                });

            Process* p = *it;
            ready.erase(it);

            if (p->startTime == -1) p->startTime = currentTime;

            int start = currentTime;
            currentTime += p->remainingTime;
            p->remainingTime = 0;
            p->completionTime = currentTime;

            gantt.push_back({p->pid, start, currentTime});
            return p;

        } else {
            Process* p = ready.front();
            ready.pop_front();

            if (p->startTime == -1) p->startTime = currentTime;

            int slice = std::min(quantum, p->remainingTime);
            int start = currentTime;
            currentTime += slice;
            p->remainingTime -= slice;

            gantt.push_back({p->pid, start, currentTime});

            if (p->remainingTime > 0){
                ready.push_back(p);
                return nullptr;
            } else {
                p->completionTime = currentTime;
                return p;
            }
        }
    }

    void printGantt() const {
        std::cout << "\n╔════════════════════════════════════════════════════════╗\n";
        std::cout << "║           GANTT CHART (Text-Based)                    ║\n";
        std::cout << "╚════════════════════════════════════════════════════════╝\n";
        
        if (gantt.empty()){
            std::cout << "(No processes executed yet)\n";
            return;
        }

        for (const auto& s : gantt) 
            std::cout << "| P" << std::setw(3) << s.pid << " ";
        std::cout << "|\n";

        std::cout << std::setw(4) << gantt.front().start;
        for (const auto& s : gantt) 
            std::cout << std::setw(8) << s.end;
        std::cout << "\n\n";
    }

    void printStats() const {
        std::cout << "\n╔════════════════════════════════════════════════════════╗\n";
        std::cout << "║           PROCESS STATISTICS                           ║\n";
        std::cout << "╚════════════════════════════════════════════════════════╝\n";
        std::cout << "┌─────┬─────┬─────┬─────┬─────┬─────┐\n";
        std::cout << "│ PID │  AT │  BT │  CT │ TAT │  WT │\n";
        std::cout << "├─────┼─────┼─────┼─────┼─────┼─────┤\n";

        double totalWT = 0, totalTAT = 0;
        int count = 0;

        for (auto &kv : allProcs){
            Process* p = kv.second;
            if (p->completionTime < 0) continue;

            int TAT = p->completionTime - p->arrivalTime;
            int WT  = TAT - p->burstTime;

            totalWT += WT;
            totalTAT += TAT;
            count++;

            std::cout << "│ P" << std::setw(2) << p->pid
                      << " │ " << std::setw(3) << p->arrivalTime
                      << " │ " << std::setw(3) << p->burstTime
                      << " │ " << std::setw(3) << p->completionTime
                      << " │ " << std::setw(3) << TAT
                      << " │ " << std::setw(3) << WT << " │\n";
        }

        std::cout << "└─────┴─────┴─────┴─────┴─────┴─────┘\n";

        if (count > 0){
            std::cout << "\n📊 Average Turnaround Time: " << std::fixed << std::setprecision(2) 
                      << (totalTAT / count) << " units\n";
            std::cout << "📊 Average Waiting Time:    " << std::fixed << std::setprecision(2) 
                      << (totalWT / count) << " units\n";
        } else {
            std::cout << "\n(No completed processes yet)\n";
        }
    }

    void exportStats(const std::string& filename) const {
        std::ofstream out(filename);
        if (!out) {
            std::cout << "Failed to open file for export.\n";
            return;
        }

        out << "PID,AT,BT,CT,TAT,WT\n";
        for (auto &kv : allProcs){
            Process* p = kv.second;
            if (p->completionTime < 0) continue;

            int TAT = p->completionTime - p->arrivalTime;
            int WT  = TAT - p->burstTime;

            out << p->pid << "," << p->arrivalTime << "," << p->burstTime << ","
                << p->completionTime << "," << TAT << "," << WT << "\n";
        }
        out.close();
        std::cout << "✓ Statistics exported to " << filename << "\n";
    }
};

static std::atomic<bool> gRunning{false};
static std::atomic<bool> gPaused{false};
static std::atomic<bool> gStopAll{false};
static std::atomic<int> gPidCounter{1};

static std::mutex blockedMtx;
static std::deque<Process*> blockedQ;

static std::mutex pauseMtx;
static std::condition_variable pauseCV;

static int rndInt(int lo, int hi){
    static thread_local std::mt19937 rng(
        (unsigned)std::chrono::high_resolution_clock::now().time_since_epoch().count()
    );
    std::uniform_int_distribution<int> dist(lo, hi);
    return dist(rng);
}

static std::vector<int> randomReq(int m){
    std::vector<int> r(m);
    for (int i=0; i<m; i++) r[i] = rndInt(0, 3);
    return r;
}

static void checkPause(){
    std::unique_lock<std::mutex> lock(pauseMtx);
    pauseCV.wait(lock, []{ return !gPaused.load(); });
}

void producerThreadFn(BoundedBuffer* buf, ResourceManager* rm, Scheduler* sch, int producerId){
    std::cout << "🏭 Producer " << producerId << " started.\n";
    
    while (!gStopAll.load()){
        if (!gRunning.load()){
            std::this_thread::sleep_for(std::chrono::milliseconds(200));
            continue;
        }

        checkPause();

        int pid = gPidCounter.fetch_add(1);
        int at  = sch->timeNow();
        int bt  = rndInt(1, 10);
        int pr  = rndInt(1, 10);
        auto req = randomReq(rm->resourceTypes());

        Process* p = new Process(pid, at, bt, pr, req);

        rm->registerProcess(*p);
        sch->registerProcess(p);
        buf->push(p);

        std::cout << "📦 Producer " << producerId << " created: " << p->toString() << "\n";

        std::this_thread::sleep_for(std::chrono::milliseconds(rndInt(300, 800)));
    }
    
    std::cout << "🛑 Producer " << producerId << " stopped.\n";
}

static void tryUnblock(ResourceManager* rm, Scheduler* sch){
    std::lock_guard<std::mutex> lock(blockedMtx);
    if (blockedQ.empty()) return;

    size_t n = blockedQ.size();
    for (size_t i=0; i<n; i++){
        Process* p = blockedQ.front();
        blockedQ.pop_front();

        std::vector<int> safeSeq;
        if (rm->requestResources(*p, safeSeq)){
            std::cout << "✓ Unblocked P" << p->pid << "\n";
            sch->addReady(p);
        } else {
            blockedQ.push_back(p);
        }
    }
}

void cpuConsumerFn(BoundedBuffer* buf, ResourceManager* rm, Scheduler* sch){
    std::cout << "💻 CPU Consumer started.\n";
    
    while (!gStopAll.load()){
        if (!gRunning.load()){
            std::this_thread::sleep_for(std::chrono::milliseconds(200));
            continue;
        }

        checkPause();

        Process* p = buf->pop();

        std::vector<int> safeSeq;
        if (rm->requestResources(*p, safeSeq)){
            std::cout << "\n✅ [BANKER SAFE] Granted resources to P" << p->pid << "\n";
            std::cout << "   Safe Sequence: ";
            for (size_t i=0; i<safeSeq.size(); i++){
                std::cout << "P" << safeSeq[i];
                if (i < safeSeq.size()-1) std::cout << " → ";
            }
            std::cout << "\n";

            sch->addReady(p);

            Process* extra = nullptr;
            while (buf->tryPop(extra)){
                std::vector<int> s2;
                if (rm->requestResources(*extra, s2)){
                    sch->addReady(extra);
                } else {
                    std::lock_guard<std::mutex> lock(blockedMtx);
                    blockedQ.push_back(extra);
                }
            }

            tryUnblock(rm, sch);

            Process* finished = sch->dispatchOneCycle();
            if (finished != nullptr){
                rm->releaseAll(*finished);
                std::cout << "🏁 P" << finished->pid << " completed → Resources released\n";
            }
        } else {
            std::lock_guard<std::mutex> lock(blockedMtx);
            blockedQ.push_back(p);
            std::cout << "\n⚠️  [BANKER UNSAFE] P" << p->pid << " blocked (deadlock prevention)\n";
        }

        std::this_thread::sleep_for(std::chrono::milliseconds(150));
    }
    
    std::cout << "🛑 CPU Consumer stopped.\n";
}

static void printSystemState(BoundedBuffer* buf, ResourceManager* rm, Scheduler* sch){
    std::cout << "\n╔════════════════════════════════════════════════════════╗\n";
    std::cout << "║              SYSTEM STATE SNAPSHOT                     ║\n";
    std::cout << "╚════════════════════════════════════════════════════════╝\n";
    
    std::cout << "📋 Buffer Size (approx):      " << buf->sizeApprox() << "\n";
    std::cout << "📋 Ready Queue Size:          " << sch->readyCount() << "\n";
    std::cout << "🕐 Current System Time:       " << sch->timeNow() << "\n";

    auto avail = rm->getAvailable();
    std::cout << "💾 Available Resources:       [";
    for (size_t i=0; i<avail.size(); i++){
        std::cout << avail[i];
        if (i+1 < avail.size()) std::cout << ", ";
    }
    std::cout << "]\n";

    auto allocs = rm->getAllocations();
    if (!allocs.empty()){
        std::cout << "\n📊 Current Allocations:\n";
        for (const auto& kv : allocs){
            std::cout << "   P" << kv.first << ": [";
            for (size_t i=0; i<kv.second.size(); i++){
                std::cout << kv.second[i];
                if (i+1 < kv.second.size()) std::cout << ", ";
            }
            std::cout << "]\n";
        }
    }

    std::lock_guard<std::mutex> lock(blockedMtx);
    std::cout << "\n🚫 Blocked Processes:         " << blockedQ.size() << "\n";
    if (!blockedQ.empty()){
        std::cout << "   PIDs: ";
        for (auto* p : blockedQ) std::cout << "P" << p->pid << " ";
        std::cout << "\n";
    }

    std::cout << "\n🎮 Simulation Status:         " 
              << (gRunning.load() ? (gPaused.load() ? "PAUSED" : "RUNNING") : "STOPPED") 
              << "\n";
}

static Process* manualProcessInput(ResourceManager* rm, Scheduler* sch){
    int bt, pr;
    std::cout << "\n┌─────────────────────────────────┐\n";
    std::cout << "│   MANUAL PROCESS CREATION       │\n";
    std::cout << "└─────────────────────────────────┘\n";
    
    std::cout << "Enter CPU Burst Time (1-20): ";
    std::cin >> bt;
    
    std::cout << "Enter Priority (1-10, lower=higher): ";
    std::cin >> pr;

    int m = rm->resourceTypes();
    std::vector<int> req(m);
    std::cout << "Enter " << m << " resource requirements (space-separated): ";
    for (int i=0; i<m; i++) std::cin >> req[i];

    int pid = gPidCounter.fetch_add(1);
    int at  = sch->timeNow();

    Process* p = new Process(pid, at, bt, pr, req);
    rm->registerProcess(*p);
    sch->registerProcess(p);
    
    return p;
}

void displayMenu(){
    std::cout << "\n";
    std::cout << "╔═════════════════════════════════════════════════════════╗\n";
    std::cout << "║         MINI OPERATING SYSTEM SIMULATOR v2.0            ║\n";
    std::cout << "╚═════════════════════════════════════════════════════════╝\n";
    std::cout << "  1️⃣  Start Simulation\n";
    std::cout << "  2️⃣  Pause Simulation\n";
    std::cout << "  3️⃣  Resume Simulation\n";
    std::cout << "  4️⃣  Add Process Manually\n";
    std::cout << "  5️⃣  Display System State\n";
    std::cout << "  6️⃣  Print Gantt Chart & Statistics\n";
    std::cout << "  7️⃣  Export Statistics to CSV\n";
    std::cout << "  8️⃣  Exit\n";
    std::cout << "╚═════════════════════════════════════════════════════════╝\n";
    std::cout << "Select option: ";
}

int main(){
    std::cout << "\n🚀 Initializing Mini OS Simulator...\n\n";
    
    const int BUFFER_CAP = 10;
    const int QUANTUM = 2;
    const int NUM_RESOURCE_TYPES = 3;
    std::vector<int> initialAvail = {5, 3, 4};

    std::cout << "⚙️  Configuration:\n";
    std::cout << "   - Buffer Capacity: " << BUFFER_CAP << "\n";
    std::cout << "   - Time Quantum (RR): " << QUANTUM << "\n";
    std::cout << "   - Resource Types: " << NUM_RESOURCE_TYPES << "\n";
    std::cout << "   - Initial Resources: [";
    for (int i=0; i<NUM_RESOURCE_TYPES; i++){
        std::cout << initialAvail[i];
        if (i < NUM_RESOURCE_TYPES-1) std::cout << ", ";
    }
    std::cout << "]\n\n";

    BoundedBuffer buffer(BUFFER_CAP);
    ResourceManager rm(initialAvail, 5000);
    Scheduler scheduler(QUANTUM);

    std::cout << "✓ Components initialized\n";
    std::cout << "✓ Starting threads...\n\n";

    std::thread prod1(producerThreadFn, &buffer, &rm, &scheduler, 1);
    std::thread prod2(producerThreadFn, &buffer, &rm, &scheduler, 2);
    std::thread cpu(cpuConsumerFn, &buffer, &rm, &scheduler);

    while (true){
        displayMenu();

        int choice;
        std::cin >> choice;

        if (choice == 1){
            if (gRunning.load()){
                std::cout << "⚠️  Simulation already running!\n";
            } else {
                gRunning.store(true);
                gPaused.store(false);
                std::cout << "✅ Simulation started.\n";
            }
            
        } else if (choice == 2){
            if (!gRunning.load()){
                std::cout << "⚠️  Simulation not running!\n";
            } else if (gPaused.load()){
                std::cout << "⚠️  Simulation already paused!\n";
            } else {
                gPaused.store(true);
                std::cout << "⏸️  Simulation PAUSED.\n";
            }
            
        } else if (choice == 3){
            if (!gPaused.load()){
                std::cout << "⚠️  Simulation not paused!\n";
            } else {
                gPaused.store(false);
                pauseCV.notify_all();
                std::cout << "▶️  Simulation RESUMED.\n";
            }
            
        } else if (choice == 4){
            Process* p = manualProcessInput(&rm, &scheduler);
            buffer.push(p);
            std::cout << "✅ Process added: " << p->toString() << "\n";
            
        } else if (choice == 5){
            printSystemState(&buffer, &rm, &scheduler);
            
        } else if (choice == 6){
            scheduler.printGantt();
            scheduler.printStats();
            
        } else if (choice == 7){
            std::string filename;
            std::cout << "Enter filename (e.g., stats.csv): ";
            std::cin >> filename;
            scheduler.exportStats(filename);
            
        } else if (choice == 8){
            std::cout << "\n🛑 Shutting down simulator...\n";
            gStopAll.store(true);
            gRunning.store(false);
            gPaused.store(false);
            pauseCV.notify_all();
            break;
            
        } else {
            std::cout << "❌ Invalid choice. Please try again.\n";
        }
    }

    std::cout << "⏳ Waiting for threads to terminate...\n";
    if (prod1.joinable()) prod1.join();
    if (prod2.joinable()) prod2.join();
    if (cpu.joinable()) cpu.join();

    std::cout << "\n✅ Mini OS Simulator terminated successfully.\n";
    std::cout << "Thank you for using the simulator!\n\n";
    
    return 0;
}
