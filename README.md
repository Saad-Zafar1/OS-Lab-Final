# Mini Operating System Simulator

An academic C++ console project demonstrating process scheduling, producer-consumer coordination, and resource allocation. The source identifies the application as **Mini Operating System Simulator v2.0**.

## Implemented concepts

- Two producer threads generate simulated processes.
- A bounded buffer uses POSIX semaphores and a mutex.
- A CPU consumer processes the simulated workload.
- Round-robin scheduling uses a configurable time quantum.
- Resource management checks for safe allocation using a Banker's-algorithm-style safety sequence.
- Processes can be blocked when resources cannot be allocated safely.
- Text Gantt charts and per-process statistics show scheduling results.
- Statistics can be exported to a CSV file.

## Requirements

A POSIX/Linux environment with a C++17-capable compiler and pthread/semaphore support is expected. The source includes `semaphore.h` and uses `std::thread`, `std::mutex`, and POSIX `sem_t`.

On Windows, use a Linux environment such as WSL or a Linux virtual machine for the POSIX APIs.

## Build and run

From the repository root, the expected Linux build command is:

```sh
git clone https://github.com/Saad-Zafar1/OS-Lab-Final.git
cd OS-Lab-Final
g++ -std=c++17 -pthread Saad_OSLab_Final_Project/SourceCode/FinalCode.cpp -o mini-os-simulator
./mini-os-simulator
```

These commands are derived from the source requirements; a fresh compilation has not been performed as part of this documentation update.

## Console menu

1. Start simulation.
2. Pause simulation.
3. Resume simulation.
4. Add a process manually.
5. Display the current system state.
6. Print a Gantt chart and process statistics.
7. Export statistics to CSV.
8. Exit.

Manual process input includes CPU burst time, priority, and resource requirements. The statistics view reports completion, turnaround, and waiting times.

## Default configuration

The constants in `main()` configure a buffer capacity of **10**, a round-robin time quantum of **2**, and **3** resource types. Initial available resources are `[5, 3, 4]`.

## Repository structure

| Path | Purpose |
| --- | --- |
| `Saad_OSLab_Final_Project/SourceCode/FinalCode.cpp` | Complete simulator source |
| `Saad_OSLab_Final_Project/CodebeingruninVM/` | Nine screenshots of execution in a VM |

## Scope and limitations

This program simulates OS concepts in a user-space console application. It is an educational model rather than a bootable operating system or a replacement for the host scheduler. Workload generation is random, so observed output can vary between runs. There is no build-system configuration or automated test suite in the repository.
