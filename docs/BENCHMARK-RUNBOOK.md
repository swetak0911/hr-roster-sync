# Benchmark Runbook - HR Roster Sync

This runbook captures host configuration and repeated 200,000-row PostgreSQL benchmark measurements. CPU/RAM and p50/p99 values must come from the machine where the benchmark is executed.

## 1. Host information (Windows PowerShell)

Run from the repository root:

    "=== OS ==="
    Get-CimInstance Win32_OperatingSystem | Select-Object Caption, Version, OSArchitecture
    "=== CPU ==="
    Get-CimInstance Win32_Processor | Select-Object -First 1 Name, NumberOfCores, NumberOfLogicalProcessors
    "=== RAM (bytes) ==="
    (Get-CimInstance Win32_ComputerSystem).TotalPhysicalMemory
    "=== Java ==="
    java -version 2>&1
    "=== Maven ==="
    mvn -version
    "=== Docker ==="
    docker version --format '{{.Server.Version}}'

### Captured assessment-machine configuration (2026-10-30)

The following values were supplied from the assessment machine:

| Item | Captured value |
|---|---|
| Operating system | Windows 11 Home Single Language, build 10.0.26300, 64-bit |
| Installed RAM | 8,451,309,568 bytes (approximately 7.87 GiB) |
| Java | Eclipse Temurin OpenJDK 21.0.13+9 LTS, 64-bit Server VM |
| Maven | Apache Maven 3.10.0 |
| Docker Desktop | 4.97.0 |
| Docker Engine | 29.9.2, Linux/amd64 (desktop-linux context) |
| CPU model / core count | Not captured in the supplied output; still required |

The CPU query did not return a visible value in the supplied output. Run this separately and append the result:

    Get-CimInstance Win32_Processor | Format-List Name,NumberOfCores,NumberOfLogicalProcessors

Save the output with the assessment evidence. These machine details were collected after the previously recorded 200k benchmark, so they are not proof that the benchmark was run on this exact configuration unless confirmed by a fresh run.

## 2. Repeated 200,000-row database benchmark

Run at least five independent iterations on an otherwise idle machine. Each iteration starts Testcontainers PostgreSQL and measures the same benchmark. Use the test's printed preview and apply milliseconds as per-run measurements.

    1..5 | ForEach-Object {
        $log = "benchmark-run-$_.log"
        Write-Host "Starting benchmark run $_ ..."
        mvn "-Dtest=ReconciliationPostgresIntegrationBenchmarkTest" "-Dbenchmark.rows=200000" test 2>&1 |
            Tee-Object -FilePath $log
        if ($LASTEXITCODE -ne 0) {
            throw "Benchmark run $_ failed. Inspect $log"
        }
    }

Extract measurements:

    Select-String -Path "benchmark-run-*.log" -Pattern "Real PostgreSQL Benchmark"

Do not combine a failed run with successful measurements. Record Docker resource limits and whether other workloads were running.

## 3. Calculate p50 and p99

For five or more observations, calculate nearest-rank percentiles separately for preview and apply. Replace every placeholder with actual measured milliseconds:

    $previewMs = @(16455, 0, 0, 0, 0)
    $applyMs   = @(3286, 0, 0, 0, 0)

    function Get-NearestRankPercentile([double[]]$Values, [double]$Percentile) {
        $sorted = @($Values | Sort-Object)
        $rank = [Math]::Ceiling($Percentile * $sorted.Count)
        return $sorted[[Math]::Max(0, $rank - 1)]
    }

    "Preview p50: $(Get-NearestRankPercentile $previewMs 0.50) ms"
    "Preview p99: $(Get-NearestRankPercentile $previewMs 0.99) ms"
    "Apply p50:   $(Get-NearestRankPercentile $applyMs 0.50) ms"
    "Apply p99:   $(Get-NearestRankPercentile $applyMs 0.99) ms"

With only five samples, p99 is the maximum observation and is not a statistically strong tail-latency estimate. Prefer at least 100 measured operations for meaningful p99 if the harness is extended to measure per-operation latency.

## 4. Throughput

The integration benchmark is a batch reconciliation benchmark, not a concurrent request load test. For a transparent batch throughput figure:

**rows per second = input CSV rows / measured preview seconds**

For the previously recorded run: 200,000 / 16.455 = approximately **12,154 input rows/second** for preview. This is a derived single-run batch rate, not API request throughput or a load-test result.

## 5. Results table

| Run | Preview (ms) | Apply (ms) | Rows | Notes |
|---|---:|---:|---:|---|
| Existing recorded run | 16,455 | 3,286 | 200,000 | Single run; machine details not captured |
| Fresh run 1 | | | 200,000 | |
| Fresh run 2 | | | 200,000 | |
| Fresh run 3 | | | 200,000 | |
| Fresh run 4 | | | 200,000 | |
| Fresh run 5 | | | 200,000 | |

## 6. Interpretation

- The previous 16.455-second preview is below the 60-second target for that recorded run.
- Do not claim a universal 60-second guarantee based on one machine/run.
- Disk-bucket implementation lowers typical memory use but can have a large bucket under skew; current benchmark does not prove a strict worst-case memory bound.
- Do not label batch rows/second as HTTP throughput.
