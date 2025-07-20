// API client for connecting to the Python pipeline backend

export interface PipelineConfig {
  sources: string[];
  maxPages: number;
  targetCount: number;
  incremental: boolean;
  hoursBack: number;
}

export interface ContinuousConfig {
  sources: string[];
  max_per_batch: number;
}

export interface PipelineStatusResponse {
  pipeline_status: string;
  cache_stats: {
    total_entries: number;
    estimated_size_bytes: number;
  };
  recent_datasets: Array<{
    filename: string;
    path: string;
    size_bytes: number;
    modified: string;
    line_count: number;
    metadata: Record<string, unknown>;
  }>;
  execution_info: {
    current_stage: string;
    collected_count: number;
    processed_count: number;
    quality_filtered_count: number;
    final_count: number;
    elapsed_time: number;
    last_execution: string;
    next_scheduled: string | null;
    errors: string[];
  } | null;
  timestamp: string;
}

export interface LogsResponse {
  logs: string[];
  error?: string;
}

const API_BASE_URL = process.env.NEXT_PUBLIC_API_BASE_URL || 'http://localhost:8000';
const USE_MOCK_API = process.env.NEXT_PUBLIC_USE_MOCK_API === 'true' || false; // Use real API by default

class PipelineAPI {
  private baseURL: string;
  private useMockAPI: boolean;

  constructor(baseURL: string = API_BASE_URL, useMockAPI: boolean = USE_MOCK_API) {
    this.baseURL = baseURL;
    this.useMockAPI = useMockAPI;
  }

  // Mock data generators
  private getMockStatus(): PipelineStatusResponse {
    const now = new Date();
    return {
      pipeline_status: "idle",
      cache_stats: {
        total_entries: 1250,
        estimated_size_bytes: 52428800
      },
      recent_datasets: [
        {
          filename: "reddit_20250718.jsonl",
          path: "/Users/kevin/bigdata/new_system/output/reddit_20250718.jsonl",
          size_bytes: 2048576,
          modified: new Date(now.getTime() - 3600000).toISOString(),
          line_count: 125,
          metadata: { source: "reddit", format: "TRD" }
        },
        {
          filename: "combined_20250718.jsonl",
          path: "/Users/kevin/bigdata/new_system/output/combined_20250718.jsonl",
          size_bytes: 4194304,
          modified: new Date(now.getTime() - 7200000).toISOString(),
          line_count: 248,
          metadata: { source: "combined", format: "TRD" }
        }
      ],
      execution_info: {
        current_stage: "대기",
        collected_count: 0,
        processed_count: 0,
        quality_filtered_count: 0,
        final_count: 0,
        elapsed_time: 0,
        last_execution: new Date(now.getTime() - 3600000).toISOString(),
        next_scheduled: null,
        errors: []
      },
      timestamp: new Date().toISOString()
    };
  }

  private getMockLogs(): LogsResponse {
    return {
      logs: [
        "[2025-07-18 03:45:12] BigData TRD 형식 통일 완료",
        "[2025-07-18 03:44:58] ExcelApp과 형식 호환성 확인",
        "[2025-07-18 03:44:35] Reddit 데이터 수집 완료: 125건",
        "[2025-07-18 03:44:20] 품질 필터링 적용: 248 → 125건",
        "[2025-07-18 03:44:10] 데이터 수집 시작",
        "[2025-07-18 03:44:05] 파이프라인 초기화 완료"
      ]
    };
  }

  private async delay(ms: number): Promise<void> {
    return new Promise(resolve => setTimeout(resolve, ms));
  }

  async getStatus(): Promise<PipelineStatusResponse> {
    if (this.useMockAPI) {
      await this.delay(100); // Simulate network delay
      return this.getMockStatus();
    }
    
    const response = await fetch(`${this.baseURL}/api/status`);
    if (!response.ok) {
      throw new Error(`HTTP error! status: ${response.status}`);
    }
    return response.json();
  }

  async startPipeline(config: Partial<PipelineConfig> = {}): Promise<{ status: string; message: string }> {
    if (this.useMockAPI) {
      await this.delay(300); // Simulate network delay
      const sources = config.sources || ['stackoverflow', 'reddit'];
      return {
        status: "started",
        message: `파이프라인이 시작되었습니다. 데이터 소스: ${sources.join(', ')}`
      };
    }

    const response = await fetch(`${this.baseURL}/api/run-pipeline`, {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
      },
      body: JSON.stringify({
        sources: config.sources?.join(',') || 'stackoverflow,reddit',
        max_pages: config.maxPages || 10,
        target_count: config.targetCount || 100,
        incremental: config.incremental !== false,
        hours_back: config.hoursBack || 24,
      }),
    });

    if (!response.ok) {
      const error = await response.json();
      throw new Error(error.detail || `HTTP error! status: ${response.status}`);
    }
    
    return response.json();
  }

  async stopPipeline(): Promise<{ status: string; message: string }> {
    if (this.useMockAPI) {
      await this.delay(200);
      return {
        status: "stopping",
        message: "파이프라인 정지가 요청되었습니다."
      };
    }

    const response = await fetch(`${this.baseURL}/api/stop-pipeline`, {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
      }
    });

    if (!response.ok) {
      const error = await response.json();
      throw new Error(error.message || `HTTP error! status: ${response.status}`);
    }
    
    return response.json();
  }

  async getLogs(): Promise<LogsResponse> {
    if (this.useMockAPI) {
      await this.delay(100);
      return this.getMockLogs();
    }
    
    const response = await fetch(`${this.baseURL}/api/logs`);
    if (!response.ok) {
      throw new Error(`HTTP error! status: ${response.status}`);
    }
    return response.json();
  }

  async getDatasets() {
    if (this.useMockAPI) {
      await this.delay(200);
      return {
        datasets: [
          {
            filename: "reddit_20250718.jsonl",
            path: "/Users/kevin/bigdata/new_system/output/reddit_20250718.jsonl",
            size_bytes: 2048576,
            line_count: 125,
            created: new Date(Date.now() - 3600000).toISOString(),
            format: "TRD",
            source: "reddit"
          },
          {
            filename: "combined_20250718.jsonl",
            path: "/Users/kevin/bigdata/new_system/output/combined_20250718.jsonl",
            size_bytes: 4194304,
            line_count: 248,
            created: new Date(Date.now() - 7200000).toISOString(),
            format: "TRD",
            source: "combined"
          }
        ]
      };
    }
    
    const response = await fetch(`${this.baseURL}/api/datasets`);
    if (!response.ok) {
      throw new Error(`HTTP error! status: ${response.status}`);
    }
    return response.json();
  }

  async cleanupCache() {
    if (this.useMockAPI) {
      await this.delay(300);
      return {
        status: "success",
        message: "캐시가 정리되었습니다. 52MB의 공간이 확보되었습니다."
      };
    }
    
    const response = await fetch(`${this.baseURL}/api/cache/cleanup`, {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
      }
    });
    if (!response.ok) {
      throw new Error(`HTTP error! status: ${response.status}`);
    }
    return response.json();
  }

  // Check if backend is available
  async checkHealth(): Promise<boolean> {
    if (this.useMockAPI) {
      return true; // Mock API is always available
    }
    
    try {
      const response = await fetch(`${this.baseURL}/api/status`);
      return response.ok;
    } catch {
      return false;
    }
  }

  async startContinuousCollection(config: ContinuousConfig): Promise<{ status: string; message: string }> {
    if (this.useMockAPI) {
      await this.delay(300);
      return {
        status: "started",
        message: `지속적 수집이 시작되었습니다. 데이터 소스: ${config.sources.join(', ')}`
      };
    }

    const response = await fetch(`${this.baseURL}/api/run-continuous`, {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
      },
      body: JSON.stringify(config),
    });

    if (!response.ok) {
      const error = await response.json();
      throw new Error(error.message || `HTTP error! status: ${response.status}`);
    }
    
    return response.json();
  }

  // Start the Python pipeline directly using the main.py script
  async startPipelineDirect(config: Partial<PipelineConfig> = {}): Promise<{ success: boolean; message: string }> {
    try {
      // For development, we'll return a mock response
      // In production, this would trigger the actual Python script
      const sources = config.sources || ['stackoverflow', 'reddit'];
      const message = `선택된 데이터 소스로 파이프라인 시작: ${sources.join(', ')}`;
      
      return {
        success: true,
        message: message
      };
    } catch (error) {
      return {
        success: false,
        message: error instanceof Error ? error.message : '알 수 없는 오류가 발생했습니다'
      };
    }
  }
}

export const pipelineAPI = new PipelineAPI();

// Custom hook for real-time pipeline status updates
export function usePipelineStatus(pollInterval = 5000) {
  const [status, setStatus] = useState<PipelineStatusResponse | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [isLoading, setIsLoading] = useState(true);

  useEffect(() => {
    const fetchStatus = async () => {
      try {
        const statusData = await pipelineAPI.getStatus();
        setStatus(statusData);
        setError(null);
      } catch (err) {
        setError(err instanceof Error ? err.message : 'Failed to fetch status');
      } finally {
        setIsLoading(false);
      }
    };

    fetchStatus();
    const intervalId = setInterval(fetchStatus, pollInterval);

    return () => {
      if (intervalId) {
        clearInterval(intervalId);
      }
    };
  }, [pollInterval]);

  return { status, error, isLoading };
}

// Custom hook for real-time logs
export function usePipelineLogs(pollInterval = 3000) {
  const [logs, setLogs] = useState<string[]>([]);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    const fetchLogs = async () => {
      try {
        const logsData = await pipelineAPI.getLogs();
        if (logsData.logs) {
          setLogs(logsData.logs);
        }
        setError(null);
      } catch (err) {
        setError(err instanceof Error ? err.message : 'Failed to fetch logs');
      }
    };

    fetchLogs();
    const intervalId = setInterval(fetchLogs, pollInterval);

    return () => {
      if (intervalId) {
        clearInterval(intervalId);
      }
    };
  }, [pollInterval]);

  return { logs, error };
}

// Import React hooks
import { useState, useEffect } from 'react';