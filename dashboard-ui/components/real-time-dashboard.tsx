"use client";

import { useState, useEffect } from "react";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Button } from "@/components/ui/button";
import { Checkbox } from "@/components/ui/checkbox";
import { Badge } from "@/components/ui/badge";
import { Progress } from "@/components/ui/progress";
import { Separator } from "@/components/ui/separator";
import AICostMonitor from "./ai-cost-monitor";
import { UserNav } from "@/components/ui/user-nav";
import { 
  Play, 
  Square, 
  RefreshCw, 
  Moon, 
  Sun, 
  Database, 
  Activity,
  Zap,
  Terminal,
  CheckCircle,
  Clock,
  AlertCircle,
  Settings
} from "lucide-react";


interface DataSource {
  id: string;
  name: string;
  enabled: boolean;
  status: "idle" | "collecting" | "processing" | "completed" | "error";
  collectedItems: number;
  processedItems: number;
}

export function RealTimeDashboard() {
  const [darkMode, setDarkMode] = useState(false);
  const [mounted, setMounted] = useState(false);
  // Remove isConnected - no backend to connect to

  const [dataSources, setDataSources] = useState<DataSource[]>([
    { id: "stackoverflow", name: "스택 오버플로우", enabled: true, status: "idle", collectedItems: 0, processedItems: 0 },
    { id: "reddit", name: "레딧 r/excel", enabled: true, status: "idle", collectedItems: 0, processedItems: 0 },
    { id: "oppadu", name: "오빠두 (한국 엑셀 커뮤니티)", enabled: true, status: "idle", collectedItems: 0, processedItems: 0 }
  ]);

  const [selectedSources, setSelectedSources] = useState<string[]>(["stackoverflow", "reddit", "oppadu"]);
  const [activeTab, setActiveTab] = useState<"dashboard" | "ai-monitor" | "collection-stats">("dashboard");
  // Remove runMode - only continuous collection now
  const [intervalMinutes, setIntervalMinutes] = useState(30);
  const [currentLogTab, setCurrentLogTab] = useState<"all" | "stackoverflow" | "reddit" | "oppadu">("all");
  const [isRunning, setIsRunning] = useState(false);
  const [viewMode, setViewMode] = useState<"reset" | "today" | "total">("total");

  // Local state for pipeline data (removed backend API dependencies)
  const [pipelineStatus, setPipelineStatus] = useState<{
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
    };
  } | null>(null);
  const [realTimeLogs, setRealTimeLogs] = useState<string[]>([]);
  
  // Remove single execution timers - not needed anymore

  // Initialize pipeline status and simulate data updates
  useEffect(() => {
    if (mounted) {
      // Initialize with mock data
      setPipelineStatus({
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
            modified: new Date(Date.now() - 3600000).toISOString(),
            line_count: 125,
            metadata: { source: "reddit", format: "TRD" }
          }
        ],
        execution_info: {
          current_stage: "대기",
          collected_count: 0,
          processed_count: 0,
          quality_filtered_count: 0,
          final_count: 0
        }
      });
      
      // Initialize logs
      setRealTimeLogs([
        "[2025-07-18 03:45:12] BigData TRD 형식 통일 완료",
        "[2025-07-18 03:44:58] ExcelApp과 형식 호환성 확인",
        "[2025-07-18 03:44:35] Reddit 데이터 수집 완료: 125건",
        "[2025-07-18 03:44:20] 품질 필터링 적용: 248 → 125건",
        "[2025-07-18 03:44:10] 데이터 수집 시작"
      ]);
    }
  }, [mounted]);

  // Initialize dark mode from localStorage
  useEffect(() => {
    setMounted(true);
    const saved = localStorage.getItem('darkMode');
    if (saved) {
      setDarkMode(JSON.parse(saved));
    }
  }, []);

  // Dark mode toggle effect with localStorage persistence
  useEffect(() => {
    if (!mounted) return;
    
    if (darkMode) {
      document.documentElement.classList.add("dark");
    } else {
      document.documentElement.classList.remove("dark");
    }
    localStorage.setItem('darkMode', JSON.stringify(darkMode));
  }, [darkMode, mounted]);

  // Remove connection status code - no backend to connect to

  // Update pipeline status when running
  useEffect(() => {
    if (isRunning) {
      const interval = setInterval(() => {
        const newLog = `[${new Date().toLocaleTimeString()}] ${[
          '데이터 수집 중...',
          '품질 필터링 적용',
          'TRD 형식 변환',
          'ExcelApp 호환성 확인',
          '데이터 저장 완료'
        ][Math.floor(Math.random() * 5)]}`;
        
        setRealTimeLogs(prev => [newLog, ...prev.slice(0, 19)]);
        
        // Update pipeline status with real data
        const totalCollectedCurrent = dataSources.reduce((sum, source) => sum + source.collectedItems, 0);
        const totalProcessedCurrent = dataSources.reduce((sum, source) => sum + source.processedItems, 0);
        
        setPipelineStatus(prev => prev ? {
          ...prev,
          pipeline_status: "running",
          execution_info: {
            ...prev.execution_info,
            current_stage: "수집 중",
            collected_count: totalCollectedCurrent,
            processed_count: totalProcessedCurrent,
            quality_filtered_count: totalProcessedCurrent,
            final_count: totalProcessedCurrent
          }
        } : prev);
      }, 2000);
      
      return () => clearInterval(interval);
    } else {
      // Reset to idle when stopped
      setPipelineStatus(prev => prev ? {
        ...prev,
        pipeline_status: "idle",
        execution_info: {
          ...prev.execution_info,
          current_stage: "대기"
        }
      } : prev);
    }
  }, [isRunning, dataSources]);

  // Reset all data function
  const resetAllData = () => {
    setDataSources(prev => prev.map(source => ({
      ...source,
      collectedItems: 0,
      processedItems: 0,
      status: "idle"
    })));
    setRealTimeLogs([]);
    setPipelineStatus(null);
  };

  // Reset duplicate collection tracking database
  const resetDuplicateTracking = () => {
    // Clear duplicate tracking database simulation
    localStorage.removeItem('duplicateTrackingDB');
    
    // Add log message
    const resetLog = `[${new Date().toLocaleTimeString('ko-KR')}] 중복 수집 차단 데이터베이스 초기화 완료`;
    setRealTimeLogs(prev => [resetLog, ...prev.slice(0, 19)]);
    
    console.log("중복 수집 차단 데이터베이스가 초기화되었습니다.");
  };

  // Single execution function removed - only continuous collection now

  const startDataCollection = async () => {
    console.log("데이터 수집 시작");
    console.log("선택된 소스:", selectedSources);
    
    try {
      // Set running state
      setIsRunning(true);
      
      // Update data sources to show as collecting
      setDataSources(prev => prev.map(source => ({
        ...source,
        status: selectedSources.includes(source.id) ? "collecting" : "idle"
      })));
      
      console.log("데이터 수집이 시작되었습니다");
      
      // Simulate data collection with mock data updates
      const interval = setInterval(() => {
        setDataSources(prev => prev.map(source => {
          if (selectedSources.includes(source.id) && source.status === "collecting") {
            return {
              ...source,
              collectedItems: source.collectedItems + Math.floor(Math.random() * 3) + 1,
              processedItems: source.processedItems + Math.floor(Math.random() * 2)
            };
          }
          return source;
        }));
      }, 3000); // Update every 3 seconds
      
      // Store interval ID for cleanup
      (window as { continuousCollectionInterval?: NodeJS.Timeout }).continuousCollectionInterval = interval;
      
    } catch (error) {
      console.error("데이터 수집 시작 실패:", error);
    }
  };

  const stopPipeline = async () => {
    try {
      // Set running state to false
      setIsRunning(false);
      
      // Clear data collection interval if running
      const windowWithInterval = window as { continuousCollectionInterval?: NodeJS.Timeout };
      if (windowWithInterval.continuousCollectionInterval) {
        clearInterval(windowWithInterval.continuousCollectionInterval);
        windowWithInterval.continuousCollectionInterval = undefined;
      }
      
      // Immediately set all sources to idle status when manually stopped
      setDataSources(prev => prev.map(source => ({
        ...source,
        status: "idle"
      })));
      
      // Save collected data to file system
      const collectedData = {
        timestamp: new Date().toISOString(),
        sources: dataSources.map(source => ({
          id: source.id,
          name: source.name,
          collectedItems: source.collectedItems,
          processedItems: source.processedItems,
          status: source.status
        })),
        totalCollected: dataSources.reduce((sum, source) => sum + source.collectedItems, 0),
        totalProcessed: dataSources.reduce((sum, source) => sum + source.processedItems, 0)
      };
      
      // Save to file system via API
      try {
        const saveResponse = await fetch('/api/collection/save', {
          method: 'POST',
          headers: {
            'Content-Type': 'application/json',
          },
          body: JSON.stringify({ collectionData: collectedData }),
        });
        
        const saveResult = await saveResponse.json();
        
        if (saveResult.success) {
          console.log('파일 저장 성공:', saveResult.file_info);
          
          // 저장된 파일 정보를 전역 변수에 저장 (pipeline status에서 사용)
          (window as { lastSavedFileInfo?: unknown }).lastSavedFileInfo = saveResult.file_info;
          
          // Store to localStorage as backup
          const existingData = JSON.parse(localStorage.getItem('collectionHistory') || '[]');
          existingData.push({
            ...collectedData,
            file_info: saveResult.file_info
          });
          localStorage.setItem('collectionHistory', JSON.stringify(existingData));
          
          // 성공 로그 추가
          const saveLog = `[${new Date().toLocaleTimeString('ko-KR')}] 파일 저장 완료: ${saveResult.file_info.filename} (${saveResult.file_info.total_entries}개 누적)`;
          setRealTimeLogs(prev => [...prev, saveLog]);
          
        } else {
          console.error('파일 저장 실패:', saveResult.error);
          
          // 실패 시 로컬 스토리지에만 저장
          const existingData = JSON.parse(localStorage.getItem('collectionHistory') || '[]');
          existingData.push(collectedData);
          localStorage.setItem('collectionHistory', JSON.stringify(existingData));
          
          const errorLog = `[${new Date().toLocaleTimeString('ko-KR')}] 파일 저장 실패: ${saveResult.error}`;
          setRealTimeLogs(prev => [...prev, errorLog]);
        }
        
      } catch (error) {
        console.error('파일 저장 API 호출 실패:', error);
        
        // API 호출 실패 시 로컬 스토리지에만 저장
        const existingData = JSON.parse(localStorage.getItem('collectionHistory') || '[]');
        existingData.push(collectedData);
        localStorage.setItem('collectionHistory', JSON.stringify(existingData));
        
        const errorLog = `[${new Date().toLocaleTimeString('ko-KR')}] 파일 저장 API 오류: 로컬 저장으로 대체`;
        setRealTimeLogs(prev => [...prev, errorLog]);
      }
      
      // Update data sources status to 'completed' but keep collected data
      setDataSources(prev => prev.map(source => ({
        ...source,
        status: "completed"
      })));
      
      // Update pipeline status with final counts
      const totalCollected = dataSources.reduce((sum, source) => sum + source.collectedItems, 0);
      const totalProcessed = dataSources.reduce((sum, source) => sum + source.processedItems, 0);
      
      // Use saved file info if available, otherwise use default
      const fileInfo = (window as { lastSavedFileInfo?: {
        filename: string;
        path: string;
        size_bytes: number;
        line_count: number;
        total_entries: number;
      } }).lastSavedFileInfo || {
        filename: `collection_${new Date().toISOString().split('T')[0]}.jsonl`,
        path: `/Users/kevin/bigdata/output/collection_${new Date().toISOString().split('T')[0]}.jsonl`,
        size_bytes: totalCollected * 2048,
        line_count: totalProcessed,
        total_entries: totalCollected
      };
      
      setPipelineStatus({
        pipeline_status: "completed",
        cache_stats: {
          total_entries: fileInfo.total_entries,
          estimated_size_bytes: fileInfo.size_bytes
        },
        recent_datasets: [{
          filename: fileInfo.filename,
          path: fileInfo.path,
          size_bytes: fileInfo.size_bytes,
          modified: new Date().toISOString(),
          line_count: fileInfo.line_count,
          metadata: { 
            sources: dataSources.filter(s => s.collectedItems > 0).map(s => s.id),
            format: "TRD",
            total_entries: fileInfo.total_entries
          }
        }],
        execution_info: {
          current_stage: "완료",
          collected_count: totalCollected,
          processed_count: totalProcessed,
          quality_filtered_count: totalProcessed, // 품질 필터링 = 처리됨과 동일
          final_count: totalProcessed // 최종 출력 = 처리됨과 동일
        }
      });
      
      // Add completion log
      const completionLog = `[${new Date().toLocaleTimeString('ko-KR')}] 수집 완료: ${totalCollected}개 수집, ${totalProcessed}개 처리됨`;
      setRealTimeLogs(prev => [...prev, completionLog]);
      
      console.log("파이프라인이 정지되었습니다. 수집된 데이터가 저장되었습니다.");
      console.log("수집 결과:", collectedData);
    } catch (error) {
      console.error("파이프라인 정지 실패:", error);
    }
  };

  // resetAllData function removed - duplicate definition

  const toggleDataSource = (sourceId: string) => {
    setDataSources(prev => prev.map(source => 
      source.id === sourceId ? { ...source, enabled: !source.enabled } : source
    ));
    
    if (selectedSources.includes(sourceId)) {
      setSelectedSources(prev => prev.filter(id => id !== sourceId));
    } else {
      setSelectedSources(prev => [...prev, sourceId]);
    }
  };

  const selectAllSources = () => {
    const allIds = dataSources.map(source => source.id);
    setSelectedSources(allIds);
    setDataSources(prev => prev.map(source => ({ ...source, enabled: true })));
  };

  const getStatusIcon = (status: string) => {
    switch (status) {
      case "collecting": return <Activity className="h-4 w-4 text-blue-500 animate-spin" />;
      case "processing": return <Zap className="h-4 w-4 text-yellow-500 animate-pulse" />;
      case "completed": return <CheckCircle className="h-4 w-4 text-green-500" />;
      case "error": return <AlertCircle className="h-4 w-4 text-red-500" />;
      default: return <Clock className="h-4 w-4 text-gray-500" />;
    }
  };

  // isRunning state is now managed locally
  const currentStage = pipelineStatus?.execution_info?.current_stage || "idle";
  const progress = getProgressPercentage(currentStage);

  // Prevent hydration mismatch by not rendering until mounted
  if (!mounted) {
    return (
      <div className="min-h-screen bg-background text-foreground flex items-center justify-center">
        <div className="text-lg">로딩 중...</div>
      </div>
    );
  }

  return (
    <div className="min-h-screen bg-background text-foreground transition-colors">
      {/* Header */}
      <div id="main-header" className="border-b">
        <div className="container mx-auto px-4 py-4">
          <div className="flex items-center justify-between">
            <div id="header-title">
              <h1 className="text-2xl font-bold">파이프라인</h1>
            </div>
            <div className="flex items-center gap-4">
              <div id="tab-navigation" className="flex items-center gap-2">
                <Button
                  variant={activeTab === "dashboard" ? "default" : "outline"}
                  size="sm"
                  onClick={() => setActiveTab("dashboard")}
                >
                  <Activity className="h-4 w-4 mr-2" />
                  Pipeline
                </Button>
                <Button
                  variant={activeTab === "ai-monitor" ? "default" : "outline"}
                  size="sm"
                  onClick={() => setActiveTab("ai-monitor")}
                >
                  <Settings className="h-4 w-4 mr-2" />
                  AI Monitor
                </Button>
                <Button
                  variant={activeTab === "collection-stats" ? "default" : "outline"}
                  size="sm"
                  onClick={() => setActiveTab("collection-stats")}
                >
                  <Database className="h-4 w-4 mr-2" />
                  수집 통계
                </Button>
              </div>
              {/* Connection status removed - no backend to connect to */}
              <Button
                id="theme-toggle"
                variant="outline"
                size="sm"
                onClick={() => setDarkMode(!darkMode)}
              >
                {mounted && darkMode ? <Sun className="h-4 w-4" /> : <Moon className="h-4 w-4" />}
              </Button>
              <UserNav />
            </div>
          </div>
        </div>
      </div>

      <div className="container mx-auto px-4 py-6 space-y-6">
        {activeTab === "dashboard" && (
          <div className="space-y-6">
        {/* Control Panel */}
        <Card id="control-panel">
          <CardHeader>
            <CardTitle className="flex items-center gap-2">
              <Database className="h-5 w-5" />
              파이프라인 제어
            </CardTitle>
          </CardHeader>
          <CardContent className="space-y-4">
            {/* Data Source Selection */}
            <div id="data-source-selection">
              <div className="flex items-center justify-between mb-3">
                <h3 className="text-sm font-medium">데이터 소스</h3>
                <Button variant="outline" size="sm" onClick={selectAllSources}>
                  전체 선택
                </Button>
              </div>
              <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
                {dataSources.map((source) => (
                  <div key={source.id} className="flex items-center justify-between p-3 border rounded-lg">
                    <div className="flex items-center gap-3">
                      <Checkbox
                        checked={source.enabled}
                        onCheckedChange={() => toggleDataSource(source.id)}
                      />
                      <div>
                        <div className="flex items-center gap-2">
                          {getStatusIcon(source.status)}
                          <span className="font-medium">{source.name}</span>
                        </div>
                        <div className="text-xs text-muted-foreground">
                          수집됨: {source.collectedItems} | 처리됨: {source.processedItems}
                        </div>
                      </div>
                    </div>
                    <Badge variant={source.enabled ? "default" : "secondary"}>
                      {source.enabled ? "활성화" : "비활성화"}
                    </Badge>
                  </div>
                ))}
              </div>
            </div>

            <Separator />

            {/* Run Mode Selection */}
            <div id="run-mode-selection" className="space-y-3">
              <div className="flex items-center gap-2">
                <label className="text-sm">수집 간격 (분):</label>
                <input 
                  type="number" 
                  value={intervalMinutes} 
                  onChange={(e) => setIntervalMinutes(parseInt(e.target.value) || 30)}
                  min={5} 
                  max={1440} 
                  className="w-20 px-2 py-1 border rounded text-sm"
                />
              </div>
            </div>

            {/* Pipeline Controls */}
            <div id="pipeline-controls" className="flex gap-2">
              <Button 
                onClick={startDataCollection} 
                disabled={isRunning || selectedSources.length === 0}
                className="flex items-center gap-2"
              >
                <Play className="h-4 w-4" />
                데이터 수집
              </Button>
              <Button 
                variant="outline" 
                onClick={stopPipeline}
                disabled={!isRunning}
                className="flex items-center gap-2"
              >
                <Square className="h-4 w-4" />
                수집 중지
              </Button>
              <Button variant="outline" onClick={() => window.location.reload()}>
                <RefreshCw className="h-4 w-4" />
                새로고침
              </Button>
            </div>
          </CardContent>
        </Card>

        {/* Status Dashboard */}
        <div className="space-y-6">
          {/* Overall Pipeline Status */}
          <Card id="overall-pipeline-status">
            <CardHeader>
              <CardTitle className="flex items-center gap-2">
                <Activity className="h-5 w-5" />
                전체 파이프라인 상태
              </CardTitle>
            </CardHeader>
            <CardContent className="space-y-4">
              <div className="space-y-2">
                <div className="flex justify-between text-sm">
                  <span>현재 단계: <strong>{getKoreanStage(currentStage)}</strong></span>
                  <span>{progress}%</span>
                </div>
                <Progress value={progress} className="h-2" />
              </div>

              <div id="pipeline-stats" className="grid grid-cols-2 md:grid-cols-4 gap-4 text-center">
                <div>
                  <div className="text-2xl font-bold">
                    {pipelineStatus?.execution_info?.collected_count || 0}
                  </div>
                  <div className="text-xs text-muted-foreground">수집됨</div>
                </div>
                <div>
                  <div className="text-2xl font-bold">
                    {pipelineStatus?.execution_info?.processed_count || 0}
                  </div>
                  <div className="text-xs text-muted-foreground">처리됨</div>
                </div>
                <div>
                  <div className="text-2xl font-bold">
                    {pipelineStatus?.execution_info?.quality_filtered_count || 0}
                  </div>
                  <div className="text-xs text-muted-foreground">품질 필터링</div>
                </div>
                <div>
                  <div className="text-2xl font-bold">
                    {pipelineStatus?.execution_info?.final_count || 0}
                  </div>
                  <div className="text-xs text-muted-foreground">최종 출력</div>
                </div>
              </div>
            </CardContent>
          </Card>

          {/* Source-specific Pipeline Status */}
          <Card id="source-specific-status">
            <CardHeader>
              <div className="flex items-center justify-between">
                <CardTitle className="flex items-center gap-2">
                  <Database className="h-5 w-5" />
                  수집처별 상세 현황
                </CardTitle>
                <div className="flex items-center gap-2">
                  <Button
                    variant="outline"
                    size="sm"
                    onClick={() => {
                      resetAllData();
                      setViewMode("total");
                    }}
                  >
                    <RefreshCw className="h-4 w-4 mr-1" />
                    초기화
                  </Button>
                  <Button
                    variant="outline"
                    size="sm"
                    onClick={resetDuplicateTracking}
                  >
                    <Database className="h-4 w-4 mr-1" />
                    중복 수집 초기화
                  </Button>
                  <Button
                    variant={viewMode === "today" ? "default" : "outline"}
                    size="sm"
                    onClick={() => setViewMode("today")}
                  >
                    오늘 수집량
                  </Button>
                  <Button
                    variant={viewMode === "total" ? "default" : "outline"}
                    size="sm"
                    onClick={() => setViewMode("total")}
                  >
                    전체 현황
                  </Button>
                </div>
              </div>
            </CardHeader>
            <CardContent>
              <div id="source-stats-grid" className="grid grid-cols-1 lg:grid-cols-3 gap-6">
                {dataSources.map((source) => {
                  const sourceStats = getSourceStats(source.id, viewMode, source);
                  return (
                    <div key={source.id} id={`source-${source.id}`} className="p-4 border rounded-lg space-y-3">
                      <div className="flex items-center justify-between">
                        <div className="flex items-center gap-2">
                          {getStatusIcon(source.status)}
                          <span className="font-semibold">{source.name}</span>
                        </div>
                        <Badge variant={source.enabled ? "default" : "secondary"}>
                          {source.enabled ? "활성" : "비활성"}
                        </Badge>
                      </div>
                      
                      {/* 수집량 표시 */}
                      <div className="grid grid-cols-2 gap-2 text-sm">
                        <div className="text-center p-2 bg-blue-50 dark:bg-blue-950 rounded">
                          <div className="font-bold text-blue-600">{sourceStats.collected}</div>
                          <div className="text-xs text-muted-foreground">수집됨</div>
                        </div>
                        <div className="text-center p-2 bg-green-50 dark:bg-green-950 rounded">
                          <div className="font-bold text-green-600">{sourceStats.processed}</div>
                          <div className="text-xs text-muted-foreground">처리됨</div>
                        </div>
                      </div>
                      
                      {/* 품질별 분류 */}
                      <div id={`quality-stats-${source.id}`}>
                        <div className="text-sm font-medium mb-2">품질별 분류</div>
                        <div className="space-y-1">
                          <div className="flex justify-between text-xs">
                            <span className="flex items-center gap-1">
                              <span>우수 (8-10점)</span>
                            </span>
                            <span className="font-semibold text-foreground">
                              {sourceStats.quality.excellent}개 ({sourceStats.quality.excellentPercent}%)
                            </span>
                          </div>
                          <div className="flex justify-between text-xs">
                            <span className="flex items-center gap-1">
                              <span>양호 (6-7점)</span>
                            </span>
                            <span className="font-semibold text-foreground">
                              {sourceStats.quality.good}개 ({sourceStats.quality.goodPercent}%)
                            </span>
                          </div>
                          <div className="flex justify-between text-xs">
                            <span className="flex items-center gap-1">
                              <span>보통 (1-5점)</span>
                            </span>
                            <span className="font-semibold text-foreground">
                              {sourceStats.quality.fair}개 ({sourceStats.quality.fairPercent}%)
                            </span>
                          </div>
                        </div>
                        
                        {/* 품질 진행률 바 */}
                        <div className="mt-2 space-y-1">
                          <div className="flex h-2 rounded-full bg-gray-200 dark:bg-gray-700 overflow-hidden">
                            <div 
                              className="bg-green-500 transition-all duration-300" 
                              style={{ width: `${sourceStats.quality.excellentPercent}%` }}
                            ></div>
                            <div 
                              className="bg-yellow-500 transition-all duration-300" 
                              style={{ width: `${sourceStats.quality.goodPercent}%` }}
                            ></div>
                            <div 
                              className="bg-red-500 transition-all duration-300" 
                              style={{ width: `${sourceStats.quality.fairPercent}%` }}
                            ></div>
                          </div>
                        </div>
                      </div>
                      
                      {/* 추가 정보 */}
                      <div className="grid grid-cols-2 gap-2 text-xs text-muted-foreground">
                        <div className="flex justify-between">
                          <span>이미지:</span>
                          <span className="font-medium">{sourceStats.withImages}개</span>
                        </div>
                        <div className="flex justify-between">
                          <span>평균 품질:</span>
                          <span className="font-medium">{sourceStats.avgQuality}</span>
                        </div>
                      </div>
                    </div>
                  );
                })}
              </div>
            </CardContent>
          </Card>
        

        {/* Real-time Logs with Rich Terminal Style */}
        <Card id="realtime-logs">
          <CardHeader>
            <div className="flex items-center justify-between">
              <CardTitle className="flex items-center gap-2">
                <Terminal className="h-5 w-5" />
                실시간 파이프라인 로그
              </CardTitle>
              <div className="flex items-center gap-2">
                <Button
                  variant="outline"
                  size="sm"
                  onClick={() => setRealTimeLogs([])}
                >
                  <RefreshCw className="h-4 w-4 mr-1" />
                  리프레시
                </Button>
                <Badge variant={realTimeLogs.length > 0 ? "default" : "secondary"}>
                  {realTimeLogs.length} lines
                </Badge>
              </div>
            </div>
          </CardHeader>
          <CardContent>
            {/* Log Tabs */}
            <div id="log-tabs" className="flex mb-3 border-b">
              {["all", "stackoverflow", "reddit", "oppadu"].map((tab) => (
                <button
                  key={tab}
                  onClick={() => setCurrentLogTab(tab as typeof currentLogTab)}
                  className={`px-4 py-2 text-sm font-medium border-b-2 transition-colors ${
                    currentLogTab === tab 
                      ? "border-primary text-primary" 
                      : "border-transparent text-muted-foreground hover:text-foreground"
                  }`}
                >
                  {tab === "all" ? "전체" : 
                   tab === "stackoverflow" ? "Stack Overflow" :
                   tab === "reddit" ? "Reddit" : "오빠두"}
                </button>
              ))}
            </div>
            
            {/* Retro Terminal */}
            <div id="terminal-container" className="bg-black border-2 border-green-400 rounded-lg overflow-hidden shadow-lg shadow-green-400/20">
              <style dangerouslySetInnerHTML={{
                __html: `
                  @keyframes blink {
                    0%, 50% { opacity: 1; }
                    51%, 100% { opacity: 0; }
                  }
                `
              }} />
              {/* Terminal Header */}
              <div id="terminal-header" className="bg-green-800 px-4 py-1 border-b border-green-400">
                <div className="flex items-center gap-2">
                  <span className="text-xs text-green-100" style={{ fontFamily: 'Courier New, Monaco, Lucida Console, Liberation Mono, DejaVu Sans Mono, Bitstream Vera Sans Mono, Courier, monospace' }}>
                    █ EXCEL-QA PIPELINE MONITOR v1.0 █
                  </span>
                </div>
              </div>
              
              {/* Terminal Body */}
              <div id="terminal-body" className="p-4 h-80 overflow-y-auto bg-black text-sm" style={{ fontFamily: 'Courier New, Monaco, Lucida Console, Liberation Mono, DejaVu Sans Mono, Bitstream Vera Sans Mono, Courier, monospace' }}>
                {realTimeLogs.length === 0 ? (
                  <div className="space-y-1">
                    <div className="text-green-400">
                      &gt; SYSTEM READY
                    </div>
                    <div className="text-green-400">
                      &gt; WAITING FOR DATA COLLECTION...
                    </div>
                    <div className="text-green-400 flex items-center">
                      &gt; STATUS: IDLE
                      <span className="text-green-400 ml-2" style={{ animation: 'blink 1s infinite' }}>_</span>
                    </div>
                  </div>
                ) : (
                  <div className="space-y-1">
                    {getFilteredLogs(realTimeLogs, currentLogTab).slice(-15).map((log, index) => (
                      <div key={index} className="text-green-400 hover:text-green-300 transition-colors">
                        &gt; {log}
                      </div>
                    ))}
                    <div className="text-green-400 flex items-center">
                      &gt; SYSTEM ACTIVE
                      <span className="text-green-400 ml-2" style={{ animation: 'blink 1s infinite' }}>_</span>
                    </div>
                  </div>
                )}
              </div>
            </div>
          </CardContent>
        </Card>
        </div>
          </div>
        )}
        
        {activeTab === "ai-monitor" && (
          <AICostMonitor />
        )}
        
        {activeTab === "collection-stats" && (
          <CollectionStatsPanel />
        )}
      </div>
    </div>
  );
}

// Helper function to get Korean stage name
function getKoreanStage(stage: string): string {
  const stageNames: Record<string, string> = {
    "idle": "대기",
    "initializing": "초기화",
    "collection": "데이터 수집",
    "processing": "데이터 처리",
    "ai_processing": "AI 처리",
    "quality_filtering": "품질 필터링",
    "deduplication": "중복 제거",
    "completed": "완료",
    "stopped": "중지됨"
  };
  
  return stageNames[stage] || stage;
}

// Helper function to calculate progress percentage
function getProgressPercentage(stage: string): number {
  const stageProgress: Record<string, number> = {
    "idle": 0,
    "initializing": 5,
    "collection": 20,
    "processing": 50,
    "ai_processing": 70,
    "quality_filtering": 85,
    "deduplication": 95,
    "completed": 100
  };
  
  return stageProgress[stage] || 0;
}

// Helper function to filter logs by source
function getFilteredLogs(logs: string[], source: string): string[] {
  if (source === "all") return logs;
  return logs.filter(log => log.toLowerCase().includes(source.toLowerCase()));
}


// Collection Stats Panel Component
function CollectionStatsPanel() {
  const [stats, setStats] = useState<{
    total_collected: number;
    total_with_images: number;
    sources: Record<string, {
      total: number;
      quality: { excellent: number; good: number; fair: number };
      with_images: number;
    }>;
    weekly_trend: {
      days: Array<{ date: string; count: number }>;
    };
  } | null>(null);
  const [loading, setLoading] = useState(true);
  
  useEffect(() => {
    const loadStats = async () => {
      try {
        // Initialize with empty data - will be populated by actual pipeline data
        const emptyStats = {
          total_collected: 0,
          total_with_images: 0,
          sources: {
            stackoverflow: {
              total: 0,
              quality: { excellent: 0, good: 0, fair: 0 },
              with_images: 0
            },
            reddit: {
              total: 0,
              quality: { excellent: 0, good: 0, fair: 0 },
              with_images: 0
            },
            oppadu: {
              total: 0,
              quality: { excellent: 0, good: 0, fair: 0 },
              with_images: 0
            }
          },
          weekly_trend: {
            days: [
              { date: '2025-07-12', count: 0 },
              { date: '2025-07-13', count: 0 },
              { date: '2025-07-14', count: 0 },
              { date: '2025-07-15', count: 0 },
              { date: '2025-07-16', count: 0 },
              { date: '2025-07-17', count: 0 },
              { date: '2025-07-18', count: 0 }
            ]
          }
        };
        
        setStats(emptyStats);
      } catch (error) {
        console.error('Failed to load collection stats:', error);
      } finally {
        setLoading(false);
      }
    };
    
    loadStats();
    const interval = setInterval(loadStats, 30000); // Update every 30 seconds
    
    return () => clearInterval(interval);
  }, []);
  
  if (loading) {
    return (
      <Card>
        <CardContent className="p-6">
          <div className="text-center text-muted-foreground">수집 통계 로딩 중...</div>
        </CardContent>
      </Card>
    );
  }
  
  if (!stats) {
    return (
      <Card>
        <CardContent className="p-6">
          <div className="text-center text-red-500">수집 통계를 로드할 수 없습니다.</div>
        </CardContent>
      </Card>
    );
  }
  
  return (
    <div className="space-y-6">
      {/* Overview Cards */}
      <div className="grid grid-cols-1 md:grid-cols-4 gap-6">
        <Card>
          <CardContent className="p-6 text-center">
            <div className="text-3xl font-bold text-blue-600">{stats.total_collected.toLocaleString()}개</div>
            <div className="text-sm text-muted-foreground">오늘 수집 총량</div>
            <div className="text-xs mt-1">이미지: {stats.total_with_images}개</div>
          </CardContent>
        </Card>
        
        {Object.entries(stats.sources).map(([source, data]) => {
          const sourceName = source === 'stackoverflow' ? 'Stack Overflow' : 
                           source === 'reddit' ? 'Reddit' : '오빠두';
          return (
            <Card key={source}>
              <CardContent className="p-6 text-center">
                <div className="text-2xl font-bold text-green-600">{data.total.toLocaleString()}개</div>
                <div className="text-sm text-muted-foreground">{sourceName}</div>
                <div className="text-xs mt-1">이미지: {data.with_images}개</div>
              </CardContent>
            </Card>
          );
        })}
      </div>
      
      {/* Quality Analysis */}
      <Card>
        <CardHeader>
          <CardTitle>수집처별 품질 분석</CardTitle>
        </CardHeader>
        <CardContent>
          <div className="grid grid-cols-1 md:grid-cols-3 gap-6">
            {Object.entries(stats.sources).map(([source, data]) => {
              const sourceName = source === 'stackoverflow' ? 'Stack Overflow' : 
                               source === 'reddit' ? 'Reddit' : '오빠두';
              const total = data.total;
              const { excellent, good, fair } = data.quality;
              
              return (
                <div key={source} className="p-4 border rounded-lg">
                  <h4 className="font-semibold mb-3">{sourceName} ({total}개)</h4>
                  <div className="space-y-2">
                    <div className="flex justify-between">
                      <span className="text-sm">우수 (8-10점):</span>
                      <span className="font-bold text-foreground">
                        {excellent}개 ({total > 0 ? (excellent/total*100).toFixed(1) : 0}%)
                      </span>
                    </div>
                    <div className="flex justify-between">
                      <span className="text-sm">양호 (6-7점):</span>
                      <span className="font-bold text-foreground">
                        {good}개 ({total > 0 ? (good/total*100).toFixed(1) : 0}%)
                      </span>
                    </div>
                    <div className="flex justify-between">
                      <span className="text-sm">보통 (1-5점):</span>
                      <span className="font-bold text-foreground">
                        {fair}개 ({total > 0 ? (fair/total*100).toFixed(1) : 0}%)
                      </span>
                    </div>
                    <div className="pt-2 border-t">
                      <div className="flex justify-between">
                        <span className="text-sm">이미지:</span>
                        <span className="font-bold text-blue-600">
                          {data.with_images}개 ({total > 0 ? (data.with_images/total*100).toFixed(1) : 0}%)
                        </span>
                      </div>
                    </div>
                  </div>
                </div>
              );
            })}
          </div>
        </CardContent>
      </Card>
      
      {/* Weekly Trend */}
      <Card>
        <CardHeader>
          <CardTitle>주간 수집 트렌드 (최근 7일)</CardTitle>
        </CardHeader>
        <CardContent>
          <div className="grid grid-cols-7 gap-2">
            {stats.weekly_trend.days.map((day) => (
              <div key={day.date} className="text-center p-3 bg-muted rounded-lg">
                <div className="text-xs text-muted-foreground mb-1">
                  {new Date(day.date).toLocaleDateString('ko-KR', { month: 'short', day: 'numeric' })}
                </div>
                <div className="font-bold text-lg text-primary">{day.count}</div>
              </div>
            ))}
          </div>
        </CardContent>
      </Card>
    </div>
  );
}

// Helper function to get source-specific stats
function getSourceStats(sourceId: string, viewMode: "reset" | "today" | "total" = "total", source?: DataSource) {
  // Use actual source data if available
  if (source) {
    const collected = source.collectedItems || 0;
    const processed = source.processedItems || 0;
    return {
      collected: collected,
      processed: processed,
      quality: calculateQualityStats(processed, sourceId),
      withImages: Math.floor(collected * 0.25),
      avgQuality: getAvgQuality(sourceId)
    };
  }
  
  // Fallback to hardcoded data only when source is not available
  const totalData = {
    stackoverflow: {
      collected: 342,
      processed: 298,
      quality: {
        excellent: 165,
        good: 98,
        fair: 35,
        excellentPercent: 55.4,
        goodPercent: 32.9,
        fairPercent: 11.7
      },
      withImages: 89,
      avgQuality: '7.8'
    },
    reddit: {
      collected: 267,
      processed: 225,
      quality: {
        excellent: 78,
        good: 112,
        fair: 35,
        excellentPercent: 34.7,
        goodPercent: 49.8,
        fairPercent: 15.5
      },
      withImages: 156,
      avgQuality: '6.4'
    },
    oppadu: {
      collected: 184,
      processed: 157,
      quality: {
        excellent: 92,
        good: 48,
        fair: 17,
        excellentPercent: 58.6,
        goodPercent: 30.6,
        fairPercent: 10.8
      },
      withImages: 23,
      avgQuality: '7.6'
    }
  };

  const todayData = {
    stackoverflow: {
      collected: 45,
      processed: 38,
      quality: {
        excellent: 21,
        good: 12,
        fair: 5,
        excellentPercent: 55.3,
        goodPercent: 31.6,
        fairPercent: 13.1
      },
      withImages: 11,
      avgQuality: '7.7'
    },
    reddit: {
      collected: 33,
      processed: 28,
      quality: {
        excellent: 10,
        good: 14,
        fair: 4,
        excellentPercent: 35.7,
        goodPercent: 50.0,
        fairPercent: 14.3
      },
      withImages: 19,
      avgQuality: '6.5'
    },
    oppadu: {
      collected: 22,
      processed: 19,
      quality: {
        excellent: 11,
        good: 6,
        fair: 2,
        excellentPercent: 57.9,
        goodPercent: 31.6,
        fairPercent: 10.5
      },
      withImages: 3,
      avgQuality: '7.5'
    }
  };

  const resetData = {
    stackoverflow: {
      collected: 0,
      processed: 0,
      quality: {
        excellent: 0,
        good: 0,
        fair: 0,
        excellentPercent: 0,
        goodPercent: 0,
        fairPercent: 0
      },
      withImages: 0,
      avgQuality: '0.0'
    },
    reddit: {
      collected: 0,
      processed: 0,
      quality: {
        excellent: 0,
        good: 0,
        fair: 0,
        excellentPercent: 0,
        goodPercent: 0,
        fairPercent: 0
      },
      withImages: 0,
      avgQuality: '0.0'
    },
    oppadu: {
      collected: 0,
      processed: 0,
      quality: {
        excellent: 0,
        good: 0,
        fair: 0,
        excellentPercent: 0,
        goodPercent: 0,
        fairPercent: 0
      },
      withImages: 0,
      avgQuality: '0.0'
    }
  };

  
  // Fallback to static data when no source is provided
  let selectedData;
  switch (viewMode) {
    case "reset":
      selectedData = resetData;
      break;
    case "today":
      selectedData = todayData;
      break;
    case "total":
    default:
      selectedData = totalData;
      break;
  }
  
  return selectedData[sourceId as keyof typeof selectedData] || {
    collected: 0,
    processed: 0,
    quality: {
      excellent: 0,
      good: 0,
      fair: 0,
      excellentPercent: 0,
      goodPercent: 0,
      fairPercent: 0
    },
    withImages: 0,
    avgQuality: '0.0'
  };
}

// Helper function to calculate quality stats based on processed items
function calculateQualityStats(processedItems: number, sourceId: string) {
  if (processedItems === 0) {
    return {
      excellent: 0,
      good: 0,
      fair: 0,
      excellentPercent: 0,
      goodPercent: 0,
      fairPercent: 0
    };
  }
  
  // Different quality distributions per source
  const qualityDistributions = {
    stackoverflow: { excellent: 0.554, good: 0.329, fair: 0.117 },
    reddit: { excellent: 0.347, good: 0.498, fair: 0.155 },
    oppadu: { excellent: 0.586, good: 0.306, fair: 0.108 }
  };
  
  const distribution = qualityDistributions[sourceId as keyof typeof qualityDistributions] || 
                      { excellent: 0.5, good: 0.3, fair: 0.2 };
  
  const excellent = Math.floor(processedItems * distribution.excellent);
  const good = Math.floor(processedItems * distribution.good);
  const fair = Math.max(0, processedItems - excellent - good);
  
  return {
    excellent,
    good,
    fair,
    excellentPercent: processedItems > 0 ? Number((excellent / processedItems * 100).toFixed(1)) : 0,
    goodPercent: processedItems > 0 ? Number((good / processedItems * 100).toFixed(1)) : 0,
    fairPercent: processedItems > 0 ? Number((fair / processedItems * 100).toFixed(1)) : 0
  };
}

// Helper function to get average quality per source
function getAvgQuality(sourceId: string): string {
  const avgQualities = {
    stackoverflow: '7.8',
    reddit: '6.4',
    oppadu: '7.6'
  };
  
  return avgQualities[sourceId as keyof typeof avgQualities] || '6.0';
}

