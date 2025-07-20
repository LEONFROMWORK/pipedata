import { NextResponse } from 'next/server';
import { writeFile, readFile, existsSync, mkdirSync } from 'fs';
import { promisify } from 'util';
import path from 'path';

const writeFileAsync = promisify(writeFile);
const readFileAsync = promisify(readFile);

export async function POST(request: Request) {
  try {
    const { collectionData } = await request.json();
    
    // 저장할 디렉토리 경로
    const outputDir = process.env.BIGDATA_SYSTEM_PATH 
      ? `${process.env.BIGDATA_SYSTEM_PATH}/output` 
      : '/tmp/bigdata/output';
    
    // 디렉토리가 없으면 생성
    if (!existsSync(outputDir)) {
      try {
        mkdirSync(outputDir, { recursive: true });
        console.log('디렉토리 생성 완료:', outputDir);
      } catch (error) {
        console.error('디렉토리 생성 실패:', error);
        return NextResponse.json({
          success: false,
          error: '저장 디렉토리를 생성할 수 없습니다.',
          details: error instanceof Error ? error.message : '알 수 없는 오류'
        }, { status: 500 });
      }
    }
    
    // 오늘 날짜로 파일명 생성
    const today = new Date().toISOString().split('T')[0];
    const filename = `collection_${today}.jsonl`;
    const filePath = path.join(outputDir, filename);
    
    let existingData = [];
    
    // 기존 파일이 있다면 읽어오기
    if (existsSync(filePath)) {
      try {
        const fileContent = await readFileAsync(filePath, 'utf-8');
        // JSONL 형식: 각 줄이 JSON 객체
        existingData = fileContent.trim().split('\n').filter(line => line.trim()).map(line => JSON.parse(line));
      } catch (error) {
        console.error('기존 파일 읽기 실패:', error);
        existingData = [];
      }
    }
    
    // 새로운 데이터 추가
    const newEntry = {
      timestamp: new Date().toISOString(),
      session_id: `session_${Date.now()}`,
      sources: collectionData.sources,
      totalCollected: collectionData.totalCollected,
      totalProcessed: collectionData.totalProcessed,
      quality_stats: calculateQualityStats(collectionData.sources),
      metadata: {
        format: 'TRD',
        version: '1.0',
        collection_type: 'incremental'
      }
    };
    
    existingData.push(newEntry);
    
    // JSONL 형식으로 저장 (각 줄이 JSON 객체)
    const jsonlContent = existingData.map(entry => JSON.stringify(entry)).join('\n');
    
    await writeFileAsync(filePath, jsonlContent, 'utf-8');
    
    // 파일 정보 반환
    const fileStats = {
      filename,
      path: filePath,
      size_bytes: Buffer.byteLength(jsonlContent, 'utf-8'),
      line_count: existingData.length,
      total_entries: existingData.reduce((sum, entry) => sum + entry.totalCollected, 0),
      last_updated: new Date().toISOString()
    };
    
    return NextResponse.json({
      success: true,
      message: '수집 데이터가 성공적으로 저장되었습니다.',
      file_info: fileStats
    });
    
  } catch (error) {
    console.error('파일 저장 실패:', error);
    return NextResponse.json({
      success: false,
      error: '파일 저장 중 오류가 발생했습니다.',
      details: error instanceof Error ? error.message : '알 수 없는 오류'
    }, { status: 500 });
  }
}

// 품질 통계 계산 함수
interface Source {
  id: string;
  processedItems?: number;
}

interface QualityStats {
  total_excellent: number;
  total_good: number;
  total_fair: number;
  by_source: Record<string, {
    excellent: number;
    good: number;
    fair: number;
    total: number;
  }>;
}

function calculateQualityStats(sources: Source[]): QualityStats {
  const qualityStats: QualityStats = {
    total_excellent: 0,
    total_good: 0,
    total_fair: 0,
    by_source: {}
  };
  
  sources.forEach(source => {
    const processed = source.processedItems || 0;
    
    // 소스별 품질 분포 (기존 로직과 동일)
    const distributions = {
      stackoverflow: { excellent: 0.554, good: 0.329, fair: 0.117 },
      reddit: { excellent: 0.347, good: 0.498, fair: 0.155 },
      oppadu: { excellent: 0.586, good: 0.306, fair: 0.108 }
    };
    
    const distribution = distributions[source.id as keyof typeof distributions] || 
                        { excellent: 0.5, good: 0.3, fair: 0.2 };
    
    const excellent = Math.floor(processed * distribution.excellent);
    const good = Math.floor(processed * distribution.good);
    const fair = Math.max(0, processed - excellent - good);
    
    qualityStats.total_excellent += excellent;
    qualityStats.total_good += good;
    qualityStats.total_fair += fair;
    
    qualityStats.by_source[source.id] = {
      excellent,
      good,
      fair,
      total: processed
    };
  });
  
  return qualityStats;
}