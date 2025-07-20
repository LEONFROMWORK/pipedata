import { NextResponse } from 'next/server';
import { readdir, readFile, stat, mkdir } from 'fs/promises';
import { existsSync } from 'fs';
import path from 'path';

export async function GET() {
  try {
    // 저장된 파일들이 있는 디렉토리 경로
    const outputDir = process.env.BIGDATA_SYSTEM_PATH 
      ? `${process.env.BIGDATA_SYSTEM_PATH}/output` 
      : '/tmp/bigdata/output';
    
    // 디렉토리가 없으면 생성
    if (!existsSync(outputDir)) {
      try {
        await mkdir(outputDir, { recursive: true });
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
    
    // 디렉토리 내 파일 목록 가져오기
    const files = await readdir(outputDir);
    
    // collection_으로 시작하는 .jsonl 파일만 필터링
    const collectionFiles = files.filter(file => 
      file.startsWith('collection_') && file.endsWith('.jsonl')
    );
    
    // 파일 정보 수집
    const fileInfos = await Promise.all(
      collectionFiles.map(async (filename) => {
        const filePath = path.join(outputDir, filename);
        const stats = await stat(filePath);
        
        // 파일 내용 읽어서 라인 수 계산
        const content = await readFile(filePath, 'utf-8');
        const lines = content.trim().split('\n').filter(line => line.trim());
        
        // 총 엔트리 수 계산 (각 라인의 JSON에서 totalCollected 합산)
        let totalEntries = 0;
        try {
          totalEntries = lines.reduce((sum, line) => {
            const entry = JSON.parse(line);
            return sum + (entry.totalCollected || 0);
          }, 0);
        } catch (error) {
          console.error(`파일 ${filename} 파싱 오류:`, error);
        }
        
        return {
          filename,
          path: filePath,
          size_bytes: stats.size,
          line_count: lines.length,
          total_entries: totalEntries,
          created: stats.birthtime.toISOString(),
          modified: stats.mtime.toISOString()
        };
      })
    );
    
    // 수정일 기준 내림차순 정렬 (최신 파일이 먼저)
    fileInfos.sort((a, b) => new Date(b.modified).getTime() - new Date(a.modified).getTime());
    
    return NextResponse.json({
      success: true,
      files: fileInfos,
      total_files: fileInfos.length,
      total_entries: fileInfos.reduce((sum, file) => sum + file.total_entries, 0)
    });
    
  } catch (error) {
    console.error('파일 목록 조회 실패:', error);
    return NextResponse.json({
      success: false,
      error: '파일 목록을 조회할 수 없습니다.',
      details: error instanceof Error ? error.message : '알 수 없는 오류'
    }, { status: 500 });
  }
}