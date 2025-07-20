#!/usr/bin/env python3
"""
수집된 Stack Overflow 데이터의 데이터베이스 저장 및 조회 테스트
"""
import asyncio
import json
import sqlite3
import sys
from pathlib import Path
from datetime import datetime

sys.path.insert(0, str(Path(__file__).parent))

from config import Config
from core.cache import LocalCache, APICache

def extract_cached_stackoverflow_data():
    """캐시에서 Stack Overflow 데이터 추출"""
    print("📊 캐시에서 Stack Overflow 데이터 추출")
    print("=" * 50)
    
    cache_db_path = Config.DATABASE_PATH
    all_questions = []
    
    with sqlite3.connect(cache_db_path) as conn:
        cursor = conn.execute("SELECT key, value FROM cache WHERE key LIKE 'so_api:%'")
        cache_entries = cursor.fetchall()
        
        for key, value_str in cache_entries:
            try:
                data = json.loads(value_str)
                if 'items' in data and len(data['items']) > 0:
                    # 질문 데이터인지 확인
                    first_item = data['items'][0]
                    if 'question_id' in first_item:
                        all_questions.extend(data['items'])
                        print(f"   추출: {len(data['items'])}개 질문 (키: {key[:20]}...)")
            except Exception as e:
                print(f"   ⚠️ 데이터 파싱 실패 (키: {key[:20]}...): {e}")
    
    # 중복 제거 (question_id 기준)
    unique_questions = {}
    for q in all_questions:
        qid = q.get('question_id')
        if qid and qid not in unique_questions:
            unique_questions[qid] = q
    
    final_questions = list(unique_questions.values())
    print(f"✅ 총 {len(final_questions)}개의 고유 질문 추출 완료")
    
    return final_questions

def create_analysis_database():
    """분석용 데이터베이스 생성"""
    print("\n🗄️ 분석용 데이터베이스 생성")
    print("=" * 50)
    
    db_path = Path(Config.DATA_DIR) / "stackoverflow_analysis.db"
    
    with sqlite3.connect(db_path) as conn:
        # 질문 테이블 생성
        conn.execute('''
            CREATE TABLE IF NOT EXISTS questions (
                question_id INTEGER PRIMARY KEY,
                title TEXT NOT NULL,
                body_markdown TEXT,
                score INTEGER DEFAULT 0,
                view_count INTEGER DEFAULT 0,
                favorite_count INTEGER DEFAULT 0,
                is_answered BOOLEAN DEFAULT 0,
                accepted_answer_id INTEGER,
                tags TEXT,  -- JSON array as text
                owner_reputation INTEGER DEFAULT 0,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                collected_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''')
        
        # 답변 테이블 생성
        conn.execute('''
            CREATE TABLE IF NOT EXISTS answers (
                answer_id INTEGER PRIMARY KEY,
                question_id INTEGER,
                body_markdown TEXT,
                score INTEGER DEFAULT 0,
                owner_reputation INTEGER DEFAULT 0,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (question_id) REFERENCES questions (question_id)
            )
        ''')
        
        # 인덱스 생성
        conn.execute('CREATE INDEX IF NOT EXISTS idx_questions_score ON questions(score)')
        conn.execute('CREATE INDEX IF NOT EXISTS idx_questions_tags ON questions(tags)')
        conn.execute('CREATE INDEX IF NOT EXISTS idx_answers_question_id ON answers(question_id)')
        
        conn.commit()
    
    print(f"✅ 데이터베이스 생성 완료: {db_path}")
    return db_path

def store_questions_in_database(questions, db_path):
    """질문 데이터를 데이터베이스에 저장"""
    print(f"\n💾 데이터베이스에 {len(questions)}개 질문 저장")
    print("=" * 50)
    
    stored_questions = 0
    stored_answers = 0
    
    with sqlite3.connect(db_path) as conn:
        for question in questions:
            try:
                # 질문 데이터 저장
                conn.execute('''
                    INSERT OR REPLACE INTO questions (
                        question_id, title, body_markdown, score, view_count, 
                        favorite_count, is_answered, accepted_answer_id, 
                        tags, owner_reputation, collected_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ''', (
                    question.get('question_id'),
                    question.get('title', ''),
                    question.get('body_markdown', ''),
                    question.get('score', 0),
                    question.get('view_count', 0),
                    question.get('favorite_count', 0),
                    question.get('is_answered', False),
                    question.get('accepted_answer_id'),
                    json.dumps(question.get('tags', [])),
                    question.get('owner', {}).get('reputation', 0),
                    datetime.now().isoformat()
                ))
                stored_questions += 1
                
                # 채택된 답변이 있으면 저장
                if question.get('accepted_answer'):
                    answer = question['accepted_answer']
                    conn.execute('''
                        INSERT OR REPLACE INTO answers (
                            answer_id, question_id, body_markdown, score, 
                            owner_reputation
                        ) VALUES (?, ?, ?, ?, ?)
                    ''', (
                        answer.get('answer_id'),
                        question.get('question_id'),
                        answer.get('body_markdown', ''),
                        answer.get('score', 0),
                        answer.get('owner', {}).get('reputation', 0)
                    ))
                    stored_answers += 1
                    
            except Exception as e:
                print(f"   ❌ 질문 {question.get('question_id')} 저장 실패: {e}")
        
        conn.commit()
    
    print(f"✅ 저장 완료:")
    print(f"   질문: {stored_questions}개")
    print(f"   답변: {stored_answers}개")

def analyze_stored_data(db_path):
    """저장된 데이터 분석"""
    print(f"\n📈 저장된 데이터 분석")
    print("=" * 50)
    
    with sqlite3.connect(db_path) as conn:
        # 기본 통계
        question_count = conn.execute("SELECT COUNT(*) FROM questions").fetchone()[0]
        answer_count = conn.execute("SELECT COUNT(*) FROM answers").fetchone()[0]
        answered_count = conn.execute("SELECT COUNT(*) FROM questions WHERE is_answered = 1").fetchone()[0]
        
        print(f"📊 기본 통계:")
        print(f"   총 질문: {question_count}개")
        print(f"   총 답변: {answer_count}개")
        print(f"   답변된 질문: {answered_count}개 ({answered_count/question_count*100:.1f}%)")
        
        # 점수 분포
        score_stats = conn.execute('''
            SELECT MIN(score), MAX(score), AVG(score)
            FROM questions
        ''').fetchone()
        
        print(f"   질문 점수: 최소 {score_stats[0]}, 최대 {score_stats[1]}, 평균 {score_stats[2]:.1f}")
        
        # 인기 태그 분석
        print(f"\n🏷️ 태그 분석:")
        tag_analysis = conn.execute("SELECT tags FROM questions WHERE tags != '[]'").fetchall()
        
        all_tags = []
        for (tags_str,) in tag_analysis:
            try:
                tags = json.loads(tags_str)
                all_tags.extend(tags)
            except:
                pass
        
        tag_counts = {}
        for tag in all_tags:
            tag_counts[tag] = tag_counts.get(tag, 0) + 1
        
        for tag, count in sorted(tag_counts.items(), key=lambda x: x[1], reverse=True)[:10]:
            print(f"   {tag}: {count}회")
        
        # 고품질 질문 (점수 높은 순)
        print(f"\n⭐ 고품질 질문 (점수 높은 순):")
        high_quality = conn.execute('''
            SELECT question_id, title, score, view_count
            FROM questions
            ORDER BY score DESC
            LIMIT 5
        ''').fetchall()
        
        for qid, title, score, views in high_quality:
            print(f"   점수 {score}: {title[:80]}...")
            print(f"            (ID: {qid}, 조회수: {views:,})")
        
        # Excel 함수 언급 분석
        print(f"\n🔧 Excel 함수 언급 분석:")
        excel_functions = ['vlookup', 'index', 'match', 'sumif', 'countif', 'if(', 'pivot']
        
        for func in excel_functions:
            count = conn.execute('''
                SELECT COUNT(*) FROM questions 
                WHERE LOWER(title) LIKE ? OR LOWER(body_markdown) LIKE ?
            ''', (f'%{func}%', f'%{func}%')).fetchone()[0]
            
            if count > 0:
                print(f"   {func.upper()}: {count}개 질문")
        
        # 샘플 Q&A 쌍 출력
        print(f"\n📝 샘플 Q&A 쌍:")
        samples = conn.execute('''
            SELECT q.question_id, q.title, q.score as q_score, 
                   a.body_markdown, a.score as a_score
            FROM questions q
            JOIN answers a ON q.question_id = a.question_id
            ORDER BY q.score DESC
            LIMIT 2
        ''').fetchall()
        
        for i, (qid, title, q_score, answer_body, a_score) in enumerate(samples, 1):
            print(f"\n   샘플 {i}:")
            print(f"   질문: {title}")
            print(f"   점수: Q{q_score}/A{a_score}")
            print(f"   답변 미리보기: {answer_body[:300]}...")

def export_analysis_results(db_path):
    """분석 결과를 파일로 내보내기"""
    print(f"\n📤 분석 결과 내보내기")
    print("=" * 50)
    
    try:
        # JSON 형태로 전체 데이터 내보내기
        with sqlite3.connect(db_path) as conn:
            # 질문과 답변을 조인한 완전한 데이터
            complete_data = conn.execute('''
                SELECT 
                    q.question_id, q.title, q.body_markdown as question_body,
                    q.score as question_score, q.view_count, q.tags,
                    a.answer_id, a.body_markdown as answer_body, a.score as answer_score
                FROM questions q
                LEFT JOIN answers a ON q.question_id = a.question_id
                ORDER BY q.score DESC
            ''').fetchall()
            
            # 데이터 구조화
            export_data = []
            for row in complete_data:
                qid, title, q_body, q_score, views, tags_str, aid, a_body, a_score = row
                
                try:
                    tags = json.loads(tags_str) if tags_str else []
                except:
                    tags = []
                
                item = {
                    "question": {
                        "id": qid,
                        "title": title,
                        "body": q_body,
                        "score": q_score,
                        "view_count": views,
                        "tags": tags
                    },
                    "answer": {
                        "id": aid,
                        "body": a_body,
                        "score": a_score
                    } if aid else None
                }
                export_data.append(item)
            
            # 파일 저장
            export_file = Path(Config.OUTPUT_DIR) / f"stackoverflow_analysis_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
            with open(export_file, 'w', encoding='utf-8') as f:
                json.dump(export_data, f, indent=2, ensure_ascii=False)
            
            print(f"✅ 분석 데이터 내보내기 완료:")
            print(f"   파일: {export_file}")
            print(f"   크기: {export_file.stat().st_size:,} bytes")
            print(f"   항목 수: {len(export_data)}")
            
            # 요약 통계 파일 생성
            summary_file = Path(Config.OUTPUT_DIR) / f"stackoverflow_summary_{datetime.now().strftime('%Y%m%d_%H%M%S')}.txt"
            with open(summary_file, 'w', encoding='utf-8') as f:
                f.write("Stack Overflow Excel Q&A 데이터 수집 요약\n")
                f.write("=" * 50 + "\n\n")
                f.write(f"수집 일시: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
                f.write(f"총 질문 수: {len([x for x in export_data if x['question']])}\n")
                f.write(f"답변 포함 질문: {len([x for x in export_data if x['answer']])}\n")
                f.write(f"평균 질문 점수: {sum(x['question']['score'] for x in export_data if x['question']) / len(export_data):.1f}\n")
                f.write(f"데이터 파일: {export_file.name}\n")
            
            print(f"   요약 파일: {summary_file}")
            
    except Exception as e:
        print(f"❌ 내보내기 실패: {e}")

def main():
    """메인 함수"""
    print("🚀 Stack Overflow 데이터베이스 저장 및 분석 테스트")
    print("=" * 60)
    
    try:
        # 1. 캐시에서 데이터 추출
        questions = extract_cached_stackoverflow_data()
        
        if not questions:
            print("❌ 추출할 질문 데이터가 없습니다.")
            return
        
        # 2. 데이터베이스 생성
        db_path = create_analysis_database()
        
        # 3. 데이터 저장
        store_questions_in_database(questions, db_path)
        
        # 4. 데이터 분석
        analyze_stored_data(db_path)
        
        # 5. 결과 내보내기
        export_analysis_results(db_path)
        
        print(f"\n🎉 데이터베이스 테스트 완료!")
        print(f"   ✅ 캐시 데이터 추출 성공")
        print(f"   ✅ 데이터베이스 저장 성공")
        print(f"   ✅ 데이터 분석 완료")
        print(f"   ✅ 결과 내보내기 완료")
        print(f"\n📁 생성된 파일:")
        print(f"   - 데이터베이스: {db_path}")
        print(f"   - 내보내기 파일: {Config.OUTPUT_DIR}")
        
    except Exception as e:
        print(f"❌ 테스트 실패: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    main()