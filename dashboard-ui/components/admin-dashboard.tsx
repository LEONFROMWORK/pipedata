"use client"

import React, { useState, useEffect } from 'react'
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from "@/components/ui/card"
import { Button } from "@/components/ui/button"
import { Badge } from "@/components/ui/badge"
import { Tabs, TabsContent, TabsList, TabsTrigger } from "@/components/ui/tabs"
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from "@/components/ui/table"
import { Dialog, DialogContent, DialogDescription, DialogHeader, DialogTitle, DialogTrigger } from "@/components/ui/dialog"
import { Textarea } from "@/components/ui/textarea"
import { Input } from "@/components/ui/input"
import { Label } from "@/components/ui/label"
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from "@/components/ui/select"
import { AlertCircle, CheckCircle, XCircle, Download, Send, Eye, RefreshCw, BarChart3 } from 'lucide-react'
import { toast } from 'sonner'

interface DataBatch {
  batch_id: string
  created_at: string
  total_items: number
  avg_quality_score: number
  sources: string[]
  status: string
  reviewed_by?: string
  reviewed_at?: string
  notes?: string
}

interface TransmissionRecord {
  id: number
  batch_id: string
  sent_at: string
  sent_by: string
  items_count: number
  success_count: number
  error_count: number
  total_items: number
}

interface AdminStats {
  batch_stats: {
    total_batches: number
    pending_batches: number
    approved_batches: number
    sent_batches: number
    total_items: number
    overall_avg_quality: number
  }
  transmission_stats: {
    total_transmissions: number
    total_items_sent: number
    total_success: number
    total_errors: number
  }
  last_updated: string
}

export default function AdminDashboard() {
  const [pendingBatches, setPendingBatches] = useState<DataBatch[]>([])
  const [transmissionHistory, setTransmissionHistory] = useState<TransmissionRecord[]>([])
  const [stats, setStats] = useState<AdminStats | null>(null)
  const [selectedBatch, setSelectedBatch] = useState<DataBatch | null>(null)
  const [batchData, setBatchData] = useState<any[]>([])
  const [isLoading, setIsLoading] = useState(false)
  const [adminToken, setAdminToken] = useState('')
  const [isAuthenticated, setIsAuthenticated] = useState(false)

  // API 호출 헬퍼
  const apiCall = async (endpoint: string, options: RequestInit = {}) => {
    const response = await fetch(`${process.env.NEXT_PUBLIC_PIPEDATA_API_URL}${endpoint}`, {
      ...options,
      headers: {
        ...options.headers,
        'Authorization': `Bearer ${adminToken}`,
        'Content-Type': 'application/json'
      }
    })

    if (!response.ok) {
      if (response.status === 401) {
        setIsAuthenticated(false)
        throw new Error('인증이 필요합니다.')
      }
      throw new Error(`API 오류: ${response.status}`)
    }

    return response.json()
  }

  // 인증 확인
  const checkAuth = async () => {
    if (!adminToken) return

    try {
      await apiCall('/api/admin/stats')
      setIsAuthenticated(true)
      await loadData()
    } catch (error) {
      setIsAuthenticated(false)
      toast.error('인증에 실패했습니다.')
    }
  }

  // 데이터 로드
  const loadData = async () => {
    try {
      setIsLoading(true)
      
      const [batchesResponse, historyResponse, statsResponse] = await Promise.all([
        apiCall('/api/admin/batches/pending'),
        apiCall('/api/admin/transmission-history?limit=20'),
        apiCall('/api/admin/stats')
      ])

      setPendingBatches(batchesResponse.batches || [])
      setTransmissionHistory(historyResponse.history || [])
      setStats(statsResponse.stats || null)
    } catch (error) {
      toast.error('데이터 로드에 실패했습니다.')
    } finally {
      setIsLoading(false)
    }
  }

  // 새 배치 생성
  const createNewBatch = async () => {
    try {
      setIsLoading(true)
      const response = await apiCall('/api/admin/batches/create', {
        method: 'POST',
        body: JSON.stringify({
          min_quality: 7.0,
          max_items: 100
        })
      })

      toast.success(`새 배치가 생성되었습니다: ${response.batch_id}`)
      await loadData()
    } catch (error) {
      toast.error('배치 생성에 실패했습니다.')
    } finally {
      setIsLoading(false)
    }
  }

  // 배치 데이터 조회
  const viewBatchData = async (batch: DataBatch) => {
    try {
      setIsLoading(true)
      const response = await apiCall(`/api/admin/batches/${batch.batch_id}/data`)
      setBatchData(response.data || [])
      setSelectedBatch(batch)
    } catch (error) {
      toast.error('배치 데이터 조회에 실패했습니다.')
    } finally {
      setIsLoading(false)
    }
  }

  // 배치 내보내기
  const exportBatch = async (batchId: string, format: string) => {
    try {
      setIsLoading(true)
      const response = await apiCall(`/api/admin/batches/${batchId}/export`, {
        method: 'POST',
        body: JSON.stringify({
          format,
          admin_id: 'admin-dashboard'
        })
      })

      toast.success(`데이터가 내보내졌습니다: ${response.filepath}`)
    } catch (error) {
      toast.error('데이터 내보내기에 실패했습니다.')
    } finally {
      setIsLoading(false)
    }
  }

  // 배치 검토
  const reviewBatch = async (batchId: string, action: 'approve' | 'reject', notes: string) => {
    try {
      setIsLoading(true)
      await apiCall(`/api/admin/batches/${batchId}/review`, {
        method: 'POST',
        body: JSON.stringify({
          action,
          admin_id: 'admin-dashboard',
          notes
        })
      })

      toast.success(`배치가 ${action === 'approve' ? '승인' : '거부'}되었습니다.`)
      await loadData()
    } catch (error) {
      toast.error('배치 검토에 실패했습니다.')
    } finally {
      setIsLoading(false)
    }
  }

  // 배치 전송
  const sendBatch = async (batchId: string) => {
    try {
      setIsLoading(true)
      const response = await apiCall(`/api/admin/batches/${batchId}/send`, {
        method: 'POST',
        body: JSON.stringify({
          admin_id: 'admin-dashboard'
        })
      })

      if (response.success) {
        toast.success(`배치가 성공적으로 전송되었습니다: ${response.items_sent}개 항목`)
      } else {
        toast.error(`배치 전송에 실패했습니다: ${response.message}`)
      }
      
      await loadData()
    } catch (error) {
      toast.error('배치 전송에 실패했습니다.')
    } finally {
      setIsLoading(false)
    }
  }

  // 인증 폼
  if (!isAuthenticated) {
    return (
      <div className="min-h-screen flex items-center justify-center bg-gray-50">
        <Card className="w-full max-w-md">
          <CardHeader>
            <CardTitle>관리자 인증</CardTitle>
            <CardDescription>관리자 토큰을 입력하세요</CardDescription>
          </CardHeader>
          <CardContent className="space-y-4">
            <div>
              <Label htmlFor="token">관리자 토큰</Label>
              <Input
                id="token"
                type="password"
                value={adminToken}
                onChange={(e) => setAdminToken(e.target.value)}
                placeholder="관리자 토큰을 입력하세요"
              />
            </div>
            <Button onClick={checkAuth} className="w-full">
              로그인
            </Button>
          </CardContent>
        </Card>
      </div>
    )
  }

  const getStatusBadge = (status: string) => {
    switch (status) {
      case 'pending':
        return <Badge variant="secondary"><AlertCircle className="w-3 h-3 mr-1" />대기</Badge>
      case 'approved':
        return <Badge variant="default"><CheckCircle className="w-3 h-3 mr-1" />승인</Badge>
      case 'rejected':
        return <Badge variant="destructive"><XCircle className="w-3 h-3 mr-1" />거부</Badge>
      case 'sent':
        return <Badge variant="outline"><Send className="w-3 h-3 mr-1" />전송됨</Badge>
      default:
        return <Badge variant="secondary">{status}</Badge>
    }
  }

  return (
    <div className="min-h-screen bg-gray-50 p-6">
      <div className="max-w-7xl mx-auto space-y-6">
        {/* 헤더 */}
        <div className="flex justify-between items-center">
          <div>
            <h1 className="text-3xl font-bold">PipeData 관리자 대시보드</h1>
            <p className="text-gray-600">데이터 배치 관리 및 ExcelApp 동기화</p>
          </div>
          <div className="flex gap-2">
            <Button onClick={loadData} disabled={isLoading}>
              <RefreshCw className="w-4 h-4 mr-2" />
              새로고침
            </Button>
            <Button onClick={createNewBatch} disabled={isLoading}>
              새 배치 생성
            </Button>
          </div>
        </div>

        {/* 통계 카드 */}
        {stats && (
          <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-4 gap-4">
            <Card>
              <CardHeader className="pb-2">
                <CardTitle className="text-sm font-medium">총 배치</CardTitle>
              </CardHeader>
              <CardContent>
                <div className="text-2xl font-bold">{stats.batch_stats.total_batches}</div>
                <p className="text-xs text-gray-600">
                  대기: {stats.batch_stats.pending_batches}, 승인: {stats.batch_stats.approved_batches}
                </p>
              </CardContent>
            </Card>

            <Card>
              <CardHeader className="pb-2">
                <CardTitle className="text-sm font-medium">총 데이터</CardTitle>
              </CardHeader>
              <CardContent>
                <div className="text-2xl font-bold">{stats.batch_stats.total_items}</div>
                <p className="text-xs text-gray-600">
                  평균 품질: {stats.batch_stats.overall_avg_quality?.toFixed(1)}
                </p>
              </CardContent>
            </Card>

            <Card>
              <CardHeader className="pb-2">
                <CardTitle className="text-sm font-medium">전송 완료</CardTitle>
              </CardHeader>
              <CardContent>
                <div className="text-2xl font-bold">{stats.transmission_stats.total_items_sent}</div>
                <p className="text-xs text-gray-600">
                  성공률: {stats.transmission_stats.total_success}/{stats.transmission_stats.total_items_sent}
                </p>
              </CardContent>
            </Card>

            <Card>
              <CardHeader className="pb-2">
                <CardTitle className="text-sm font-medium">전송 배치</CardTitle>
              </CardHeader>
              <CardContent>
                <div className="text-2xl font-bold">{stats.batch_stats.sent_batches}</div>
                <p className="text-xs text-gray-600">
                  총 전송: {stats.transmission_stats.total_transmissions}회
                </p>
              </CardContent>
            </Card>
          </div>
        )}

        {/* 메인 탭 */}
        <Tabs defaultValue="pending" className="space-y-4">
          <TabsList>
            <TabsTrigger value="pending">대기 중인 배치</TabsTrigger>
            <TabsTrigger value="history">전송 이력</TabsTrigger>
          </TabsList>

          <TabsContent value="pending">
            <Card>
              <CardHeader>
                <CardTitle>대기 중인 배치</CardTitle>
                <CardDescription>검토 및 승인이 필요한 데이터 배치</CardDescription>
              </CardHeader>
              <CardContent>
                <Table>
                  <TableHeader>
                    <TableRow>
                      <TableHead>배치 ID</TableHead>
                      <TableHead>생성일</TableHead>
                      <TableHead>아이템 수</TableHead>
                      <TableHead>평균 품질</TableHead>
                      <TableHead>소스</TableHead>
                      <TableHead>상태</TableHead>
                      <TableHead>작업</TableHead>
                    </TableRow>
                  </TableHeader>
                  <TableBody>
                    {pendingBatches.map((batch) => (
                      <TableRow key={batch.batch_id}>
                        <TableCell className="font-mono text-xs">
                          {batch.batch_id.substring(0, 8)}...
                        </TableCell>
                        <TableCell>
                          {new Date(batch.created_at).toLocaleDateString('ko-KR')}
                        </TableCell>
                        <TableCell>{batch.total_items}</TableCell>
                        <TableCell>{batch.avg_quality_score.toFixed(1)}</TableCell>
                        <TableCell>
                          <div className="flex flex-wrap gap-1">
                            {batch.sources.slice(0, 2).map((source) => (
                              <Badge key={source} variant="outline" className="text-xs">
                                {source}
                              </Badge>
                            ))}
                            {batch.sources.length > 2 && (
                              <Badge variant="outline" className="text-xs">
                                +{batch.sources.length - 2}
                              </Badge>
                            )}
                          </div>
                        </TableCell>
                        <TableCell>{getStatusBadge(batch.status)}</TableCell>
                        <TableCell>
                          <div className="flex gap-1">
                            <Button
                              size="sm"
                              variant="outline"
                              onClick={() => viewBatchData(batch)}
                            >
                              <Eye className="w-3 h-3" />
                            </Button>
                            
                            {batch.status === 'pending' && (
                              <>
                                <Dialog>
                                  <DialogTrigger asChild>
                                    <Button size="sm" variant="outline">
                                      검토
                                    </Button>
                                  </DialogTrigger>
                                  <DialogContent>
                                    <DialogHeader>
                                      <DialogTitle>배치 검토</DialogTitle>
                                      <DialogDescription>
                                        배치 {batch.batch_id.substring(0, 8)}... 검토
                                      </DialogDescription>
                                    </DialogHeader>
                                    <ReviewBatchForm
                                      batch={batch}
                                      onReview={reviewBatch}
                                    />
                                  </DialogContent>
                                </Dialog>
                              </>
                            )}

                            {batch.status === 'approved' && (
                              <Button
                                size="sm"
                                onClick={() => sendBatch(batch.batch_id)}
                                disabled={isLoading}
                              >
                                <Send className="w-3 h-3" />
                              </Button>
                            )}

                            <Select onValueChange={(format) => exportBatch(batch.batch_id, format)}>
                              <SelectTrigger className="w-auto h-8">
                                <Download className="w-3 h-3" />
                              </SelectTrigger>
                              <SelectContent>
                                <SelectItem value="json">JSON</SelectItem>
                                <SelectItem value="csv">CSV</SelectItem>
                                <SelectItem value="excel">Excel</SelectItem>
                              </SelectContent>
                            </Select>
                          </div>
                        </TableCell>
                      </TableRow>
                    ))}
                  </TableBody>
                </Table>
              </CardContent>
            </Card>
          </TabsContent>

          <TabsContent value="history">
            <Card>
              <CardHeader>
                <CardTitle>전송 이력</CardTitle>
                <CardDescription>ExcelApp으로 전송된 배치 이력</CardDescription>
              </CardHeader>
              <CardContent>
                <Table>
                  <TableHeader>
                    <TableRow>
                      <TableHead>배치 ID</TableHead>
                      <TableHead>전송일</TableHead>
                      <TableHead>전송자</TableHead>
                      <TableHead>아이템 수</TableHead>
                      <TableHead>성공/실패</TableHead>
                      <TableHead>성공률</TableHead>
                    </TableRow>
                  </TableHeader>
                  <TableBody>
                    {transmissionHistory.map((record) => (
                      <TableRow key={record.id}>
                        <TableCell className="font-mono text-xs">
                          {record.batch_id?.substring(0, 8)}...
                        </TableCell>
                        <TableCell>
                          {new Date(record.sent_at).toLocaleString('ko-KR')}
                        </TableCell>
                        <TableCell>{record.sent_by}</TableCell>
                        <TableCell>{record.items_count}</TableCell>
                        <TableCell>
                          <Badge variant={record.error_count > 0 ? 'destructive' : 'default'}>
                            {record.success_count}/{record.items_count}
                          </Badge>
                        </TableCell>
                        <TableCell>
                          {((record.success_count / record.items_count) * 100).toFixed(1)}%
                        </TableCell>
                      </TableRow>
                    ))}
                  </TableBody>
                </Table>
              </CardContent>
            </Card>
          </TabsContent>
        </Tabs>

        {/* 배치 데이터 상세 다이얼로그 */}
        <Dialog open={!!selectedBatch} onOpenChange={() => setSelectedBatch(null)}>
          <DialogContent className="max-w-4xl max-h-[80vh] overflow-y-auto">
            <DialogHeader>
              <DialogTitle>
                배치 데이터: {selectedBatch?.batch_id.substring(0, 8)}...
              </DialogTitle>
              <DialogDescription>
                총 {batchData.length}개 아이템
              </DialogDescription>
            </DialogHeader>
            <div className="space-y-4">
              {batchData.slice(0, 10).map((item, index) => (
                <Card key={item.id}>
                  <CardHeader className="pb-2">
                    <div className="flex justify-between items-start">
                      <CardTitle className="text-sm">질문 #{index + 1}</CardTitle>
                      <Badge variant="outline">
                        품질: {item.quality_score}
                      </Badge>
                    </div>
                  </CardHeader>
                  <CardContent className="space-y-2">
                    <div>
                      <Label className="font-medium">질문:</Label>
                      <p className="text-sm">{item.question.substring(0, 200)}...</p>
                    </div>
                    <div>
                      <Label className="font-medium">답변:</Label>
                      <p className="text-sm">{item.answer.substring(0, 200)}...</p>
                    </div>
                    <div className="flex gap-2">
                      <Badge variant="secondary">{item.difficulty}</Badge>
                      <Badge variant="outline">{item.source}</Badge>
                    </div>
                  </CardContent>
                </Card>
              ))}
              {batchData.length > 10 && (
                <p className="text-center text-gray-500">
                  및 {batchData.length - 10}개 아이템 더...
                </p>
              )}
            </div>
          </DialogContent>
        </Dialog>
      </div>
    </div>
  )
}

// 배치 검토 폼 컴포넌트
function ReviewBatchForm({ 
  batch, 
  onReview 
}: { 
  batch: DataBatch
  onReview: (batchId: string, action: 'approve' | 'reject', notes: string) => void 
}) {
  const [notes, setNotes] = useState('')
  const [action, setAction] = useState<'approve' | 'reject'>('approve')

  return (
    <div className="space-y-4">
      <div>
        <Label>검토 결정</Label>
        <Select value={action} onValueChange={(value: 'approve' | 'reject') => setAction(value)}>
          <SelectTrigger>
            <SelectValue />
          </SelectTrigger>
          <SelectContent>
            <SelectItem value="approve">승인</SelectItem>
            <SelectItem value="reject">거부</SelectItem>
          </SelectContent>
        </Select>
      </div>
      
      <div>
        <Label>검토 노트</Label>
        <Textarea
          value={notes}
          onChange={(e) => setNotes(e.target.value)}
          placeholder="검토 의견을 입력하세요..."
          rows={3}
        />
      </div>

      <Button 
        onClick={() => onReview(batch.batch_id, action, notes)}
        className="w-full"
      >
        {action === 'approve' ? '승인' : '거부'}
      </Button>
    </div>
  )
}