'use client'

import { useSession, signOut } from 'next-auth/react'
import { Button } from '@/components/ui/button'
import { LogOut, User } from 'lucide-react'

export function UserNav() {
  const { data: session } = useSession()

  if (!session?.user) {
    return null
  }

  return (
    <div className="flex items-center space-x-4">
      <div className="flex items-center space-x-2">
        <div className="w-8 h-8 bg-blue-600 rounded-full flex items-center justify-center">
          <User className="w-4 h-4 text-white" />
        </div>
        <div className="text-sm">
          <p className="font-medium text-gray-900">{session.user.name}</p>
          <p className="text-gray-500">{session.user.email}</p>
        </div>
      </div>
      
      <Button
        variant="outline"
        size="sm"
        onClick={() => signOut({ callbackUrl: '/login' })}
        className="flex items-center space-x-2"
      >
        <LogOut className="w-4 h-4" />
        <span>로그아웃</span>
      </Button>
    </div>
  )
}