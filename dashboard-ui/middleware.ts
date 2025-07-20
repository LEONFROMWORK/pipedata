import { withAuth } from "next-auth/middleware"

export default withAuth(
  function middleware(req) {
    // 추가 미들웨어 로직이 필요하면 여기에 추가
  },
  {
    callbacks: {
      authorized: ({ token }) => {
        // 토큰이 있으면 인증된 사용자
        return !!token
      },
    },
  }
)

export const config = {
  matcher: [
    /*
     * Match all request paths except for the ones starting with:
     * - api/auth (NextAuth.js routes)
     * - _next/static (static files)
     * - _next/image (image optimization files)
     * - favicon.ico (favicon file)
     * - login (login page)
     * - public folder
     */
    "/((?!api/auth|_next/static|_next/image|favicon.ico|login|public).*)",
  ],
}